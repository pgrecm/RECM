/**********************************************************************************/
/* Backup WAL command                                                             */
/* Usage:                                                                         */
/*    backup wal                                                                  */                                  
/*      Available Option(s):                                                      */
/*         /delete                   Force deletion of backuped wal               */
/*         /lock                     Disable retention policy for this backup     */
/*         /nochkpt                  Do not initiate CHECKPOINT (switch wal)      */
/*         /nodelete                 Do not perform WAL deletion for this run     */
/*         /parallel=N               Use parallel threads to make backup faster   */
/*         /switch                   Invoke a switch WAL before backup            */
/*         /verbose                                                               */
/**********************************************************************************/
#define WAL_NOT_BACKUPED 0
#define WAL_BACKUPED 1
#define WAL_CAN_BE_DELETED 2
#define WAL_ALREADY_BACKUPED 4

void threadInitalizeTable();
int threadCountRunning(int maxThreads);
int ThreadGetAvailableProcess(int maxThreads);

/********************************************************************************/
/* Create the RECM file by a thread                                             */
/********************************************************************************/
void *threadCreateWALPiece(void *dummyPtr)
{
   ThreadData *td=(ThreadData *)dummyPtr;
   long oldest=0;
   long youngest=0;
   long file_age_min;
                       
   pthread_mutex_lock( &mutex1 );
   time_t system_time = time(NULL);
   oldest=system_time;
   youngest=0;
   char *tarFileName=malloc(1024);
   sprintf(tarFileName,"%s/%s_%ld_%s.recm",                                                                                          // Only 1 piece
                       varGet(GVAR_BACKUPDIR),
                       varGet(RECM_BCKID),
                       td->pieceNumber,
                       varGet(RECM_BCKTYPE));
   
   td->zH=RECMcreate(tarFileName,td->pieceNumber,varGetInt(GVAR_COMPLEVEL));
   free(tarFileName);
   
   td->zH->start_time=time(NULL);
   
   
   pthread_mutex_unlock( &mutex1 );

   long thread_file_processed=0;                                                                                                   // Verification counter
   long thread_size_processed=0;                                                                                                   // Verification counter 
   
   char *filename=malloc(1024);
   
   TreeItem* ti=td->pgdataTree->first;
   long autodelwal=varGetLong(GVAR_CLUAUTODELWAL); 
   TRACE("[%ld] WAL directory used : '%s'\n",td->pieceNumber,td->rootdirectory);
   int errcnt=0;
   while (ti != NULL && errcnt == 0)
   {
      if (ti->sequence == td->pieceNumber)
      {
         file_age_min=(system_time - ti->mtime) / 60;                                                                          // To have result in minutes
         TRACE("ti->flag=%x backuped=%x\n",ti->flag,(ti->used & WAL_BACKUPED));

         if (ti->flag == TREE_ITEM_FILE && (ti->used & WAL_BACKUPED) == 0)
         {
            if (oldest == 0) { oldest=ti->mtime;youngest=ti->mtime;}
                        else { if (oldest > ti->mtime)   oldest=ti->mtime;
                               if (youngest < ti->mtime) youngest=ti->mtime;
                             };

            sprintf(filename,"%s/%s",td->rootdirectory,ti->name);
            if (RECMaddFile(td->zH,ti->name,filename,td->rootdirectory,ti->st_mode,ti->st_uid,ti->st_gid)==false)
            {
               errcnt++;
               TRACE("[Worker %ld] FAILED TO ADD FILE '%s' (errnt=%d)\n",td->pieceNumber,filename,errcnt);
               
            }
            else
            {
               thread_file_processed++;
               thread_size_processed+=ti->size;
               VERBOSE("WAL file '%s' copying\n",ti->name);
               TRACE("[Worker %ld] ADD FILE '%s' (errnt=%d)\n",td->pieceNumber,filename,errcnt);
               ti->used=flagSetOption(ti->used,WAL_BACKUPED);
               if (autodelwal > -1 && file_age_min > autodelwal) ti->used=flagSetOption(ti->used,WAL_CAN_BE_DELETED);
            };
         }
         else
         {
            VERBOSE("WAL file '%s' already backuped\n",ti->name);
            if (autodelwal > -1 && file_age_min > autodelwal)                                                                      // If '/delwal' > -1 and file age > /delwal
            {
               ti->used=flagSetOption(ti->used,WAL_CAN_BE_DELETED);
            }
         }
      };
      ti=ti->next;
   }
   free(filename);
   
   TRACE("[Worker %ld] count=%lu size=%lu\n",td->pieceNumber,thread_file_processed,thread_size_processed);
   if (RECMclose(td->zH,thread_size_processed,thread_file_processed)==false) errcnt++;

   pthread_mutex_lock( &mutex1 );
   RECMUpdateDepositAtClose(td->zH,thread_size_processed,thread_file_processed);
   if (errcnt > 0)
   {
      td->rc=1;
      td->state=THREAD_END_FAILED;
   }
   else
   {
      td->state=THREAD_END_SUCCESSFULL;
      td->rc=0;
      thread_all_file_processed+=thread_file_processed;                                                                            // Update global Verification counter
      thread_all_size_processed+=thread_size_processed;                                                                            // Update global Verification counter 
   };                                                                                                                                              
   pthread_mutex_unlock( &mutex1 );
   return(NULL);
}

/********************************************************************************/
/* Start a Thread                                                               */
/********************************************************************************/
int threadStartWAL(int idThread, TreeEntry * tree,int pieceNumber,const char *bckid,const char *bcktyp,const char *rootDir)
{
   pthread_mutex_lock( &mutex1 );
   threads[idThread].pieceNumber=pieceNumber;                                           // ID of the Piece to create
   threads[idThread].state=THREAD_RUNNING;
   threads[idThread].pgdataTree=tree;                                                   // File table
   threads[idThread].rc=pthread_create(&threads[idThread].threadID, NULL, threadCreateWALPiece,&threads[idThread]);
   strcpy(threads[idThread].bck_id,bckid);                                              // Backup UID                                              // Sequence used by this thread
   strcpy(threads[idThread].bck_typ,bcktyp);                                            // Backup Type
   strcpy(threads[idThread].rootdirectory,rootDir);
   
   pthread_mutex_unlock( &mutex1 );
   return(threads[idThread].rc);
}


/********************************************************************************/
/* Load all WaL files                                                           */
/********************************************************************************/
int loadWALfiles(TreeEntry *tree,const char *rootdir)
{
   struct stat   file_st;
   int            rc;
   DIR           *dp;
   struct dirent *ep;
//   ArrayItem     *itm;
//   index_details *ndx;
//   TreeItem *saved;
//   char          *lnk_name;
   char          *fullname=malloc(1024);
//   char          *full_linkname;
//   int            skip_file;
//   char          *fsm_file;
   TRACE ("ROOT Entry='%s'\n",rootdir);
   dp = opendir(rootdir);
   if (dp != NULL)
   {
      while ( (ep = readdir (dp)))
      {
         if (strcmp(ep->d_name,".")              == 0) { continue; };                                                              // ignore some files
         if (strcmp(ep->d_name,"..")             == 0) { continue; };
         if (ep->d_type != DT_REG)                     { continue; }; // Just take files. Ignore Directories and links
         sprintf(fullname,"%s/%s",rootdir,ep->d_name);
         rc=stat(fullname, &file_st);
         TRACE("stat '%s' [rc=%d (errno %d) : '%s'(%u) / %u / %u]\n",fullname,rc,errno,maskCreate(file_st.st_mode),file_st.st_mode,file_st.st_uid,file_st.st_gid);
         treeAddItem(tree,ep->d_name,TREE_ITEM_FILE,&file_st);
      };
      closedir (dp);
   }
   free(fullname);
   return(true);
}

/********************************************************************************/
/* Evaluate the size of each pieces,depending on the parallelism choosen        */
/* the function return the number of pieces to create                           */
/********************************************************************************/
int prepareWALPieces (TreeEntry *tree,long limitSize,long limitFile)
{
   char *query=malloc(1024);
   long cumulSize=0;
   long cumulFiles=0;
   int ZERO_FILE=1;
   int SEQ=1;
   TreeItem* ti=tree->first;
   while (ti != NULL)
   {
      sprintf(query,"select count(*) from %s.backup_wals where cid=%s and walfile='%s'",      // Verify if the file has been already backuped
                    varGet(GVAR_DEPUSER),
                    varGet(GVAR_CLUCID),
                    ti->name);
      int nRows=DEPOquery(query,0);
      int fnd=0;
      if (nRows > 0) { fnd=DEPOgetInt(0,0); };
      DEPOqueryEnd();
      if (fnd > 0)
      {
         TRACE("File '%s' already backuped [ Setting WAL_ALREADY_BACKUPED ]\n",ti->name);
         ti->used=WAL_ALREADY_BACKUPED+WAL_BACKUPED;                                                               // Assume this file is already backuped
         ti->sequence=SEQ;
      }
      else
      {
         verification_limitFile++;                                     //Counters for validation at the end of the backup
         verification_limitSize+=ti->size;     
         TRACE("File '%s' not backuped yet [ Setting WAL_NOT_BACKUPED ]\n",ti->name);
         ti->used=WAL_NOT_BACKUPED;                                                           // This one will be added to Backup                                                   
         cumulSize=cumulSize+ti->size;                                                        // add file size 
         cumulFiles++;
         ZERO_FILE=0;
         ti->sequence=SEQ;                                                                    // add file to same backup piece
         if (cumulSize > limitSize || cumulFiles >= limitFile)                                // Change backup piece ?
         {
            SEQ++;                                                                            // Change backup piece
            cumulFiles=0;                                                                     // and reset counters
            cumulSize=0;
         };
      };
      ti=ti->next;
   };
   free(query);
   if (ZERO_FILE == 1) SEQ=-1; // No file to backup...
   return(SEQ);
}

int updateDEPOSITforWAL(TreeEntry *tree)
{
   int errcnt=0;
   char *query=malloc(1024);
   TreeItem* ti=tree->first;
   while (ti != NULL)
   {
      if ( (ti->used & WAL_BACKUPED) == WAL_BACKUPED)
      {
         sprintf(query,"insert into %s.backup_wals(cid,bck_id,walfile,ondisk,bckcount,edate)"                                      // Create one record per WAL
                                   " values (%s,'%s','%s',1,1,to_timestamp(%lu))",
                       varGet(GVAR_DEPUSER),
                       varGet(GVAR_CLUCID),
                       varGet(RECM_BCKID),
                       ti->name,
                       ti->mtime);
         int nRows=DEPOquery(query,0);
         if (nRows != 0 || DEPOrowCount() != 1)
         {
            errcnt++;
            ERROR(ERR_DBQRYERROR,"[insert-backup_wals] %s\n",DEPOgetErrorMessage());
         };
         DEPOqueryEnd();         
      }
      ti=ti->next;
   };
   free(query);
   return( errcnt == 0);
}

long deleteWALfiles(const char *wal_folder,TreeEntry *tree)
{
   // The DELETE wal file may be perturbed by a replication slot.
   char * slot_walfiles=malloc(1024);
   strcpy(slot_walfiles,",");
   char * query=malloc(1024);
   sprintf(query,"select slot_name,pg_walfile_name(restart_lsn),restart_lsn,case active when true then 'RUNNING' else 'STOPPED' end "
                 " from pg_replication_slots order by 2");
   int skip_deletion_count=0;
   int slot_count=0;
   int slot_running=0;
   int rc=CLUquery(query,1);
   if (rc > 0)
   {
      for (int n=0;n < rc; n++)
      {
         slot_count++;
         VERBOSE("Slot '%s' detected (%s at LSN '%s')\n",
                 CLUgetString(n,0),
                 CLUgetString(n,3),
                 CLUgetString(n,2));
         strcat(slot_walfiles,CLUgetString(n,1));
         strcat(slot_walfiles,",");
         if (strcmp(CLUgetString(n,3),"RUNNING") == 0) slot_running++;
      }
   };
   CLUqueryEnd();
   long wal_deleted=0;
   char *fullname=malloc(1024);
   TreeItem* ti=tree->first;
   int skip_deletion=0;
   while (ti != NULL)
   {
      if ( (ti->used & WAL_CAN_BE_DELETED) == WAL_CAN_BE_DELETED &&
           (ti->used & WAL_BACKUPED) == WAL_BACKUPED
         )
      {
         sprintf(fullname,"%s/%s",wal_folder,ti->name);
         if (skip_deletion == 0) 
         {
            if (strstr(slot_walfiles,ti->name) != NULL)
            { 
               skip_deletion++; 
            }
         };
         if (skip_deletion == 0)
         {
            VERBOSE ("deleting WAL '%s'\n",fullname);
            unlink(fullname);
            wal_deleted++;
         }
         else
         {
            VERBOSE("Keep WAL '%s' (Slot)\n",fullname);
            skip_deletion_count++;
         }
      }
      ti=ti->next;
   };
   free(slot_walfiles);
   free(query);
   free(fullname);
   if (skip_deletion_count > 10)
   {
      if (slot_count > slot_running && slot_count > 0 && wal_deleted == 0)
      {
         WARN(WRN_SLOTPRESENT,"Slot is stopped. Restart the SLOT or drop it, to avoid disk space issue. No more WAL files are deleted.\n");
      };
   }
   return(wal_deleted);
}


void COMMAND_BACKUPWAL(int idcmd,char *command_line)
{
   long BACKUP_OPTIONS=0; 
   
   if (isConnectedAndRegistered() == false) return;
   
   if (IsClusterEnabled(varGetLong(GVAR_CLUCID)) == false) 
   {
      WARN(ERR_SUSPENDED,"Cluster '%s' is 'DISABLED'. Operation ignored.\n",varGet(GVAR_CLUNAME));
      return;
   }

   if (varExist(GVAR_WALDIR) == false)
   {
      ERROR(ERR_NOWALDIR,"WAL directory not set.(See show cluster).\n");
      return;
   }
   if (varExist(GVAR_BACKUPDIR) == false)
   {
      ERROR(ERR_NOBACKUPDIR,"Backup directory not set.(See show cluster).\n");
      return;
   }
   if (optionIsSET("opt_verbose") == true) globalArgs.verbosity=true; 
   
   memBeginModule();
   
   if (varGetInt(GVAR_COMPLEVEL) > -1){ BACKUP_OPTIONS=backupSetOption(BACKUP_OPTIONS,BACKUP_OPTION_COMPRESSED); };   /* Change : 0xA000A */
   if (optionIsSET("opt_lock"))       { BACKUP_OPTIONS=backupSetOption(BACKUP_OPTIONS,BACKUP_OPTION_KEEPFOREVER);  }; /* Change : 0xA000A */

   char *destination_folder=memAlloc(1024);
   varTranslate(destination_folder,varGet(GVAR_BACKUPDIR),1);
   char *wal_folder=memAlloc(1024);
   varTranslate(wal_folder,varGet(GVAR_WALDIR),1);

   
   if (directory_exist(destination_folder) == false) 
   { 
      mkdirs(destination_folder,maskTranslate(varGet(GVAR_CLUMASK_BACKUP))); 
   }
   if (directory_exist(destination_folder) == false)
   {
      ERROR(ERR_INVDIR,"Invalid Backup directory '%s'\n",destination_folder);
      memEndModule();
      return;
   }
   

   
#define DELETE_WAL_ON_BACKUP 0   
#define KEEP_WAL_ON_BACKUP 1   
   
   int opt_deleteWAL=DELETE_WAL_ON_BACKUP;

   if (optionIsSET("opt_delete") == true)   opt_deleteWAL=DELETE_WAL_ON_BACKUP;
   if (optionIsSET("opt_nodelete") == true) opt_deleteWAL=KEEP_WAL_ON_BACKUP;                                                             // Set Verbosity

   varAdd(RECM_BCKTYPE,RECM_BCKTYP_WAL);

   char *qtag=asLiteral(varGet("qal_tag"));
   char *tag=memAlloc(128);
   if (strcmp(qtag,QAL_UNSET) == 0) { varTranslate(tag,"{YYYY}{MM}{DD}_{CluName}_WAL",0); }
                               else { strcpy(tag,qtag); }
   if (isValidName(tag) == false)
   {
      ERROR(ERR_INVCHARINNAME,"Invalid character in tag '%s'.\n",tag);
      memEndModule();
      return;
   };
   
   char *query=memAlloc(1024);
   
   int disable_checkpoint=false;
   // Check if a backup is already running (RECM_BACKUP_STATE_RUNNING)
   // We wait maximum 2 minutes before to abandon...
   sprintf(query,"select count(*) from %s.backups b where b.cid=%s and b.bcktyp in ('FULL','WAL') and bcksts=%d",
                 varGet(GVAR_DEPUSER),
                 varGet(GVAR_CLUCID),
                 RECM_BACKUP_STATE_RUNNING);
   int nRUN=DEPOquery(query,0);
   nRUN=DEPOgetInt(0,0);
   DEPOqueryEnd();
   if (nRUN > 0) { VERBOSE("A backup is already running.  Disabling Checkpoint and switch WAL...\n");
                   disable_checkpoint=true;
                 };
/*   int maxWait=10;
   while (nRUN > 0 && maxWait > 0)
   {
      INFO("A Backup is currently running...Waiting few seconds for the end of the backup...\n");
      sleep(10);
      DEPOquery(query,0);
      nRUN=DEPOgetInt(0,0);
      DEPOqueryEnd();
      maxWait--;
   };
   if (nRUN > 0 && maxWait <= 0)
   {
      ERROR(ERR_BCKINPROGRESS,"A backup is already running.\n");
      memEndModule();
      return;
   }
*/
   //Check if we already have an available FULL backup for the current TIMELINE
   sprintf(query,"select count(*) from %s.backups b where cid=%s and bcktyp='FULL' and bcksts=0 and timeline=%ld",
                 varGet(GVAR_DEPUSER),
                 varGet(GVAR_CLUCID),
                 pgGetCurrentTimeline());
   int nTL=DEPOquery(query,0);
   TRACE("[rc=%d]\n",nTL);
   int nBckFULL=DEPOgetInt(0,0);
   if (nBckFULL == 0)
   { 
      DEPOqueryEnd();
      ERROR(ERR_FULLREQUIRED,"There is NO FULL backup for timeline %ld. Perform a full backup prior taking WAL backups.\n",pgGetCurrentTimeline());
      memEndModule();
      return;
   }
   DEPOqueryEnd();

   if (optionIsSET("opt_switch") == true && disable_checkpoint == false)                                                                                          // Execute pg_switch_wal()
   {
      cluster_doSwitchWAL(optionIsSET("opt_nochkpt") == false);
      sleep(1);
   };
   
   thread_all_file_processed=0;
   thread_all_size_processed=0;
   verification_limitFile=0; // This value will be compared with the result of 'thread_all_file_processed' content, as control
   verification_limitSize=0; // This value will be compared with the result of 'thread_all_size_processed' content, as control
   

   /* Prepare threads */
   threadInitalizeTable();                                                                                                         // Initialize Thread table

   // Parallel qualifier
   int parallel=1;
   if (qualifierIsUNSET("qal_parallel") == false)                                                                                  // Validate the '/parallel=n'...must be in range 1..MAX_THREADS
   {
      parallel=varGetInt("qal_parallel");
      if (parallel < 1) parallel=1;
      if (parallel > MAX_THREADS) parallel=MAX_THREADS;
      if (parallel > 1) VERBOSE("PARALLEL backup requested with %d thread(s)\n",parallel);
   };
   TreeEntry *walTree=treeCreate();
//   char *WALDIR=memAlloc(1024);
//   sprintf(WALDIR,"%s/pg_wal",varGet(GVAR_CLUPGDATA));
   
   loadWALfiles(walTree,wal_folder);                                                                                                  // Load reference of all present WAL files

   if (walTree->entries == 0)                                                                                                 // If no WAL present, no need to create empty WAL backup                     
   {
     INFO("No WAL to backup.\n");
     treeDestroy(walTree);
     memEndModule();
     return;
   }
   
   // MAXSIZE limit
   long limitFile=varGetLong(GVAR_CLUBACKUP_MAXWALFILES);
   if (limitFile < HARD_LIMIT_MINWALFILES) limitFile=HARD_LIMIT_MINWALFILES;                                                       // Hard limit : Minimum backup piece WAL count
   
   long limitSize=getHumanSize(varGet(GVAR_CLUBACKUP_MAXWALSIZE));                                                                    // prepare the limit for MAXSIZE
   if (limitSize < getHumanSize(HARD_LIMIT_MINWALSIZE)) limitSize=getHumanSize(HARD_LIMIT_MINWALSIZE);
  
   if (parallel > 1)
   {
      long sections=(walTree->totalsize / limitSize);
      if (sections < parallel) { limitSize=(walTree->totalsize / parallel); }
                          else { limitSize=(walTree->totalsize / sections); };
      VERBOSE("Each backup pieces will have an average size of %s\n",DisplayprettySize(limitSize));
   }
   int number_of_pieces=prepareWALPieces (walTree,limitSize,limitFile);                                                            // Evaluate number of pieces to create
   
   if (number_of_pieces == -1|| verification_limitFile == 0)
   {
     INFO("No WAL to backup.\n");
     treeDestroy(walTree);
     memEndModule();
     return;
   }

   
   // Load special information for restore
   int rcx=CLUquery("select setting from pg_settings where name='log_timezone'",0);
   varAdd(GVAR_CLUTZ,CLUgetString(0,0));
   CLUqueryEnd();
   rcx=CLUquery("select setting from pg_settings where name='DateStyle'",0);
   varAdd(GVAR_CLUDS,CLUgetString(0,0));
   CLUqueryEnd();
   
   RECMClearPieceList();                                                                                                  // cleanup zip_filelist
   cluster_getCurrentWAL();                                                                                               // Get Current WAL
   createSequence(1);                                                                                                     // Reset piece sequence
   // Create Backup UID
   createUID();
   VERBOSE("Starting WAL backup of '%s'\n",varGet(SYSVAR_LASTLSN));
   varAdd(RECM_WALSTART,varGet(SYSVAR_LASTLSN));

   sprintf(query,"insert into %s.backups (cid,bcktyp,bcksts,bck_id,bcktag,hstname,pgdata,pgversion,"                      // Create record in  'running' state
                 "timeline,options,bcksize,pcount,ztime,sdate,bckdir)"
                 " values (%s,'%s',%d,'%s','%s','%s','%s',%d,%ld,%ld,%ld,%ld,'%s','%s','%s')",
        varGet(GVAR_DEPUSER),
        varGet(GVAR_CLUCID),
        varGet(RECM_BCKTYPE),
        RECM_BACKUP_STATE_RUNNING,
        varGet(RECM_BCKID),
        tag,
        varGet(GVAR_CLUHOST),
        varGet(GVAR_CLUPGDATA),
        varGetInt(GVAR_CLUVERSION),
        pgGetCurrentTimeline(),
        BACKUP_OPTIONS,
        thread_all_size_processed,
        thread_all_file_processed,
        varGet(GVAR_CLUTZ),
        varGet(GVAR_CLUDS),
        destination_folder);
   DEPOquery(query,0);
   DEPOqueryEnd();
   

   VERBOSE("Will create %d pieces by %d thread(s)\n",number_of_pieces,parallel);

   // Now we are going to create each backup piece by a thread.
   int current_piece=1;                                                                                                            // Scan array and assign a 'Piece Number' to an available thread
   int errcnt=0;
   while (current_piece <= number_of_pieces && errcnt == 0)
   {
      int nbrRunningProcess=threadCountRunning(parallel);                                                                          // Get Active thread count
      while (nbrRunningProcess >= parallel)                                                                                        // Wait for room in thread table regarding the limit set by command
      {
         sleep(1);
         nbrRunningProcess=threadCountRunning(parallel);
      };
      int numThread=ThreadGetAvailableProcess(parallel);                                                                           // Get free thread slot from thread table
      if (threadStartWAL(numThread,walTree,current_piece,varGet(RECM_BCKID),varGet(RECM_BCKTYPE),wal_folder) != 0)                            // Start new thread to process piece number 'current_piece'
      {
         errcnt++;
      };
      current_piece++;
   };

   while (threadCountRunning(parallel) > 0) { printf("Waitend\n");sleep(1); };                                                                         // Wait the end of all threads

   TRACE ("Backup writes : thread_all_file_processed=%lu verification_limitFile=%lu\n",thread_all_file_processed,verification_limitFile);
   TRACE ("Backup writes : thread_all_size_processed=%lu verification_limitSize=%lu\n",thread_all_size_processed,verification_limitSize);
   if (thread_all_file_processed == verification_limitFile && thread_all_size_processed == verification_limitSize)                 // All the counters should be equals
   {                                                                                                                               // Variables 'verification_limitXXX' are set at load file inventory (sequential)
      TRACE("BACKUP is OK\n");                                                                                                     // Variables 'thread_all_file_processed' and 'thread_all_size_processed'
   }                                                                                                                               //       are fullfilled by each thread. At the end, all the counters have to be equals !
   else
   {
      errcnt++;                                                                                                                    //      otherwise, we reject the backup.
      TRACE("REJECT BACKUP\n");
   };

   /* if backup is OK, perform the deletion on backuped files */
   long total_wal_deleted=0;
   
   
   if (errcnt == 0)
   {
      if (updateDEPOSITforWAL(walTree) == true) 
      {
         if (opt_deleteWAL == DELETE_WAL_ON_BACKUP && errcnt == 0)                                                                 // Now, we can delete files
         {
            total_wal_deleted=deleteWALfiles(wal_folder,walTree);
         };
      }
      else { errcnt++; };
   }
   else { errcnt++; };
   int bcksts;
   if (errcnt > 0) { bcksts=RECM_BACKUP_STATE_FAILED; }
              else { bcksts=RECM_BACKUP_STATE_AVAILABLE; };
   sprintf(query,"update %s.backups set bcksts=%d,bcksize=%ld,pcount=%d,edate=current_timestamp "                                  // Update backup record to become 'AVAILABLE'
                 " where cid=%s and bck_id='%s'",
                 varGet(GVAR_DEPUSER),
                 bcksts,
                 thread_all_size_processed,
                 number_of_pieces,
                 varGet(GVAR_CLUCID),
                 varGet(RECM_BCKID));
   DEPOquery(query,0);
   DEPOqueryEnd();
   RECMsetProperties(varGet(RECM_BCKID));
 
   sprintf(query,"update %s.clusters set lstwal=(select max(bdate) from %s.backups b "                                             // Update last WAL backup in DEPOSIT
                 " where bcktyp='WAL' and cid=%s) where cid=%s;",
                 varGet(GVAR_DEPUSER),
                 varGet(GVAR_DEPUSER),
                 varGet(GVAR_CLUCID),
                 varGet(GVAR_CLUCID));
   DEPOquery(query,1);
   DEPOqueryEnd();
   if (errcnt == 0)                                                                                                                // Display some statistics 
   {
      INFO("Backup WAL UID '%s' succesfully created.\n",varGet(RECM_BCKID));
      INFO("Total pieces ......... : %-8d\n",number_of_pieces);
      INFO("Total files backuped.. : %-8ld\n",thread_all_file_processed);
      if (varGetLong(GVAR_CLUAUTODELWAL) > -1)
      {
         INFO("Total files deleted... : %-8ld (Files aged %ld minute(s) or more)\n",total_wal_deleted,varGetLong(GVAR_CLUAUTODELWAL));
      }
      INFO("Total Backup size .... : %-8ld (%s)\n",thread_all_size_processed, DisplayprettySize(thread_all_size_processed) );
   }
   memEndModule();                                                                                                                 // Free allocated resources
   qualifierUNSET("qal_tag");
   qualifierUNSET("qal_directory");
   qualifierUNSET("qal_parallel");
}
