/**********************************************************************************/
/* Backup WAL command                                                             */
/* Usage:                                                                         */
/*    backup wal                                                                  */                                  
/*      Available Option(s):                                                      */
/*         /delete                   Force deletion of backuped wal               */
/*         /lock                     Disable retention policy for this backup     */
/*         /nochkpt                  Do not initiate CHECKPOINT (switch wal)      */
/*         /nodelete                 Do not perform WAL deletion for this run     */
/*         /switch                   Invoke a switch WAL before backup            */
/*         /verbose                                                               */
/**********************************************************************************/
void COMMAND_BACKUPWAL(int idcmd,const char *command_line)
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
   if (optionIsSET("opt_nodelete") == true) opt_deleteWAL=KEEP_WAL_ON_BACKUP;
   
   int opt_verbose=optionIsSET("opt_verbose");
   int saved_verbose=globalArgs.verbosity;
   globalArgs.verbosity=opt_verbose;

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

                               
   /* start */
   DIR *dp;
   struct dirent *ep;
   struct stat st;
   dp = opendir (wal_folder);
   if (dp == NULL)
   {
      ERROR(ERR_NODIRACCESS,"Cannot acces directory '%s'\n",wal_folder);
      memEndModule();
      return;
   }
   if (optionIsSET("opt_switch") == true) 
   { 
      cluster_doSwitchWAL(optionIsSET("opt_nochkpt") == false); 
   };
   char *query=memAlloc(1024);
   
   // Check if a backup is already running (RECM_BACKUP_STATE_RUNNING)
   sprintf(query,"select count(*) from %s.backups b where b.cid=%s and b.bcktyp in ('FULL','WAL') and bcksts=%d",
                 varGet(GVAR_DEPUSER),
                 varGet(GVAR_CLUCID),
                 RECM_BACKUP_STATE_RUNNING);
   int nRUN=DEPOquery(query,0);
   int RUNCount=DEPOgetInt(0,0);
   if (RUNCount > 0)
   {
      DEPOqueryEnd();
      ERROR(ERR_BCKINPROGRESS,"A backup is already running.\n");
      memEndModule();
      return;
   }
   DEPOqueryEnd();
   
   //Check if we already have an available FULL backup for the current TIMELINE
   sprintf(query,"select count(*) from %s.backups b where cid=%s and bcktyp='FULL' and bcksts=0 and timeline=%ld",
                 varGet(GVAR_DEPUSER),
                 varGet(GVAR_CLUCID),
                 pgGetCurrentTimeline());
   int nTL=DEPOquery(query,0);
   int nBckFULL=DEPOgetInt(0,0);
   if (nBckFULL == 0)
   { 
      DEPOqueryEnd();
      ERROR(ERR_FULLREQUIRED,"There is NO FULL backup for timeline %ld. Perform a full backup prior taking WAL backups.\n",pgGetCurrentTimeline());
      memEndModule();
      return;
   }
   DEPOqueryEnd();
   
   VERBOSE("Scanning directory '%s'\n",wal_folder);
   char *fullname=memAlloc(1024);

   long total_backup_pieces=0;
   long total_backup_files=0;
   long total_backup_size=0;
   long total_wal_deleted=0;
   long backup_piece_maxsize=getHumanSize(varGet(GVAR_CLUBACKUP_MAXSIZE));                                                         // prepare the limit for MAXSIZE
   long backup_piece_maxfiles=varGetLong(GVAR_CLUBACKUP_MAXFILES);                                                                 // prepare the limit for MAXFILES
   if (backup_piece_maxfiles <= 0) backup_piece_maxfiles=DEFAULT_MAXFILES;                                                         
   if (backup_piece_maxsize <= 0)  backup_piece_maxsize=DEFAULT_MAXSIZE;                                                           
   long piece_maxfiles=0;                                                                                                          // Reset piece file count limit
   long piece_maxsize=0;                                                                                                           // Reset piece size limit
     
   time_t oldest=0;
   time_t youngest=0;
   Array *WALlist=arrayCreate(ARRAY_NO_ORDER);
   int first_task=1;
   int errcnt=0;
   while ( (ep = readdir (dp)) && (errcnt == 0) )                                                                                  // Get  file from directory
   {
      if (ep->d_type == DT_DIR) { continue; }; // Skip all directories                                                             // Is it a directory ?
      sprintf(fullname,"%s/%s",wal_folder,ep->d_name);
      int rcs=stat(fullname, &st);                                                                                                 // Get file properties
      time_t system_time = time(NULL);
      long file_age_min=(system_time - st.st_mtime) / 60;                                                                          // To have result in minutes 
      if (oldest == 0) { oldest=st.st_mtime;youngest=st.st_mtime;}
                  else { if (oldest > st.st_mtime) oldest=st.st_mtime;
                         if (youngest < st.st_mtime) youngest=st.st_mtime;
                       };
      sprintf(query,"select count(*) from %s.backup_wals where cid=%s and walfile='%s'",
                    varGet(GVAR_DEPUSER),
                    varGet(GVAR_CLUCID),
                    ep->d_name);
      int nRows=DEPOquery(query,0);
      int fnd=0;
      if (nRows == 0) { fnd=0; }
                 else { fnd=DEPOgetInt(0,0); };
      DEPOqueryEnd();
      if (fnd == 0)
      {
         if (first_task == 1)
         {
            RECMClearPieceList();                                                                                                  // cleanup zip_filelist
            cluster_getCurrentWAL();                                                                                               // Get Current WAL
            createSequence(1);                                                                                                     // Reset piece sequence
            createUID();                                                                                                           // Create Backup UID
            VERBOSE("Starting WAL backup of '%s'\n",varGet(SYSVAR_LASTLSN));
            varAdd(RECM_WALSTART,varGet(SYSVAR_LASTLSN));
            first_task=0;
            total_backup_pieces++;
            // Create record for backup in 'running' state
            sprintf(query,"insert into %s.backups (cid,bcktyp,bcksts,bck_id,bcktag,hstname,pgdata,pgversion,"
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
                 total_backup_size,
                 total_backup_files,
                 varGet(GVAR_CLUTZ),
                 varGet(GVAR_CLUDS),
                 destination_folder);
            DEPOquery(query,0);
            DEPOqueryEnd();

         }
         sprintf(query,"insert into %s.backup_wals(cid,bck_id,walfile,ondisk,bckcount,edate)"
                                   " values (%s,'%s','%s',1,1,to_timestamp(%lu))",
                       varGet(GVAR_DEPUSER),
                       varGet(GVAR_CLUCID),
                       varGet(RECM_BCKID),
                       ep->d_name,
                       st.st_mtime);
         nRows=DEPOquery(query,0);
         DEPOqueryEnd();
         if ((piece_maxsize >= backup_piece_maxsize) || (piece_maxfiles >= backup_piece_maxfiles))
         {
            if (RECMclose(piece_maxsize,piece_maxfiles)==false) errcnt++;
            nextSequence();
            piece_maxfiles=0;                                                                                                      // Reset piece file count limit
            piece_maxsize=0;                                                                                                       // Reset piece size limit
            total_backup_pieces++;                                                                                                 // statistics : number of pieces
         };

         VERBOSE("Copying WAL file '%s'\n",fullname);
         if (RECMaddFile(ep->d_name,fullname,wal_folder) == true)
         {
            total_backup_files++;
            total_backup_size+=st.st_size;
            piece_maxfiles++;                                                                                                      // file count cummuled (for statistics)
            piece_maxsize=piece_maxsize+st.st_size;                                                                                // file size cummuled (for statistics)
            if (varGetLong(GVAR_CLUAUTODELWAL) > -1 && file_age_min > varGetLong(GVAR_CLUAUTODELWAL))                              // If 'DELWAL' is > -1
            { 
               ArrayItem *itm=malloc(sizeof(ArrayItem));                                                                           // Remember the file to delete
               itm->key=strdup(ep->d_name);                                                                                        // Because we need to delete files
               itm->data=NULL;                                                                                                     // after the ZIP file is close.
               arrayAdd(WALlist,itm);
            }
         }
         else
         {
            errcnt++;
         };
      }
      else
      { // File is already backuped. It can be deleted
         VERBOSE("WAL file '%s' already backuped (%d)\n",ep->d_name,fnd);
         if (varGetLong(GVAR_CLUAUTODELWAL) > -1 && file_age_min > varGetLong(GVAR_CLUAUTODELWAL))
         {
            ArrayItem *itm=malloc(sizeof(ArrayItem));
            itm->key=strdup(ep->d_name);
            itm->data=NULL;
            arrayAdd(WALlist,itm);
         }
      };
   };

   if (total_backup_files > 0)
   {
      if (RECMclose(piece_maxsize,piece_maxfiles) == false) errcnt++;
      int bcksts=RECM_BACKUP_STATE_AVAILABLE;
      if (errcnt > 0) bcksts=RECM_BACKUP_STATE_FAILED;
      
      // Load special information for restore
      int rcx=CLUquery("select setting from pg_settings where name='log_timezone'",0);
      varAdd(GVAR_CLUTZ,CLUgetString(0,0));
      CLUqueryEnd();
      rcx=CLUquery("select setting from pg_settings where name='DateStyle'",0);
      varAdd(GVAR_CLUDS,CLUgetString(0,0));
      CLUqueryEnd();
      
      char *wrkstr=malloc(128);
      strftime(wrkstr, 49, "%Y-%m-%d %H:%M:%S", localtime(&youngest));                                                             // Get Youngest WAL file
      strftime(wrkstr, 49, "%Y-%m-%d %H:%M:%S", localtime(&oldest));                                                               // Get Oldest WAL file
      free(wrkstr);
      sprintf(query,"update %s.backups set bcksts=%d,bcksize=%ld,pcount=%ld,edate=current_timestamp "                              // Update backup record
                    " where cid=%s and bck_id='%s'",
                    varGet(GVAR_DEPUSER),
                    bcksts,
                    total_backup_size,
                    total_backup_files,
                    varGet(GVAR_CLUCID),
                    varGet(RECM_BCKID));
      DEPOquery(query,0);
      DEPOqueryEnd();
      RECMsetProperties(varGet(RECM_BCKID));
   }
   else
   {
     VERBOSE("No WAL to backup.\n");
   }
   closedir (dp);
   
   // Delete all WAL already backuped.
   if (opt_deleteWAL == DELETE_WAL_ON_BACKUP && errcnt == 0)                                                                       // Now, we can delete files
   {                                                                                                                               // we have in the list
      ArrayItem *itm=arrayFirst(WALlist);
      while (itm != NULL)
      {
          sprintf(fullname,"%s/%s",wal_folder,itm->key);
          VERBOSE ("deleting WAL '%s'\n",fullname);
          unlink(fullname);
          total_wal_deleted++;
          arrayDelete(WALlist,itm);
          itm=arrayFirst(WALlist);
      };
   }
   else
   {
      if (errcnt == 0) { WARN(WRN_KEEPWAL,"WAL not deleted by request.\n"); };
   }
   free(WALlist);
   sprintf(query,"update %s.clusters set lstwal=(select max(bdate) from %s.backups b "                                             // Update backup record in DEPOSIT
                    " where bcktyp='WAL' and cid=%s) where cid=%s;",
                 varGet(GVAR_DEPUSER),
                 varGet(GVAR_DEPUSER),
                 varGet(GVAR_CLUCID),
                 varGet(GVAR_CLUCID));
   DEPOquery(query,1);
   DEPOqueryEnd();
   if (errcnt ==0 && total_backup_files > 0)                                                                                       // Display some statistics 
   {
      INFO("Backup UID : %s\n",varGet(RECM_BCKID));
      INFO("Total pieces ......... : %-8ld\n",total_backup_pieces);
      INFO("Total files backuped.. : %-8ld\n",total_backup_files);
      if (varGetLong(GVAR_CLUAUTODELWAL) > -1)
      {
         INFO("Total files deleted... : %-8ld (Files aged %ld minute(s) or more)\n",total_wal_deleted,varGetLong(GVAR_CLUAUTODELWAL));
      }
      INFO("Total Backup size .... : %-8ld (%s)\n",total_backup_size, DisplayprettySize(total_backup_size) );
   }
   

   // Free allocated resources
   memEndModule();
   globalArgs.verbosity=saved_verbose;
}
