/**********************************************************************************/
/* Backup FULL command                                                            */
/* Usage:                                                                         */
/*    backup full                                                                 */                 
/*      Available Option(s):                                                      */
/*         /cleanup                  Launch 'delete backup/obsolete'              */
/*         /exclusive                launch backup in 'exclusive' mode            */
/*         /lock                     Disable retention policy for this backup     */
/*         /noindex                  Remove indexes from backup                   */
/*         /verify                   Launch 'verify backup'                       */
/*         /wal                      Launch 'backup wal'                          */
/*         /verbose                  .                                            */
/*      Available Qualifier(s):                                                   */
/*         /tag=<String>             Define custom keyword to backup.             */
/*         /directory=<path>         Use a different location for this backup     */
/*         /parallel=n               Launch backup with multiple threads          */
/**********************************************************************************/


static long long limitSize=0;
static long long cumulSize=0;
static long long cumulFiles=0;


#define SKIP_NONE 0
#define SKIP_FILE 1
#define SKIP_LOG  2


/********************************************************************************/
/* Evaluate the size of each pieces,depending on the parallelism choosen        */
/* the function return the number of pieces to create                           */
/********************************************************************************/
int prepareTreePieces (TreeEntry *tree,long limitSize,long limitFile)
{
   long cumulSize=0;
   long cumulFiles=0;
   int SEQ=1;
   TreeItem* ti=tree->first;
   while (ti != NULL)
   {
      ti->used=0;
      switch(ti->flag)
      {
         case TREE_ITEM_DIR  : ti->sequence=SEQ;
                               break;
         case TREE_ITEM_LINK : ti->sequence=SEQ;
                               break;
         case TREE_ITEM_FILE : cumulSize=cumulSize+ti->size;
                               cumulFiles++;
                               ti->sequence=SEQ;
                               if (cumulSize > limitSize || cumulFiles >= limitFile)
                               {
                                  SEQ++;
                                  cumulFiles=0;
                                  cumulSize=0;
                               };

                               break;
      }
      ti=ti->next;
   };
   if (cumulFiles == 0 && cumulSize == 0 && SEQ > 1) SEQ--; 
   return(SEQ);
}

/********************************************************************************/
/* Load all directory tree                                                      */
/********************************************************************************/
int loadDirectoryTreeObjects(TreeEntry *tree,const char *pRootdir)
{
   char right_path[1024];
   struct stat   file_st;
   int            rc;
   DIR           *dp;
   struct dirent *ep;
   ArrayItem     *itm;
   index_details *ndx;
   TreeItem *saved;
   char          *lnk_name;
   char          *fullname=malloc(1024);
   char          *full_linkname;
   int            skip_file;
   char          *fsm_file;
   char          *rootdir;
   int ROOT_TYPE;
   
   TRACE ("ROOT Entry='%s'\n",pRootdir);
   if (strstr(pRootdir,"@?@") != NULL)
   {
      ROOT_TYPE=TREE_ITEM_LINK;
      char *s=strstr(pRootdir,"@?@");
      s[0]=0x00;
      s+=3;
      strcpy(right_path,s);
      rootdir=(char *)pRootdir;
   }
   else
   {
      ROOT_TYPE=TREE_ITEM_DIR;
      rootdir=(char *)pRootdir;
      right_path[0]=0x00;
   }
   TRACE ("ROOT directory='%s' | Right part='%s'\n",rootdir,right_path);   
   rc=stat(rootdir, &file_st);
   TRACE("Root Dir Stat '%s' [rc=%d (errno %d)] [%u | %u | %u]\n",rootdir,rc,errno,file_st.st_mode,file_st.st_uid,file_st.st_gid);
   treeAddItem(tree,pRootdir,ROOT_TYPE,&file_st); 
   
   dp = opendir(rootdir);
   if (dp != NULL)
   {
      while ( (ep = readdir (dp)))
      {
         if (strcmp(ep->d_name,".")              == 0) { continue; };                                                              // ignore some files
         if (strcmp(ep->d_name,"..")             == 0) { continue; };
         sprintf(fullname,"%s/%s",rootdir,ep->d_name);
         rc=stat(fullname, &file_st);
         TRACE("stat '%s' [rc=%d (errno %d) : '%s'(%u) / %u / %u]\n",fullname,rc,errno,maskCreate(file_st.st_mode),file_st.st_mode,file_st.st_uid,file_st.st_gid);
         switch(ep->d_type)
         {
         case DT_LNK : lnk_name=malloc(1024);                                                                                      // Follow LINK like a folder
                       full_linkname=malloc(1024);
                       memset(lnk_name,0,1024);                                                                                    // Translate the link
                       rc = readlink (fullname, lnk_name, 1024);
                       sprintf(full_linkname,"%s@?@%s",fullname,lnk_name);
                       TRACE("[B]LINK='%s'\n",fullname);
                       saved=tree->currentFolder;
                       treeAddItem(tree,full_linkname,TREE_ITEM_LINK,&file_st);                                                    // Add Link path to Tree
                       loadDirectoryTreeObjects(tree,full_linkname);                 // Follow the link normally
                       if (saved != NULL) tree->currentFolder=saved;
                       free(full_linkname);
                       free(lnk_name);
                       break;
         case DT_DIR : TRACE("[C]DIR ='%s'\n",fullname);
                       saved=tree->currentFolder;
//                       treeAddItem(tree,fullname,TREE_ITEM_DIR,&file_st);                                                                // Add path to Tree
                       loadDirectoryTreeObjects(tree,fullname);
                       if (saved != NULL) tree->currentFolder=saved;
                       break;
         case DT_REG : skip_file=SKIP_NONE;
                       fsm_file=malloc(1024);                                                                                      // Each index may have an 'xxx_vm' file ( visibility map file )
                       itm=arrayFirst(oid_indexes);
                       if (itm != NULL)
                       {
                          ndx=(index_details *)itm->data;
                          while (itm != NULL && skip_file == SKIP_NONE)
                          {
                             if (strcmp(ndx->ndx_rfn,ep->d_name) == 0) { skip_file=SKIP_FILE;break; }                              // Exclude index file
                             strcpy(fsm_file,ndx->ndx_rfn);
                             strcat(fsm_file,"_vm");
                             if (strcmp(fsm_file,ep->d_name) == 0) { skip_file=SKIP_FILE;break; }                                  // Exclude visibility map file attached to index also
                             itm=arrayNext(oid_indexes);
                             if (itm != NULL) ndx=(index_details *)itm->data;
                          };
                       };
                       free(fsm_file);
                       if (skip_file == SKIP_NONE)                                                                                 // Check if we can omit cluster log files
                       {
                          if (strlen(varGet(RECM_LOGDIR)) != 0)
                          {
                             char *PGLOG=malloc(1024);
                             if (strchr(varGet(RECM_LOGDIR),'/') == NULL)                                                          // This is a subfolder below PGDATA
                             {
                                sprintf(PGLOG,"%s/%s",varGet(GVAR_CLUPGDATA),varGet(RECM_LOGDIR));
                             }
                             else
                             {
                                sprintf(PGLOG,"%s",RECM_LOGDIR);
                             }
                             if (strcmp(rootdir,PGLOG) == 0) { skip_file=SKIP_LOG; };                                              // Skip any files within folder '<PGDATA>/log' (extracted from pg_settings, parameter 'log_directory' */
                             free(PGLOG);
                          }
                       }
                       if (skip_file == SKIP_NONE && strstr (fullname,"archive_status/") != NULL)
                       {
                          if (strstr(fullname,".ready") != NULL) skip_file=SKIP_FILE;
                          if (strstr(fullname,".done")  != NULL) skip_file=SKIP_FILE;
                       }
                       if (skip_file == SKIP_NONE && strcmp (ep->d_name,"postmaster.pid") == 0) skip_file=SKIP_FILE;              // Never backup this file

                       switch(skip_file)
                       {
                          case SKIP_NONE : treeAddItem(tree,ep->d_name,TREE_ITEM_FILE,&file_st);
                                           verification_limitFile++;                                     //Counters for validation at the end of the backup
                                           verification_limitSize+=file_st.st_size;                          //Counters for validation at the end of the backup
                                           break;
                          case SKIP_FILE : TRACE("skip_file == SKIP_FILE\n");
                                           TRACE("Excluding index file '%s' from backup\n",ep->d_name); 
                                           break;
                          case SKIP_LOG :  TRACE("skip_file == SKIP_LOG\n");
                                           TRACE("Excluding log file '%s' from backup\n",ep->d_name);
                                           break;
                       };
                       break;
         } // end SWITCH
      };
      closedir (dp);
   }
   free(fullname);
   return(true);
}

/********************************************************************************/
/* Create the RECM file by a thread                                             */
/********************************************************************************/
void *threadCreateBackupPiece(void *dummyPtr)
{
   ThreadData *td=(ThreadData *)dummyPtr;
   char *rootdir=strdup(varGet(GVAR_CLUPGDATA));
   pthread_mutex_lock( &mutex1 );
   char *tarFileName=malloc(1024);
   char *filename=malloc(1024);
   char *lastPath=malloc(1024);
   sprintf(tarFileName,"%s/%s_%ld_%s.recm",                                                                                          // Only 1 piece
                       varGet(GVAR_BACKUPDIR),
                       varGet(RECM_BCKID),
                       td->pieceNumber,
                       varGet(RECM_BCKTYPE));
   
   td->zH=RECMcreate(tarFileName,td->pieceNumber,varGetInt(GVAR_COMPLEVEL));
   td->zH->parallel_count=td->parallel_count; // Need to tell RECM files we are working with threads
   
   td->zH->start_time=time(NULL);
   
   
   pthread_mutex_unlock( &mutex1 );
   
   long thread_file_processed=0;
   long thread_size_processed=0;
   int errcnt=0; 
   
   struct stat   file_st;
   stat(rootdir, &file_st);
   if (RECMaddDirectory(td->zH,rootdir,file_st.st_mode,file_st.st_uid,file_st.st_gid)==false) errcnt++;
   TRACE("%d[Worker %ld]ADD ROOT DIRECTORY '%s' (errnt=%d)\n",td->idThread,td->pieceNumber,rootdir,errcnt);
   
   lastPath[0]=0x00;                                                            
   TreeItem* ti=td->pgdataTree->first;
   
   while (ti != NULL && errcnt == 0)
   {
      if (ti->sequence == td->pieceNumber)
      {
         if (ti->path != NULL)
         {
            strcpy(lastPath,ti->path->name);                                    // Remember where we are path
//            if (ti->path->used == 0)
//            {
               if (RECMaddDirectory(td->zH,lastPath,ti->path->st_mode,ti->path->st_uid,ti->path->st_gid)==false) errcnt++;
               TRACE("%d[Worker %ld]ADD DIRECTORY '%s' (errnt=%d)\n",td->idThread,td->pieceNumber,lastPath,errcnt);
//            };
         };
         if (ti->flag != TREE_ITEM_FILE)
         {
            if (RECMaddDirectory(td->zH,ti->name,ti->st_mode,ti->st_uid,ti->st_gid)==false) errcnt++;
            TRACE("%d[Worker %ld]ADD DIRECTORY II '%s' (errnt=%d)\n",td->idThread,td->pieceNumber,ti->name,errcnt);
            strcpy(lastPath,ti->name);
         }
         if (ti->flag == TREE_ITEM_FILE && ti->used == 0)
         {
            thread_file_processed++;
            thread_size_processed+=ti->size;
            sprintf(filename,"%s/%s",lastPath,ti->name);
            //if (file_exists(filename) == false) { printf("******** MISSING : %s\n",filename); };
            if (RECMaddFile(td->zH,ti->name,filename,rootdir,ti->st_mode,ti->st_uid,ti->st_gid)==false) errcnt++;
            TRACE("%d[Worker %ld]ADD FILE '%s' (errnt=%d)\n",td->idThread,td->pieceNumber,filename,errcnt);
            ti->used++;
         }
      };
      ti=ti->next;
   }

   TRACE("%d[Worker %ld] count=%lu size=%lu\n",td->idThread,td->pieceNumber,thread_file_processed,thread_size_processed);
   if (RECMclose(td->zH,thread_size_processed,thread_file_processed)==false) errcnt++;

   RECMUpdateDepositAtClose(td->zH,thread_size_processed,thread_file_processed);

   pthread_mutex_lock( &mutex1 );

   if (errcnt > 0)
   {
      td->rc=1;
      td->state=THREAD_END_FAILED;
   }
   else
   {
      td->state=THREAD_END_SUCCESSFULL;
      td->rc=0;
      thread_all_file_processed+=thread_file_processed;
      thread_all_size_processed+=thread_size_processed;
   };

   pthread_mutex_unlock( &mutex1 );

   free(lastPath);
   free(filename);
   free(tarFileName);
   free(rootdir);
   return(NULL);
}

/********************************************************************************/
/* Start a Thread                                                               */
/********************************************************************************/
int threadStartFULL(int idThread, TreeEntry * tree,int pieceNumber,const char *bckid,const char *bcktyp,int parallel_count)
{
   pthread_mutex_lock( &mutex1 );
   threads[idThread].idThread=idThread;
   threads[idThread].state=THREAD_RUNNING;
   threads[idThread].pieceNumber=pieceNumber;                                           // ID of the Piece to create
   threads[idThread].pgdataTree=tree;                                                   // File table
   threads[idThread].parallel_count=parallel_count;
   strcpy(threads[idThread].bck_id,bckid);                                              // Backup UID                                              // Sequence used by this thread
   strcpy(threads[idThread].bck_typ,bcktyp);                                            // Backup Type
   threads[idThread].rc=pthread_create(&threads[idThread].threadID, NULL, threadCreateBackupPiece,&threads[idThread]);
   if (threads[idThread].rc != 0)
   {
      int errn=errno;
      ERROR(ERR_STHFAILED,"Starting thread [%d] failed (rc=%d / errno=%d]%s )\n",idThread,threads[idThread].rc,errn,strerror(errn));
   };
   pthread_mutex_unlock( &mutex1 );
   return(threads[idThread].rc);
}


void COMMAND_BACKUPFULL(int idcmd,char *command_line)
{
   long BACKUP_OPTIONS=0; 
   
   if (isConnectedAndRegistered() == false) return;
   
   if (IsClusterEnabled(varGetLong(GVAR_CLUCID)) == false) 
   {
      WARN(ERR_SUSPENDED,"Cluster '%s' is 'DISABLED'. Operation ignored.\n",varGet(GVAR_CLUNAME));
      return;
   }
   
   if (varGetInt(GVAR_COMPLEVEL) > -1){ BACKUP_OPTIONS=backupSetOption(BACKUP_OPTIONS,BACKUP_OPTION_COMPRESSED); };  
   if (optionIsSET("opt_noindex"))    { BACKUP_OPTIONS=backupSetOption(BACKUP_OPTIONS,BACKUP_OPTION_NOINDEX);    };  
   if (optionIsSET("opt_exclusive"))  { BACKUP_OPTIONS=backupSetOption(BACKUP_OPTIONS,BACKUP_OPTION_EXCLUSIVE);  };  
   if (optionIsSET("opt_lock"))       { BACKUP_OPTIONS=backupSetOption(BACKUP_OPTIONS,BACKUP_OPTION_KEEPFOREVER);};
   
   if (optionIsSET("opt_verbose") == true) globalArgs.verbosity=true;                                                              // Set Verbosity

   if (varExist(GVAR_BACKUPDIR) == false)
   {
      ERROR(ERR_NOBACKUPDIR,"Backup directory not set.(See show cluster).\n");
      return;
   }
   memBeginModule();

   char *destination_folder=memAlloc(1024);
   varTranslate(destination_folder,varGet(GVAR_BACKUPDIR),1);

   
   if (directory_exist(destination_folder) == false) 
   { 
      mkdirs(destination_folder,maskTranslate(varGet(GVAR_CLUMASK_BACKUP)));                                                       // Create backup directory
   }
   if (directory_exist(destination_folder) == false)
   {
      ERROR(ERR_INVDIR,"Invalid Backup directory '%s'\n",destination_folder);
      memEndModule();
      return;
   }

   varAdd(RECM_BCKTYPE,RECM_BCKTYP_FULL);

   char *qtag=varGet("qal_tag");
   char *tag=memAlloc(128);
   if (strcmp(qtag,QAL_UNSET) == 0) { varTranslate(tag,"{YYYY}{MM}{DD}_{CluName}_FULL",0); }
                               else { strcpy(tag,qtag); }
   if (isValidName(tag) == false)
   {
      ERROR(ERR_INVCHARINNAME,"Invalid character in tag '%s'\n",tag);
      memEndModule();
      return;
   };

   char *query=memAlloc(1024);
   
   // Check if a backup is already running (RECM_BACKUP_STATE_RUNNING)
   // We wait maximum 2 minutes before to abandon...
   sprintf(query,"select count(*) from %s.backups b where b.cid=%s and b.bcktyp in ('FULL','WAL') and bcksts=%d",
                 varGet(GVAR_DEPUSER),
                 varGet(GVAR_CLUCID),
                 RECM_BACKUP_STATE_RUNNING);
   int nRUN=DEPOquery(query,0);
   nRUN=DEPOgetInt(0,0);
   DEPOqueryEnd();
   int maxWait=12; // wait 120 seconds max before to abort
   while (nRUN > 0 && maxWait > 0)
   {
      VERBOSE("A Backup is currently running...Waiting few seconds for the end of the backup...\n");
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

   
   // Some parameters must be set to have the backup working correctly
   // - 1 - : 'archive_command' parameter must be set
   // - 2 - : 'wal_level' parameter must be set to 'replica'

   int rc=CLUquery("SELECT setting from pg_settings where name='archive_command'",0);
   char *p=CLUgetString(0,0);
   if (p == NULL || strlen(p) == 0 || strstr(p,"%p") == NULL)
   {
      ERROR(ERR_MISSCONFIG,"Missing configuration. parameter 'archive_command' must be set to have backups working properly.\n");
      memEndModule();
      return;
   };
   CLUqueryEnd();
   rc=CLUquery("SELECT setting from pg_settings where name='wal_level'",0);
   p=CLUgetString(0,0);
   if (p == NULL || strlen(p) == 0 || strcmp(p,"replica") != 0)
   {
      ERROR(ERR_MISSCONFIG,"Missing configuration. parameter 'wal_level' must be set to 'replica' to have backups working properly.\n");
      memEndModule();
      return;
   };
   CLUqueryEnd();

   rc=CLUquery("SELECT pg_is_in_backup(),pg_backup_start_time(),now()-pg_backup_start_time()",0);
   if (strcmp(CLUgetString(0,0),"t") == 0)
   {
      INFO("A backup is already running, Started at '%s' ( %s )\n",
           CLUgetString(0,1),
           CLUgetString(0,2));
      CLUqueryEnd();
      memEndModule();
      return;
   }
   CLUqueryEnd();


   RECMClearPieceList();                                                                                                           // cleanup zip_filelist
   
   rc=CLUquery("select setting from pg_settings where name='log_directory'",0);
   if (rc != 1) { varAdd(RECM_LOGDIR,""); }
           else { varAdd(RECM_LOGDIR,CLUgetString(0,0)); };
   CLUqueryEnd();

   if (optionIsSET("opt_switch") == true) { cluster_doSwitchWAL(false); }                                                          // Create a WAL before to start backup
                                     else { cluster_getCurrentWAL();};
   createSequence(1);
   createUID();                                                                                                                    // Create UNIQUE IDENTIFIER. Define 'uid' variable
   varAdd(RECM_WALSTART,varGet(SYSVAR_LASTLSN));

   /* CHANGE: 0xA0006 implement '/exclusive' option */
   if (optionIsSET("opt_exclusive") == true)
   {
      sprintf(query,"SELECT pg_start_backup('%s',false,true)",varGet(RECM_BCKID));                                                 // BACKUP in EXCLUSIVE mode
   }
   else
   {
      sprintf(query,"SELECT pg_start_backup('%s',false,false)",varGet(RECM_BCKID));                                                // BACKUP in NON EXCLUSIVE mode
   };
   rc=CLUquery(query,0);
   if (rc != 1)
   {
      ERROR(ERR_BEGBCKFAIL,"Could not initiate start backup : %s",CLUgetErrorMessage());
      INFO("Execute 'CANCEL BACKUP' to stop any running PostgreSQL backup.");
      CLUqueryEnd();
      memEndModule();
      return;
   };
   varAdd(RECM_START_LSN,CLUgetString(0,0));                                                                                       // In output, we got START LSN
   TRACE("Starting backup at LSN %s\n",varGet(RECM_START_LSN));
   CLUqueryEnd();
   
   sprintf(query,"SELECT pg_walfile_name('%s')",varGet(RECM_START_LSN));                                                           // Keep in mind the WAL file needed at START of backup
   rc=CLUquery(query,0);
   varAdd(RECM_START_WALFILE,CLUgetString(0,0));
   CLUqueryEnd();

   VERBOSE("Starting backup at LSN %s, WALfile '%s'\n",varGet(RECM_START_LSN),varGet(RECM_START_WALFILE));
   if (optionIsSET("opt_exclusive") == true) VERBOSE("Backup running in EXCLUSIVE mode.\n");
   
   // Create Backup record in 'RUNNING' state
   sprintf(query,"insert into %s.backups (cid,bcktyp,bcksts,bck_id,bcktag,hstname,pgdata,pgversion,timeline,options,bcksize,pcount,bwall,bwalf,bckdir)"
                 " values (%s,'%s',%d,'%s','%s','%s','%s',%d,%ld,%ld,0,0,'%s','%s','%s')",
                 varGet(GVAR_DEPUSER),
                 varGet(GVAR_CLUCID),
                 varGet(RECM_BCKTYPE),
                 RECM_BACKUP_STATE_RUNNING,
                 varGet(RECM_BCKID),
                 asLiteral(tag),
                 varGet(GVAR_CLUHOST),
                 varGet(GVAR_CLUPGDATA),
                 varGetInt(GVAR_CLUVERSION),
                 pgGetCurrentTimeline(),
                 BACKUP_OPTIONS,
                 varGet(RECM_START_LSN),
                 varGet(RECM_START_WALFILE),
                 destination_folder);
   // Keep in mind user indexes
   DEPOquery(query,0);
   DEPOqueryEnd();
   if (oid_indexes == NULL) { oid_indexes=arrayCreate(ARRAY_NO_ORDER); }                                                           // Create array for indexes
                          else { ArrayItem* itm=arrayLast(oid_indexes);                                                            // Reuse array, but cleanup all items from array
                                 while (itm != NULL)
                                 {
                                    arrayDetach(oid_indexes,itm);
                                    free(itm);
                                    itm=arrayLast(oid_indexes);
                                 }
                               };
   // Loop on ALL databases
   char *dbname=memAlloc(256);
   char *dboid=memAlloc(10);
   int dbtyp;
   char *connectString=memAlloc(1024);
   //PGresult *clu_res;
   PGresult *subres;
   PGconn     *dblist_cnx=NULL;
   PGresult   *dblist_res=NULL;
   int rows_db=CLUquery("with masterdb as (select datname,oid from pg_database where oid=(select min(oid) from pg_database where oid > (select oid from pg_database where datname='template0')))"
                       " select d.oid,d.datname,case when d.oid = m.oid then 0 else case d.datistemplate when true then 1 else 2 end end dbtype" 
                       " from pg_database d,masterdb m order by d.datname",0);
   //int rows_db = PQntuples(clu_res);
   for(int itemIndex=0; itemIndex < rows_db; itemIndex++)
   {
      strcpy(dboid, CLUgetString( itemIndex, 0));
      strcpy(dbname,CLUgetString( itemIndex, 1));
      dbtyp=CLUgetInt( itemIndex, 2);
      TRACE("dboid=%s dbname=%s dbtyp=%d\n",dboid,dbname,dbtyp);
      switch(dbtyp)
      {
      case 0 : // Master database (postgres by default). We will save all tablespaces
               subres=PQexec(clu_cnx,"select oid,spcname,pg_tablespace_location(oid) from pg_tablespace where spcname not in ('pg_global','pg_default')");
               if (PQresultStatus(subres) == PGRES_TUPLES_OK)
               {
                  int rows = PQntuples(subres);
                  for(int i=0; i< rows; i++)
                  {
                     sprintf(query,"insert into %s.backup_tbs (cid,bck_id,tbs_oid,tbs_nam,tbs_loc) values (%s,'%s',%ld,'%s','%s')",
                                       varGet(GVAR_DEPUSER),
                                       varGet(GVAR_CLUCID),
                                       varGet(RECM_BCKID),
                                       atol(PQgetvalue(subres, i, 0)),
                                       PQgetvalue(subres, i, 1),
                                       PQgetvalue(subres, i, 2));
                     TRACE("master query=%s\n",query);
                     DEPOquery(query,1);
                     DEPOqueryEnd();
                  };
               };
               PQclear(subres);
               break;
      case 2 : // User's database
               TRACE("Database '%s'[%s] (%d) is part of backup\n",dbname,dboid,dbtyp);
               VERBOSE("Analyzing Database '%s' objects\n",dbname);
               sprintf(query,"insert into %s.backup_dbs (cid,bck_id,db_nam,db_oid,db_typ) values (%s,'%s','%s',%s,%d)",
                                 varGet(GVAR_DEPUSER),
                                 varGet(GVAR_CLUCID),
                                 varGet(RECM_BCKID),
                                 dbname,
                                 dboid,
                                 dbtyp);                                                                //      -- 0=system db // 1=template // 2=user db
               TRACE("QUERY(Database names):%s\n",query);
               DEPOquery(query,1);
               DEPOqueryEnd();
               sprintf(connectString,"dbname=%s user=%s password=%s host=%s port=%s",
                                     dbname,
                                     varGet(GVAR_CLUUSER),
                                     varGet(GVAR_CLUPWD),
                                     varGet(GVAR_CLUHOST),
                                     varGet(GVAR_CLUPORT));
               dblist_cnx = PQconnectdb(connectString);
               if (PQstatus(dblist_cnx) == CONNECTION_OK)
               {
                  // Memorize Table names
                  sprintf(query,"select n.nspname,c.relname,c.relfilenode,c.reltablespace from pg_class c,pg_namespace n"
                                " where c.relkind='r' and c.relnamespace = n.oid"
                                "   and n.nspname not in ('pg_catalog','information_schema') order by 1,2");
                  dblist_res=PQexec(dblist_cnx,query);
                  TRACE("QUERY(table names):%s\n",query);
                  if (PQresultStatus(dblist_res) == PGRES_TUPLES_OK)
                  {
                     int rows = PQntuples(dblist_res);
                     for(int i=0; i< rows; i++)
                     {
                        sprintf(query,"insert into %s.backup_tbl(cid,bck_id,tab_dbid,tab_sch,tab_nam,tab_fid,tbs_oid) values ('%s','%s',%s,'%s','%s',%s,%s)",
                                         varGet(GVAR_DEPUSER),
                                         varGet(GVAR_CLUCID),
                                         varGet(RECM_BCKID),
                                         dboid,
                                         PQgetvalue(dblist_res, i, 0),
                                         PQgetvalue(dblist_res, i, 1),
                                         PQgetvalue(dblist_res, i, 2),
                                         PQgetvalue(dblist_res, i, 3));
                        DEPOquery(query,1);
                        DEPOqueryEnd(); 
                     };
                  }
               }
               PQclear(dblist_res);
               // Loading indexes in memory (if '/noindex' option is set)
               if (optionIsSET("opt_noindex") == true)
               {
                  dblist_res = PQexec(dblist_cnx,"select r.relfilenode,s.nspname,u.rolname,r.relname,i.tablename" \
                                                 "  from pg_class r,pg_authid u,pg_namespace s,pg_indexes i"  \
                                                 "  where r.relnamespace=s.oid" \
                                                 "    and r.relowner=u.oid" \
                                                 "    and r.relkind='i'" \
                                                 "    and i.schemaname=s.nspname" \
                                                 "    and i.indexname=r.relname" \
                                                 "    and s.nspname not in ('pg_catalog','pg_toast')" \
                                                 "    ");
                  if (PQresultStatus(dblist_res) == PGRES_TUPLES_OK)
                  {
                     int rows = PQntuples(dblist_res);
                     for(int i=0; i < rows; i++)
                     {
                        index_details *ndx=malloc(sizeof(index_details));
                        ndx->ndx_cat=strdup( dbname );
                        ndx->ndx_rfn=strdup( PQgetvalue(dblist_res, i,0) );
                        ndx->ndx_sch=strdup( PQgetvalue(dblist_res, i,1) );
                        ndx->ndx_own=strdup( PQgetvalue(dblist_res, i,2) );
                        ndx->ndx_nam=strdup( PQgetvalue(dblist_res, i,3) );
                        ndx->ndx_tbl=strdup( PQgetvalue(dblist_res, i,4) );
                        ArrayItem *itmR=malloc(sizeof(ArrayItem));
                        itmR->data=(char *)ndx;
                        arrayAdd(oid_indexes,itmR);
                        TRACE("Discover index '%s' from '%s' (%s)\n",ndx->ndx_nam,ndx->ndx_cat,ndx->ndx_rfn);
                     };
                  };
                  PQclear(dblist_res);
               };
               break;
      }; // Only db type 2
   }; // End ofLOOP on DBs
   CLUqueryEnd();
   

   thread_all_file_processed=0;
   thread_all_size_processed=0;
   verification_limitFile=0; // This value will be compared with the result of 'thread_all_file_processed' content, as control
   verification_limitSize=0; // This value will be compared with the result of 'thread_all_size_processed' content, as control
   threadInitalizeTable();                                                                                                         // Initialize Thread table

   // Now, we are going to scan all sub-directories

   TreeEntry *pgdataTree=treeCreate();                                                                                             // Initialize File tree 
   loadDirectoryTreeObjects(pgdataTree,varGet(GVAR_CLUPGDATA));                                                                    // Load Directory tree 

   // Parallel qualifier
   int parallel=1;
   if (qualifierIsUNSET("qal_parallel") == false)                                                                                  // Validate the '/parallel=n'...must be in range 1..MAX_THREADS
   {
      parallel=varGetInt("qal_parallel");
      if (parallel < 1) parallel=1;
      if (parallel > MAX_THREADS) parallel=MAX_THREADS;
      if (parallel > 1) VERBOSE("PARALLEL backup requested with %d thread(s)\n",parallel);
   };

   // MAXSIZE limit
   long limitFile=varGetLong(GVAR_CLUBACKUP_MAXFILES);
   if (limitFile < HARD_LIMIT_MINFILES) limitFile=HARD_LIMIT_MINFILES;
   long limitSize=getHumanSize(varGet(GVAR_CLUBACKUP_MAXSIZE));                                                                    // prepare the limit for MAXSIZE
   if (limitSize < getHumanSize(HARD_LIMIT_MINSIZE)) limitSize=getHumanSize(HARD_LIMIT_MINSIZE);
   if (parallel > 1)
   {
      long sections=(pgdataTree->totalsize / limitSize);
      if (sections < parallel) { limitSize=(pgdataTree->totalsize / parallel); }
                          else { limitSize=(pgdataTree->totalsize / sections); };
      VERBOSE("Each backup pieces will have an average size of %s\n",DisplayprettySize(limitSize));
   }
   int number_of_pieces=prepareTreePieces (pgdataTree,limitSize,limitFile);                                                         // Slice the directory tree

   if (parallel > 1) { VERBOSE("Will create %d pieces by %d thread(s)\n",number_of_pieces,parallel); }
                else { VERBOSE("Will create %d pieces\n",number_of_pieces); };

   // Now we are going to create each backup piece by threads.
   int current_piece=1; // Scan array and assign a 'Piece Number' to an available thread
   int errcnt=0;
   while (current_piece <= number_of_pieces && errcnt == 0)
   {
      int nbrRunningProcess=threadCountRunning(parallel);
      while (nbrRunningProcess >= parallel) 
      {
         sleep(1);
         nbrRunningProcess=threadCountRunning(parallel);
      };
      int numThread=ThreadGetAvailableProcess(parallel);
      if (threadStartFULL(numThread,pgdataTree,current_piece,varGet(RECM_BCKID),varGet(RECM_BCKTYPE),parallel) != 0)
      {
         errcnt++;
      };
      current_piece++;
   };

   while (threadCountRunning(parallel) > 0) { sleep(1); };                                                                         // Wait the end of all threads

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
   
   // Initiate end backup 'pg_stop_backup'
   if (optionIsSET("opt_exclusive") == true)
   {
      rc=CLUquery("select replace(split_part(cast (pg_stop_backup('t') as varchar),',',1),'(','');",0);
   }
   else
   {
      rc=CLUquery("select replace(split_part(cast (pg_stop_backup('f') as varchar),',',1),'(','');",0);
   };
   if (rc != 1)
   {
      ERROR(ERR_ENDBCKFAIL,"Could not initiate stop backup : %s",CLUgetErrorMessage());
   };
   varAdd(RECM_STOP_LSN,CLUgetString(0,0));
   CLUqueryEnd();

   sprintf(query,"SELECT pg_walfile_name('%s')",varGet(RECM_STOP_LSN));
   rc=CLUquery(query,0);
   varAdd(RECM_STOP_WALFILE,CLUgetString(0,0));
   VERBOSE("Ending backup at '%s' (%s) \n",
           varGet(RECM_STOP_LSN),
           varGet(RECM_STOP_WALFILE));
   CLUqueryEnd();
   
   
   // Load special information for restore
   int rcx=CLUquery("select setting from pg_settings where name='log_timezone'",0);
   varAdd(GVAR_CLUTZ,CLUgetString(0,0));
   CLUqueryEnd();
   rcx=CLUquery("select setting from pg_settings where name='DateStyle'",0);
   varAdd(GVAR_CLUDS,CLUgetString(0,0));
   CLUqueryEnd();

   // Write Indexes
   if (errcnt == 0 && arrayCount(oid_indexes) > 0)
   {
      char *query=malloc(8192);
      char *ddl=malloc(8192);
      index_details *ndx=NULL;
      ArrayItem *itm=arrayFirst(oid_indexes);
      if (itm != NULL) ndx=(index_details *)itm->data;
      while (itm != NULL)
      {
         sprintf(query,"insert into %s.backup_ndx(cid,bck_id,bd_dbn,bd_sch,bd_tbl,bd_ndx) values (%s,'%s','%s','%s','%s','%s')",
                       varGet(GVAR_DEPUSER),
                       varGet(GVAR_CLUCID),
                       varGet(RECM_BCKID),
                       ndx->ndx_cat,
                       ndx->ndx_sch,
                       ndx->ndx_tbl,
                       ndx->ndx_nam);
         rc=DEPOquery(query,3);
         DEPOqueryEnd();
         if (rc == -1) { ERROR(ERR_DBQRYERROR,"Deposit query error\n"); 
                         errcnt++;
                       };
         itm=arrayNext(oid_indexes);
         if (itm != NULL) ndx=(index_details *)itm->data;
      };
      free(ddl);
      free(query);
   }
   // Write 'backups' information into database
   int bcksts;
   if (errcnt > 0) { bcksts=RECM_BACKUP_STATE_FAILED; }
              else { bcksts=RECM_BACKUP_STATE_AVAILABLE;
                     RECMsetProperties(varGet(RECM_BCKID));
                   };
   sprintf(query,"update %s.backups set bcksts=%d,bcksize=%ld,pcount=%d,edate=now(),ewall='%s',ewalf='%s',ztime='%s',sdate='%s' where bck_id='%s'",
                       varGet(GVAR_DEPUSER),
                       bcksts,
                       thread_all_size_processed,
                       number_of_pieces,
                       varGet(RECM_STOP_LSN),
                       varGet(RECM_STOP_WALFILE),
                       varGet(GVAR_CLUTZ),
                       varGet(GVAR_CLUDS),
                       varGet(RECM_BCKID));
   rc=DEPOquery(query,0);
   DEPOqueryEnd();

   sprintf(query,"update %s.clusters set lstfull=(select max(bdate) from %s.backups b "
                 " where bcktyp='FULL' and cid=%s) where cid=%s;",
                 varGet(GVAR_DEPUSER),
                 varGet(GVAR_DEPUSER),
                 varGet(GVAR_CLUCID),
                 varGet(GVAR_CLUCID));
   DEPOquery(query,1);
   DEPOqueryEnd();
   
   treeDestroy(pgdataTree);

   sprintf(query,"select coalesce(to_char(bdate,'%s'),''),coalesce(to_char(edate,'%s'),''),trunc(coalesce(EXTRACT(EPOCH FROM (edate - bdate)),0)),"
                 " pcount"
                 " from %s.backups  where cid=%s and bck_id='%s'",
                  varGet(GVAR_CLUDATE_FORMAT),
                  varGet(GVAR_CLUDATE_FORMAT),
                  varGet(GVAR_DEPUSER),
                  varGet(GVAR_CLUCID),
                  varGet(RECM_BCKID));   
   rc=DEPOquery(query,1);
   INFO("Backup %s UID '%s' succesfully created.\n",varGet(RECM_BCKTYPE),varGet(RECM_BCKID));
   INFO("Started at: '%s'\n",DEPOgetString(0,0));
   INFO("       LSN: '%s' ( %s )\n",varGet(RECM_START_LSN),varGet(RECM_START_WALFILE));
   INFO("Ended at..: '%s'\n",DEPOgetString(0,1));
   INFO("       LSN: '%s' ( %s )\n",varGet(RECM_STOP_LSN),varGet(RECM_STOP_WALFILE));
   INFO("Total pieces ......... : %s\n",DEPOgetString(0,3));
   INFO("Total files backuped.. : %lu\n", thread_all_file_processed);
   INFO("Total Backup size .... : %lu ( %s )\n",thread_all_size_processed,DisplayprettySize(thread_all_size_processed));
   INFO("Elapsed Time : %s\n",DisplayElapsed(DEPOgetLong(0,2)));

   DEPOqueryEnd();

   if (optionIsSET("opt_rp") == true && errcnt==0)                                                                            // /cleanup : call 'delete backup/obsolete'
   { 
      TRACE("calling COMMAND_CREATERP\n");

      SETOption("opt_temporary");
      qualifierSET("qal_name",varGet(RECM_BCKID));     // Restore point will have the Backup UID as restore point
      COMMAND_CREATERP(CMD_CREATERP,command_line);
   };

   if (optionIsSET("opt_wal") == true && errcnt==0)                                                                                // /wal : call 'backup wal'
   {                                                                                                                               // We keep '/parallel' for WAL backup also
      TRACE("calling COMMAND_BACKUPWAL\n");
      //      varAdd("opt_verbose",VAR_SET_VALUE);
      UNSETOption("opt_switch");
      UNSETOption("opt_exclusive");
      UNSETOption("opt_noindex");
      qualifierSET("qal_tag",tag);
      COMMAND_BACKUPWAL(CMD_BACKUPWAL,command_line);
   };
   if (optionIsSET("opt_verify") == true && errcnt==0)                                                                            // /Verify : call 'verify backup'
   { 
      TRACE("calling COMMAND_VERIFYBACKUP\n");
//      varAdd("opt_verbose",VAR_SET_VALUE);
      qualifierUNSET("qal_tag");
      qualifierUNSET("qal_before");
      qualifierUNSET("qal_after");
      qualifierUNSET("qal_uid");
      COMMAND_VERIFYBACKUP(CMD_VERIFYBACKUP,command_line);
   };
   if (optionIsSET("opt_cleanup") == true && errcnt==0)                                                                            // /cleanup : call 'delete backup/obsolete'
   { 
      TRACE("calling COMMAND_DELETEBACKUP\n");
//      varAdd("opt_verbose",VAR_SET_VALUE);
      qualifierUNSET("qal_before");
      qualifierUNSET("qal_after");
      qualifierUNSET("qal_uid");
      SETOption("opt_obsolete");
      UNSETOption("opt_incomplete");
      UNSETOption("opt_failed");
      UNSETOption("opt_noremove");
      COMMAND_DELETEBACKUP(CMD_DELETEBACKUP,command_line);
   };
   memEndModule();
   qualifierUNSET("qal_tag");
   qualifierUNSET("qal_directory");
   qualifierUNSET("qal_parallel");
}
