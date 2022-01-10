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
/**********************************************************************************/

static unsigned long  total_backup_size=0;                                                                                         // Backup statistic counters
static unsigned long  total_backup_files=0;
static unsigned long  total_backup_pieces=0;
static unsigned long  total_skipped_size=0;
static unsigned long  total_skipped_files=0;
static unsigned long  grand_total_files=0;
static long piece_maxfiles=0;                                                                                                      // Reset piece file count limit
static long piece_maxsize=0;                                                                                                       // Reset piece size limit

/**********************************************************************************/
/* Analyze ROOT directory ( PGDATA ) adn all subdirectires                        */
/* Skip all LINKS, and process all 'pg_tblspc' separately                         */
/**********************************************************************************/
int FullBackupScanDirectory(const char *rootdir,int level,int opt_noindex)
{
   int rc;
   int errcnt=0;
   if (level == 0)
   {
      piece_maxfiles=0;                                                                                                            // Reset piece file count limit
      piece_maxsize=0;                                                                                                             // Reset piece size limit
   };

   DIR *dp;
   struct dirent *ep;
   struct stat st;
   char *link_dir=malloc(1024);
   char *link_name=malloc(1024);
   dp = opendir (rootdir);
   if (dp != NULL)
   {
      long backup_piece_maxsize=getHumanSize(varGet(GVAR_CLUBACKUP_MAXSIZE));                                                      // prepare the limit for MAXSIZE
      long backup_piece_maxfiles=varGetLong(GVAR_CLUBACKUP_MAXFILES);                                                              // prepare the limit for MAXFILES
      if (backup_piece_maxfiles <= 0) backup_piece_maxfiles=DEFAULT_MAXFILES;
      if (backup_piece_maxsize <= 0)  backup_piece_maxsize=DEFAULT_MAXSIZE;
   
      while ( (ep = readdir (dp)) && errcnt == 0)                                                                                  // Get  file from directory
      {
#define SKIP_NONE 0
#define SKIP_FILE 1
#define SKIP_LOG  2
         int skip_file=SKIP_NONE;
         if (strcmp(ep->d_name,".")              == 0) { continue; };
         if (strcmp(ep->d_name,"..")             == 0) { continue; };
         if (strcmp(ep->d_name,"postmaster.pid") == 0) { continue; };                                                              // Skip file 'postmaster.id'
         char *fullname=malloc(strlen(rootdir) + strlen(ep->d_name) + 2);
         char *lnk_name=NULL;
         sprintf(fullname,"%s/%s",rootdir,ep->d_name);
         TRACE("Fullname='%s'\n",fullname);
         switch(ep->d_type)
         {
         case DT_LNK : TRACE("IS A LINK\n");                                                                                       // Follow link
                       lnk_name=malloc(1024);
                       memset(lnk_name,0,1024);
                       rc = readlink (fullname, lnk_name, 1024);
                       VERBOSE("Following LINK '%s' (%s)\n",ep->d_name,lnk_name);
                       char *full_linkname=malloc(2048);
                       sprintf(full_linkname,"%s@?@%s",fullname,lnk_name);
                       if (RECMaddDirectory(full_linkname)==false)
                       { 
                          errcnt++;
                       }                                                                                                           // link_name@?@full_path
                       else 
                       { 
                          errcnt+=FullBackupScanDirectory(fullname,level+1,opt_noindex);                                           // Go down directory tree
                       };   
                       break;                                                                                                      // Skip all LNK
         case DT_DIR : TRACE("DT_DIR\n");
                       if ((piece_maxsize >= backup_piece_maxsize) || (piece_maxfiles >= backup_piece_maxfiles))
                       {
                          if (RECMclose(piece_maxsize,piece_maxfiles)==false) errcnt++;
                          nextSequence();
                          piece_maxfiles=0;                                                                                        // Reset piece file count limit
                          piece_maxsize=0;                                                                                         // Reset piece size limit
                          total_backup_pieces++;                                                                                   // statistics : number of pieces
                       };
                       int rcd=stat(fullname, &st);                                                                                // Get file properties
                       if (RECMaddDirectory(fullname)==false) errcnt++;
                       errcnt+=FullBackupScanDirectory(fullname,level+1,opt_noindex);                                              // Go down directory tree
                       break;
         case DT_REG : TRACE("DT_REG\n");
                       index_details *ndx=NULL;
                       if (opt_noindex == true)
                       {
                          char *fsm_file=malloc(1024);                                                                             // Each index may have an 'xxx_vm' file ( visibility map file )
                          TRACE("alloc fsm_file\n");
                          ArrayItem *itm=arrayFirst(oid_indexes);
                          TRACE("First oid_indexes\n");
                          if (itm != NULL) ndx=(index_details *)itm->data;
                          while (itm != NULL && skip_file == SKIP_NONE)
                          {
                             if (strcmp(ndx->ndx_rfn,ep->d_name) == 0) { skip_file=SKIP_FILE;break; }                              // Exclude index file
                             strcpy(fsm_file,ndx->ndx_rfn);
                             strcat(fsm_file,"_vm");
                             if (strcmp(fsm_file,ep->d_name) == 0) { skip_file=SKIP_FILE;break; }                                  // Exclude visibility map file attached to index also
                             itm=arrayNext(oid_indexes);
                             if (itm != NULL) ndx=(index_details *)itm->data;
                          };
                          free(fsm_file);
                       };
                       TRACE("stat file '%s'\n",fullname);
                       
                       if (strlen(varGet(RECM_LOGDIR)) != 0)
                       {
                          char *PGLOG=malloc(1024);
                          if (strchr(varGet(RECM_LOGDIR),'/') == NULL)                                   // This is a subfolder below PGDATA
                          {
                             sprintf(PGLOG,"%s/%s",varGet(GVAR_CLUPGDATA),varGet(RECM_LOGDIR));
                          }
                          else
                          {
                             sprintf(PGLOG,"%s",RECM_LOGDIR);
                          }
                          if (strcmp(rootdir,PGLOG) == 0) { skip_file=SKIP_LOG; };                                                 // Skip any files within folder '<PGDATA>/log' (extracted from pg_settings, parameter 'log_directory' */
                          free(PGLOG);
                       }
                       int rcs=stat(fullname, &st);                                                                                // Get file properties
                       switch(skip_file)
                       {
                       case SKIP_NONE : TRACE("skip_file == SKIP_NONE\n");
                                        piece_maxfiles++;                                                                          // file count cummuled (for statistics)
                                        grand_total_files++;
                                        piece_maxsize=piece_maxsize+st.st_size;                                                    // file size cummuled (for statistics)
                                        
                                        
                                        TRACE("zipadd_file (%s,%s)\n",ep->d_name,fullname);
                                        if (RECMaddFile(ep->d_name,fullname,varGet(GVAR_CLUPGDATA))==false) errcnt++;
                                        total_backup_size+=st.st_size;
                                        total_backup_files++;
                                        if ((piece_maxsize >= backup_piece_maxsize) || (piece_maxfiles >= backup_piece_maxfiles))
                                        {
                                           if (RECMclose(piece_maxsize,piece_maxfiles)==false) errcnt++;
                                           nextSequence();
                                           piece_maxfiles=0;                                                                       // Reset piece file count limit
                                           piece_maxsize=0;                                                                        // Reset piece size limit
                                           total_backup_pieces++;                                                                  // statistics : number of pieces
                                        }
                                        break;
                       case SKIP_FILE : TRACE("skip_file == SKIP_FILE\n");
                                        TRACE("Excluding index file '%s' from backup\n",ep->d_name); 
                                        total_skipped_size+=st.st_size;                                                            // statistics : number of bytes skipped
                                        total_skipped_files++;                                                                     // statistics : number of file skipped (indexes)
                                        break;
                       case SKIP_LOG :  TRACE("skip_file == SKIP_LOG\n");
                                        TRACE("Excluding log file '%s' from backup\n",ep->d_name);
                                        total_skipped_size+=st.st_size;                                                            // statistics : number of bytes skipped
                                        total_skipped_files++;                                                                     // statistics : number of file skipped (indexes)
                                        break;
                       };
                       break;
         } // end SWITCH
         free(fullname);
      };
      closedir (dp);

      free(link_dir);
      free(link_name);
   }                       
   else
   {
      errcnt++;
      ERROR(ERR_DIROPEN,"Cannot access directory '%s': %s\n",varGet(GVAR_CLUPGDATA),strerror(errno));
      free(link_dir);
      free(link_name);
      return(errcnt);
   }

   if (level == 0 && errcnt==0)
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
      if (piece_maxfiles > 0)
      {
         if (RECMclose(piece_maxsize,piece_maxfiles)==false) errcnt++;
         piece_maxfiles=0;                                                                                                         // Reset piece file count limit
         piece_maxsize=0;                                                                                                          // Reset piece size limit
         total_backup_pieces++;                                                                                                    // statistics : number of pieces
      };
   
      if (errcnt ==0)
      {
         INFO("Backup UID : %s\n",varGet(RECM_BCKID));
         INFO("Total pieces ......... : %-8ld\n",total_backup_pieces);
         INFO("Total files backuped.. : %-8ld\n",grand_total_files);
         INFO("Total Backup size .... : %-8ld (%s)\n",total_backup_size, DisplayprettySize(total_backup_size) );
         if (total_skipped_files > 0)
         {
            INFO("Total bytes skipped... : %-8ld (%ld MB)\n",total_skipped_size,prettySize(total_skipped_size,'M') );
            INFO("Total files skipped... : %-8ld\n",total_skipped_files);
         }
      };
   }
   return(errcnt);
}


void COMMAND_BACKUPFULL(int idcmd,const char *command_line)
{
   long BACKUP_OPTIONS=0; 
   
   if (isConnectedAndRegistered() == false) return;
   
   if (IsClusterEnabled(varGetLong(GVAR_CLUCID)) == false) 
   {
      WARN(ERR_SUSPENDED,"Cluster '%s' is 'DISABLED'. Operation ignored.\n",varGet(GVAR_CLUNAME));
      return;
   }
   
   total_backup_size=0;
   total_backup_files=0;
   total_backup_pieces=0;
   total_skipped_size=0;
   total_skipped_files=0;
   grand_total_files=0;
   piece_maxfiles=0;                                                                                                               // Reset piece file count limit
   piece_maxsize=0; 


   if (varGetInt(GVAR_COMPLEVEL) > -1){ BACKUP_OPTIONS=backupSetOption(BACKUP_OPTIONS,BACKUP_OPTION_COMPRESSED); };  
   if (optionIsSET("opt_noindex"))    { BACKUP_OPTIONS=backupSetOption(BACKUP_OPTIONS,BACKUP_OPTION_NOINDEX);    };  
   if (optionIsSET("opt_exclusive"))  { BACKUP_OPTIONS=backupSetOption(BACKUP_OPTIONS,BACKUP_OPTION_EXCLUSIVE);  };  
   if (optionIsSET("opt_lock"))       { BACKUP_OPTIONS=backupSetOption(BACKUP_OPTIONS,BACKUP_OPTION_KEEPFOREVER);  };
   //INFO("BACKUP_OPTIONS=%ld\n",BACKUP_OPTIONS);
   int opt_verbose=optionIsSET("opt_verbose");                                                                                     // Change verbosity
   int saved_verbose=globalArgs.verbosity;
   globalArgs.verbosity=opt_verbose;
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
   
   /* CHANGE: 0xA0005 : Keep in mind the subfolder 'log' to skip any log files */
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
   PGresult *clu_res;
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

   int errcnt=FullBackupScanDirectory(varGet(GVAR_CLUPGDATA),0,optionIsSET("opt_noindex")); 

   // Initiate end backup 'pg_stop_backup'
   /* CHANGE: 0xA0006 implement '/exclusive' option */
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
      
      
   // Write 'backups' information into database
   int bcksts=RECM_BACKUP_STATE_AVAILABLE;
   if (errcnt > 0) bcksts=RECM_BACKUP_STATE_FAILED;
   sprintf(query,"update %s.backups set bcksts=%d,bcksize=%ld,pcount=%ld,edate=now(),ewall='%s',ewalf='%s',ztime='%s',sdate='%s' where bck_id='%s'",
                       varGet(GVAR_DEPUSER),
                       bcksts,
                       total_backup_size,
                       total_backup_pieces,
                       varGet(RECM_STOP_LSN),
                       varGet(RECM_STOP_WALFILE),
                       varGet(GVAR_CLUTZ),
                       varGet(GVAR_CLUDS),
                       varGet(RECM_BCKID));
   
   rc = DEPOquery(query,0);
   DEPOqueryEnd();

   sprintf(query,"update %s.clusters set lstfull=(select max(bdate) from %s.backups b "
                 " where bcktyp='FULL' and cid=%s) where cid=%s;",
                 varGet(GVAR_DEPUSER),
                 varGet(GVAR_DEPUSER),
                 varGet(GVAR_CLUCID),
                 varGet(GVAR_CLUCID));
   DEPOquery(query,1);
   DEPOqueryEnd();

   if (errcnt == 0) RECMsetProperties(varGet(RECM_BCKID));
   
   if (optionIsSET("opt_wal") == true && errcnt==0)                                                                                // /wal : call 'backup wal'
   {
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
   

   // Restore verbosity
   globalArgs.verbosity=saved_verbose;
   memEndModule();
}
