/**********************************************************************************/
/* Backup META command                                                            */
/* Usage:                                                                         */
/*    backup meta                                                                 */                 
/*      Available Option(s):                                                      */
/*         /lock                     Disable retention policy for this backup     */
/*         /verbose                  .                                            */
/*      Available Qualifier(s):                                                   */
/*         /tag=<String>             Define custom keyword to backup.             */
/*         /db=<dblist>              list all databases to proceed                */
/**********************************************************************************/
/*
@command backup meta
@definition
Generated METADATA (DDL) backup of PostgreSQL database.
This is a backup of the structure, without data.
(See also {&restore meta&} for restoring METADATA files)

@option "/verbose" "Display more details"
@option "/tag=TAG"           "Change default TAG value. (Default is 'YYYY_MM_DD_HHhMM'"
@option "/db=LIST"           "Specify the databases to generate the META backup"
@option "/lock"              "Disable retention policy for this backup"
@example
@inrecm backup meta /verbose 
@out recm-inf: Creating meta sql file '/Library/PostgreSQL/12/data/recm_meta/metadata_CLU12_demo.sql' for database 'demo'
@out recm-inf: Creating piece '/Volumes/pg_backups/0001633f12c81ef49b38_1_META.recm'
@out recm-inf: Creating meta sql file '/Library/PostgreSQL/12/data/recm_meta/metadata_CLU12_depo_prod.sql' for database 'depo_prod'
@out recm-inf: Creating meta sql file '/Library/PostgreSQL/12/data/recm_meta/metadata_CLU12_limkydb.sql' for database 'limkydb'
@out recm-inf: Creating meta sql file '/Library/PostgreSQL/12/data/recm_meta/metadata_CLU12_recm_db2.sql' for database 'recm_db2'
@out recm-inf: Creating meta sql file '/Library/PostgreSQL/12/data/recm_meta/metadata_CLU12_recmdb.sql' for database 'recmdb'
@out recm-inf: Creating meta sql file '/Library/PostgreSQL/12/data/recm_meta/metadata_CLU12_recmrepo.sql' for database 'recmrepo'
@out recm-inf: Backup Piece /Volumes/pg_backups/0001633f12c81ef49b38_1_META.recm FileSize=9824 (Compression ratio: 80.29%)
@out recm-inf: Backup META UID '0001633f12c81ef49b38' succesfully created.
@out recm-inf: Total pieces ......... : 1       
@out recm-inf: Total files backuped.. : 6       
@out recm-inf: Total Backup size .... : 49843    (48 KB)
@inrecm
@end
*/

void COMMAND_BACKUPMETA(int idcmd,char *command_line)
{
   long BACKUP_OPTIONS=0; 
//   zip_t *zipHandle=NULL;
   
   if (isConnectedAndRegistered() == false) return;

   if (IsClusterEnabled(varGetLong(GVAR_CLUCID)) == false) 
   {
      WARN(ERR_SUSPENDED,"Cluster '%s' is 'DISABLED'. Operation ignored.\n",varGet(GVAR_CLUNAME));
      return;
   }

   
   if (varExist(GVAR_BACKUPDIR) == false)
   {
      ERROR(ERR_NOBACKUPDIR,"Backup directory not set.(See show cluster);\n");
      return;
   }
   
   if (directory_exist(varGet(GVAR_BACKUPDIR)) == false) 
   { 
      mkdirs(varGet(GVAR_BACKUPDIR),maskTranslate(varGet(GVAR_CLUMASK_BACKUP) )); 
   }
   if (directory_exist(varGet(GVAR_BACKUPDIR)) == false) 
   { 
      ERROR(ERR_INVDIR,"Invalid backup directory '%s'\n",varGet(GVAR_BACKUPDIR)); 
      return;
   }

   memBeginModule();
   
   if (varGetInt(GVAR_COMPLEVEL) > -1){ BACKUP_OPTIONS=backupSetOption(BACKUP_OPTIONS,BACKUP_OPTION_COMPRESSED); };  
   if (optionIsSET("opt_lock"))       { BACKUP_OPTIONS=backupSetOption(BACKUP_OPTIONS,BACKUP_OPTION_KEEPFOREVER);  };
   
   char *destination_folder=memAlloc(1024);

   /* CHANGE : 0xA0004 Check if RECM_META_DIR exist */
   char *RECM_META_DIR=getenv(ENVIRONMENT_RECM_META_DIR);                                                                          // Check if environment variable RECM_META_DIR exist
   if (RECM_META_DIR != NULL) { strcpy(destination_folder,RECM_META_DIR); }                                                        
                         else { sprintf(destination_folder,"%s/recm_meta",varGet(GVAR_CLUPGDATA)); }                               // By default, METADATA are generated in subdirectory 'recm_meta'
   if (directory_exist(destination_folder) == false)                                                                               
   {                                                                                                                               
      mkdirs(destination_folder,maskTranslate(varGet(GVAR_CLUMASK_BACKUP) ));                                                      // Create directory if it does not exist
   };                                                                                                                              
   if (directory_exist(destination_folder) == false)                                                                               // Abort process if could not create diectory
   {
      ERROR(ERR_INVMETADIR,"Invalid META data directory '%s'\n",destination_folder);
      memEndModule();
      return; 
   };

   if (optionIsSET("opt_verbose") == true) globalArgs.verbosity=true;                                                              // Set Verbosity

   varAdd(RECM_BCKTYPE,RECM_BCKTYP_META);
    
   long TL=pgGetCurrentTimeline();
   
   char *qtag=asLiteral(varGet("qal_tag"));
   char *tag=memAlloc(128);
   if (strcmp(qtag,QAL_UNSET) == 0) { varTranslate(tag,"{YYYY}{MM}{DD}_{CluName}_META",0); }
                               else { strcpy(tag,qtag); };
   if (isValidName(tag) == false)                                                                                                  // Reject some characters
   {
      ERROR(ERR_INVCHARINNAME,"Invalid character in tag '%s'\n",tag);
      memEndModule();
      return;
   };
   
   Array *dblist=arrayCreate(ARRAY_UP_ORDER);

   int list_minus=0;
   int list_plus=0;
   char *database_list=memAlloc(1024); 
   database_list[0]=0x00;
   if (qualifierIsUNSET("qal_db") == false)
   {
      strcpy(database_list,varGet("qal_db"));                                                                                      // We accept multiple string
      strcat(database_list,",");                                                                                                   // spearated by comma
      char *itm=database_list;
      char *virgule=strchr(itm,',');
      while (virgule != NULL)
      {
         char *spaces=virgule;
         virgule[0]=0x00;
         spaces--;
         while (spaces[0] == ' ') { spaces--; };
         spaces++;
         spaces[0]=0x00;
         if (strlen(itm) > 0)
         {
            if (itm[0] == '-') { list_minus++; } else { list_plus++; };
            arrayAddKey(dblist,itm,NULL);
         };
         virgule++;
         if (virgule[0] != 0x00)
         {
            itm=virgule;
            while (itm[0] == ' ' && itm[0] != 0x00) { itm++; };
            if (itm != NULL) { virgule=strchr(itm,','); } else {virgule=NULL;};
         }
         else
         { virgule=NULL; };
      };
   }
   else
   {
      database_list[0]=0x00;
   }
   
   RECMClearPieceList();
   createSequence(1);                                                                                                              // Reset Backup Piece sequence
   createUID();                                                                                                                    // Create UID     

   char *tarFileName=malloc(1024);
   sprintf(tarFileName,"%s/%s_1_%s.recm",                                                                                          // Only 1 piece
                       varGet(GVAR_BACKUPDIR),
                       varGet(RECM_BCKID),
                       varGet(RECM_BCKTYPE));
   
   RECMDataFile *hDF=NULL;
   // Loop on ALL databases
   char *dbname=memAlloc(256);
   char *dboid=memAlloc(10);
   char *query=memAlloc(1024);
   int dbtyp;
   int rows_db=CLUquery("with masterdb as (select datname,oid from pg_database where oid=(select min(oid) from pg_database where oid > (select oid from pg_database where datname='template0')))"
                       " select d.oid,d.datname,case when d.oid = m.oid then 0 else case d.datistemplate when true then 1 else 2 end end dbtype" 
                       " from pg_database d,masterdb m order by d.datname",0);
   long total_backup_size=0;
   long total_backup_files=0;

   int errcnt=0;
   int itemIndex=0;
   while (itemIndex < rows_db && errcnt == 0)
   {
      strcpy(dboid, CLUgetString( itemIndex, 0));
      strcpy(dbname,CLUgetString( itemIndex, 1));
      dbtyp=CLUgetInt( itemIndex, 2);
      TRACE("dboid=%s dbname=%s dbtyp=%d\n",dboid,dbname,dbtyp);
      int accept=false;
      switch(dbtyp)
      {
      case 0 : // Master database : skipped
               break;
      case 2 : // User's database
               // call pg_dump
               // Accepted syntax : 'dbname1,dbname2,-dbname3', to select 2 db and reject the last one
               
               if (list_plus == 0 && list_minus == 0)
               { accept=true; }
               else
               { 
                 if (list_plus == 0 && list_minus > 0)
                 {
                    accept=true;                                                                                                   // Accept specified database by default
                    ArrayItem *itm=arrayFirst(dblist);
                    while (itm != NULL && accept == true)
                    {
                       char *minus=itm->key;
                       char *nam=itm->key;
                       if (minus[0] == '-')                                                                                        // Reject database
                       {
                          nam++;
                          if (strcmp(nam,dbname) == 0) { accept=false; };            
                       }
                       itm=arrayNext(dblist);
                    }
                  }
                  else
                  {
                    int fnd=false;
                    ArrayItem *itm=arrayFirst(dblist);
                    while (itm != NULL && fnd == false)
                    {
                       char *minus=itm->key;
                       char *nam=itm->key;
                       if (minus[0] == '-' )                                                                                       // Reject database            
                       {
                          nam++;
                          if (strcmp(nam,dbname) == 0) { accept=false;fnd=true; }; 
                       }
                       else
                       {
                          if (minus[0] == '+' )                                                                                    // Accept database            
                          {
                             nam++;
                             if (strcmp(nam,dbname) == 0) { accept=true;fnd=true; }; 
                          }
                          else
                          {
                             if (strcmp(nam,dbname) == 0) { accept=true;fnd=true; }; 
                          }
                       }
                       itm=arrayNext(dblist);
                     }
                  }
               };
               if (accept == true)
               {
                  char *dumpfile=malloc(1024);
                  sprintf(dumpfile,"%s/metadata_%s_%s.sql",destination_folder,varGet(GVAR_CLUNAME),dbname);
                  char *pg_dump=malloc(1024);
                  sprintf(pg_dump,"%s/pg_dump --no-password",varGet(SYSVAR_PGBIN));
                  //if (varIsSET(GVAR_CLUHOST) == true) { strcat(pg_dump," --host=");strcat(pg_dump,varGet(GVAR_CLUHOST)); };
                  if (varIsSET(GVAR_CLUPORT) == true) { strcat(pg_dump," --port=");strcat(pg_dump,varGet(GVAR_CLUPORT)); };
                  if (varIsSET(GVAR_CLUUSER) == true) { strcat(pg_dump," --username=");strcat(pg_dump,varGet(GVAR_CLUUSER)); };
                  strcat(pg_dump," --dbname=");
                  strcat(pg_dump,dbname);
                  strcat(pg_dump," --schema-only --file=");
                  strcat(pg_dump,dumpfile);
                  TRACE("pg_dump=%s\n",pg_dump);
                  VERBOSE("Creating meta sql file '%s' for database '%s'\n",dumpfile,dbname);
                  shell(pg_dump,true);
                  if (hDF == NULL)
                  {
                     // Load special information for restore
                     int rcx=CLUquery("select setting from pg_settings where name='log_timezone'",0);
                     varAdd(GVAR_CLUTZ,CLUgetString(0,0));
                     CLUqueryEnd();
                     rcx=CLUquery("select setting from pg_settings where name='DateStyle'",0);
                     varAdd(GVAR_CLUDS,CLUgetString(0,0));
                     CLUqueryEnd();
                     sprintf(query,"insert into %s.backups (cid,bcktyp,bcksts,bck_id,bcktag,hstname,pgdata,pgversion,"
                                   "timeline,options,bcksize,pcount,ztime,sdate,bckdir)"
                                   " values (%s,'%s',%d,'%s','%s','%s','%s',%d,%ld,%ld,%ld,1,'%s','%s','%s')",
                                   //        cid typ   id   tag  hst  dta  ver tm opt siz 
                                   varGet(GVAR_DEPUSER),
                                   varGet(GVAR_CLUCID),
                                   varGet(RECM_BCKTYPE),
                                   RECM_BACKUP_STATE_RUNNING,
                                   varGet(RECM_BCKID),
                                   tag,
                                   varGet(GVAR_CLUHOST),
                                   varGet(GVAR_CLUPGDATA),
                                   varGetInt(GVAR_CLUVERSION),
                                   TL,
                                   BACKUP_OPTIONS,
                                   0L,
                                   varGet(GVAR_CLUTZ),
                                   varGet(GVAR_CLUDS),
                                   varGet(GVAR_BACKUPDIR));
                     rcx=DEPOquery(query,0);
                     DEPOqueryEnd();
                     hDF=RECMcreate(tarFileName,1,varGetInt(GVAR_COMPLEVEL));
                     free(tarFileName);  
                  };
                  struct stat st;
                  int rc=stat(dumpfile, &st);
                  TRACE("stat '%s'[rc=%d]\n",dumpfile,rc);
                  if (RECMaddFile(hDF,getFileNameOnly(dumpfile),dumpfile,varGet(GVAR_CLUPGDATA),st.st_mode,st.st_uid,st.st_gid) == false) errcnt++;
                  total_backup_files++;
                  total_backup_size+=st.st_size;
                  free(dumpfile);
               };
               break;
      }; // Only db type 2
      itemIndex++;
   }; // End ofLOOP on DBs
   CLUqueryEnd();

   if (total_backup_files > 0 && errcnt == 0)
   {
      RECMclose(hDF,total_backup_size,total_backup_files);
      RECMUpdateDepositAtClose(hDF,total_backup_size,total_backup_files);
      
      // Update status of backup
      sprintf(query,"update %s.backups set bcksts=%d,bcksize=%ld,pcount=%d,edate=current_timestamp "                                  // Update backup record to become 'AVAILABLE'
                    " where cid=%s and bck_id='%s'",
                    varGet(GVAR_DEPUSER),
                    RECM_BACKUP_STATE_AVAILABLE,
                    total_backup_size,
                    1,
                    varGet(GVAR_CLUCID),
                    varGet(RECM_BCKID));
      DEPOquery(query,0);      

      // Update cluster record for last backup config date.
      sprintf(query,"update %s.clusters set lstmeta=(select max(bdate) from %s.backups b "
                    " where bcktyp='%s' and cid=%s) where cid=%s",
                      varGet(GVAR_DEPUSER),
                      varGet(GVAR_DEPUSER),
                      varGet(RECM_BCKTYPE),
                      varGet(GVAR_CLUCID),
                      varGet(GVAR_CLUCID));
      DEPOquery(query,1);
      DEPOqueryEnd();     
      
      RECMsetProperties(varGet(RECM_BCKID));
      INFO("Backup %s UID '%s' succesfully created.\n",varGet(RECM_BCKTYPE),varGet(RECM_BCKID));
      INFO("Total pieces ......... : %-8ld\n",1L);
      INFO("Total files backuped.. : %-8ld\n",total_backup_files);
      INFO("Total Backup size .... : %-8ld (%s)\n",total_backup_size, DisplayprettySize(total_backup_size) );
   }
   else
   {
     if (errcnt == 0) VERBOSE("No META to backup.\n");
   }
   memEndModule();
   return;
}
