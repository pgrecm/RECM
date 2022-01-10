/**********************************************************************************/
/* SHOW CLUSTER command                                                           */
/* Usage:                                                                         */
/*    show cluster                                                                */                 
/*      Available Option(s):                                                      */
/*         /backupdir               Display BACKUP destination folder             */
/*         /cfg                     Retention for FULL backups                    */
/*         /compression             Enable compression(0-9)/Disable=-1            */
/*         /db                      Database used to connect to the cluster       */
/*         /date_format             Date format                                   */
/*         /delwal                  Automatically delete WAL after backups        */
/*         /description             Cluster Description                           */
/*         /full                    Retention for FULL backups                    */
/*         /ip                      IP of the cluster                             */
/*         /maxfiles                Maximum files per backup piece                */
/*         /maxsize                 Maximum backup piece size                     */
/*         /meta                    Retention for META backups                    */
/*         /pwd                      user's password                              */
/*         /pgdata                                                                */
/*         /port                     Port used to connect to the cluster          */
/*         /usr                      User used to connect to the cluster          */
/*         /waldir                   Display WAL destination folder               */
/*      Available Qualifier(s):                                                   */
/*         /parameter=<STRING>       List settings of the cluster                 */
/**********************************************************************************/                                      
void COMMAND_SHOWCLUSTER(int idcmd,char *command_line)
{
   if (CLUisConnected() == false)
   {
      ERROR(ERR_NOTCONNECTED,"Not connected to any cluster.\n");
      return;
   };
   if (DEPOisConnected() == false)
      WARN(WRN_NOTDEPOSITCNX,"Not connected to any deposit.\n");

   if (varGetLong(GVAR_CLUCID) == 0)
      WARN(WRN_NOTREGISTERED,"Not registered to any deposit.\n");

   int id=0;
   if (optionIsSET("opt_blksize")     == true) { printf("Memory buffer size (MB): '%s'\n",          varGet(GVAR_CLUREADBLOCKSIZE)); id++; };
   if (optionIsSET("opt_date_format") == true) { printf("Date format: '%s'\n",                      varGet(GVAR_CLUDATE_FORMAT)); id++; };
   if (optionIsSET("opt_delwal")      == true) { printf("Delete WAL after backup(minutes): '%s'\n", varGet(GVAR_CLUAUTODELWAL)); id++; };
   if (optionIsSET("opt_maxsize")     == true) { printf("Backup piece MAX size: '%s'\n",            varGet(GVAR_CLUBACKUP_MAXSIZE)); id++; };
   if (optionIsSET("opt_maxfiles")    == true) { printf("Backup piece MAX files: '%s'\n",           varGet(GVAR_CLUBACKUP_MAXFILES)); id++; };
   if (optionIsSET("opt_full")        == true) { printf("Full+WAL backup retention: '%s'\n",        varGet(GVAR_CLURETENTION_FULL)); id++; };
   if (optionIsSET("opt_cfg")         == true) { printf("Config backup retention: '%s'\n",          varGet(GVAR_CLURETENTION_CONFIG)); id++; };
   if (optionIsSET("opt_meta")        == true) { printf("Metadata backup retention: '%s'\n",        varGet(GVAR_CLURETENTION_META)); id++; };
   if (optionIsSET("opt_name")        == true) { printf("Cluster Name: '%s'\n",                     varGet(GVAR_CLUNAME)); id++; }; 
   if (optionIsSET("opt_description") == true) { printf("Cluster description: '%s'\n",              varGet(GVAR_CLUDESCR)); id++; }; 
   if (optionIsSET("opt_pgdata")      == true) { printf("PGDATA directory: '%s'\n",                 varGet(GVAR_CLUPGDATA)); id++; }; 
   if (optionIsSET("opt_usr")         == true) { printf("User: '%s'\n",                             varGet(GVAR_CLUUSER)); id++; }; 
   if (optionIsSET("opt_pwd")         == true) { printf("Password: '%s'\n",                         varGet(GVAR_CLUPWD)); id++; }; 
   if (optionIsSET("opt_db")          == true) { printf("Database: '%s'\n",                         varGet(GVAR_CLUDB)); id++; }; 
   if (optionIsSET("opt_port")        == true) { printf("Listen Port: '%s'\n",                      varGet(GVAR_CLUPORT)); id++; }; 
   if (optionIsSET("opt_waldir")      == true) { printf("WAL directory: '%s'\n",                    varGet(GVAR_WALDIR)); id++; }; 
   if (optionIsSET("opt_backupdir")   == true) { printf("Backup directory: '%s'\n",                 varGet(GVAR_BACKUPDIR)); id++; }; 
   if (optionIsSET("opt_compression") == true) { printf("Compression Level(-1..9): '%s'\n",         varGet(GVAR_COMPLEVEL)); id++; }; 
   if (optionIsSET("opt_concurrently")== true) { char r[4];
                                                 if (varGetInt(GVAR_REINDEXCONCURRENTLY) == true) { strcpy(r,"yes"); }
                                                                                             else { strcpy(r,"no"); };
                                                 printf("Rebuild Index Concurrently: '%s'\n",r);
                                                 id++; 
                                               };
   if (optionIsSET("opt_ip") == true) { printf("IP address: '%s'\n",                                varGet(GVAR_CLUIP)); id++; }; 
   if (qualifierIsUNSET("qal_parameter") == false)
   {
      char *query=malloc(1024);
      sprintf(query,"select name,setting from pg_settings where name like lower('%%%s%%')",varGet("qal_parameter"));
      TRACE("query=%s\n",query);
      int rows=CLUquery(query,0);
      if (rows > 0)
      {
         int row=0;
         while (row < rows)
         {
            printf("%s='%s'\n",CLUgetString(row,0),CLUgetString(row,1));
            row++;
         };
      }
      CLUqueryEnd();
      free(query);
      id++;
   }
   if (id ==0)
   {
      char *query=malloc(1024);
      
      printf("recm version %s.\n",varGet(RECM_VERSION));
      printf("Cluster parameters:\n");
        printOption("Cluster ID(CID)","",             varGet(GVAR_CLUCID));
        printOption("Cluster Name",   "",             varGet(GVAR_CLUNAME));
        printOption("Description",    "/description", varGet(GVAR_CLUDESCR));
        printOption("Version",        "",             varGet(GVAR_CLUVERSION));
        printOption("PGDATA",         "/pgdata",      varGet(GVAR_CLUPGDATA));
        printOption("Date format",    "/date_format", varGet(GVAR_CLUDATE_FORMAT));
      
        int rows=CLUquery("select pg_current_wal_lsn(),pg_walfile_name(pg_current_wal_lsn()),timeline_id FROM pg_control_checkpoint();",0);
        printOption("Current WAL position","",CLUgetString(0,0));
        printOption("Current WAL file",    "",CLUgetString(0,1));
        printOption("Current Timeline(TL)","",CLUgetString(0,2));
        CLUqueryEnd();
      
      printSection("Connection");
        printOption("User",             "/usr",         varGet(GVAR_CLUUSER));
        printOption("Password",         "/pwd",     "********");
        printOption("Database",         "/db",      varGet(GVAR_CLUDB));
        printOption("Listen Port",      "/port",         varGet(GVAR_CLUPORT));
      printSection("Backups/Restore");
              printOption("Memory buffer size (MB)",               "/blksize",          varGet(GVAR_CLUREADBLOCKSIZE   ));
              printOption("Delete WAL after backup(minutes)",      "/delwal",           varGet(GVAR_CLUAUTODELWAL      ));
              printOption("Backup piece MAX size",                 "/maxsize",          varGet(GVAR_CLUBACKUP_MAXSIZE  ));
              printOption("Backup piece MAX files",                "/maxfiles",         varGet(GVAR_CLUBACKUP_MAXFILES ));
              printOption("Full+WAL backup retention",             "/full",             varGet(GVAR_CLURETENTION_FULL  ));
              printOption("Config backup retention",               "/cfg",              varGet(GVAR_CLURETENTION_CONFIG));
              printOption("Metadata backup retention",             "/meta",             varGet(GVAR_CLURETENTION_META  ));
              char r[4];
              if (varGetInt(GVAR_REINDEXCONCURRENTLY) == true) { strcpy(r,"yes"); }
                                                          else { strcpy(r,"no"); };
              printOption("Rebuild Index Concurrently","/concurrently",r); 
        printOption("WAL directory",    "/waldir",       varGet(GVAR_WALDIR));
        printOption("Backup directory", "/backupdir",    varGet(GVAR_BACKUPDIR));
        printOption("Compression Level(-1..9)","/compression",  varGet(GVAR_COMPLEVEL));
      printSection("Replication - Standby information (to connect to this cluster)");
        sprintf(query,"select rep_usr,rep_pwd,clu_addr,rep_opts from %s.clusters where CID=%s",
                       varGet(GVAR_DEPUSER),
                       varGet(GVAR_CLUCID));
        rows=DEPOquery(query,0);
        printOption("replication user",        "/repusr",       DEPOgetString(0,0));
        printOption("replication password",    "/reppwd",       "********");
        printOption("IP address",              "/ip",           DEPOgetString(0,2));
        printOption("replication options",     "",              DEPOgetString(0,3));
        DEPOqueryEnd();
      printSection("Deposit");
        printOption("Registered deposit",   "",          varGet(GVAR_REGDEPO));
      
      if (DEPOisConnected() == true)
      {
         sprintf(query,"select clu_sts from %s.clusters where CID=%s",
                       varGet(GVAR_DEPUSER),
                       varGet(GVAR_CLUCID));
         rows=DEPOquery(query,0);
         sprintf(query,"select clu_sts,"
                       "       coalesce(to_char(lstfull, 'DD/MM/YYYY HH24:MI'),to_char(lstfull, 'DD/MM/YYYY HH24:MI'),'(never)'),"
                       "       coalesce(to_char(lstwal, 'DD/MM/YYYY HH24:MI'),to_char(lstwal, 'DD/MM/YYYY HH24:MI'),'(never)'),"
                       "       coalesce(to_char(lstcfg, 'DD/MM/YYYY HH24:MI'),to_char(lstcfg, 'DD/MM/YYYY HH24:MI'),'(never)')"
                       "  from %s.clusters where CID=%s",
                       varGet(GVAR_DEPUSER),
                       varGet(GVAR_CLUCID));
         rows=DEPOquery(query,0);
         printOption("Status",            "",DEPOgetString(0,0));
         printOption("Last FULL backup",  "",DEPOgetString(0,1));
         printOption("Last WAL backup",   "",DEPOgetString(0,2));
         printOption("Last CONFIG backup","",DEPOgetString(0,3));
         printf("\nTo modify configuration, use qualifier enclosed in parentheses\n");
         printf("Example:\n");
         printf("\tRECM> modify cluster /waldir=\"<my_folder>\"\n");
      
         DEPOqueryEnd();
      }
      free(query);
   }
}

