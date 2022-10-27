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
/*
@command show cluster
@definition
Display RECM cluster's properties.
(See also {&modify cluster&} for more details)
@option "/backupdir"     "Backup directory"
@option "/blksize"       "Block size of working memory buffer"
@option "/cfg"           "Retention for CONFIG backups"
@option "/compression"   "'-1' to disable. '1'..'9' set the compression level."
@option "/db"            "Database name to connect to (Default is 'postgres')"
@option "/date_format"   "Date format (Default is 'YYYY-MM-DD HH24:MI:SS')"
@option "/delwal"        "Delay of WAL deletion by WAL backup"
@option "/description"   "Description of the cluster"
@option "/full"          "Retention for FULL and WAL backups"
@option "/ip"            "IP of the cluster"
@option "/maxfiles"      "Maximum files within a FULL backup set"
@option "/maxwalfiles"   "Maximum files within a WAL backup set"
@option "/maxsize"       "Maximum size of a backup set"
@option "/maxwalsize"    "Maximum size of a WAL backup set"
@option "/meta"          "Retention for METADATA backups"
@option "/name"          "Cluster name (Correspond to PostgreSQL parameter 'cluster_name'"
@option "/pwd"           "User's password used to connect to the cluster"
@option "/pgdata"        ""
@option "/port"          "Port used to connect to the cluster"
@option "/repusr"        ""
@option "/reppwd"        ""
@option "/repopts"       ""
@option "/usr"           "User used to connect to the cluster"
@option "/waldir"        "WAL destination folder"
@option "/parameter=S"   "Display PostgreSQL cluster's parameter"
 
@example
@inrecm show cluster /pgdata
&out PGDATA directory: '/Library/PostgreSQL/12/data'
@inrecm show cluster /backupdir /waldir
@out WAL directory: '/Library/PostgreSQL/12/WALfiles/'
@out Backup directory: '/Volumes/pg_backups/'
@inrecm show cluster /param=ssl 
@out ssl='off'
@out ssl_ca_file=''
@out ssl_cert_file='server.crt'
@out ssl_ciphers='HIGH:MEDIUM:+3DES:!aNULL'
@out ssl_crl_file=''
@out ssl_dh_params_file=''
@out ssl_ecdh_curve='prime256v1'
@out ssl_key_file='server.key'
@out ssl_library='OpenSSL'
@out ssl_max_protocol_version=''
@out ssl_min_protocol_version='TLSv1'
@out ssl_passphrase_command=''
@out ssl_passphrase_command_supports_reload='off'
@out ssl_prefer_server_ciphers='on'
@inrecm  
@end
 
*/
void COMMAND_SHOWCLUSTER(int idcmd,char *command_line)
{
   if (CLUisConnected(true) == false) return;
   if (DEPOisConnected(false) == false) WARN(WRN_NOTDEPOSITCNX,"Not connected to any deposit.\n");
   if (varGetLong(GVAR_CLUCID) == 0)    WARN(WRN_NOTREGISTERED,"Not registered to any deposit.\n");

   int id=0;
   if (optionIsSET("opt_blksize")     == true) { printf("Memory buffer size (MB): '%s'\n",          varGet(GVAR_CLUREADBLOCKSIZE)); id++; };
   if (optionIsSET("opt_date_format") == true) { printf("Date format: '%s'\n",                      varGet(GVAR_CLUDATE_FORMAT)); id++; };
   if (optionIsSET("opt_delwal")      == true) { printf("Delete WAL after backup(minutes): '%s'\n", varGet(GVAR_CLUAUTODELWAL)); id++; };
   if (optionIsSET("opt_maxsize")     == true) { printf("Backup piece MAX size: '%s'\n",            varGet(GVAR_CLUBACKUP_MAXSIZE)); id++; };
   if (optionIsSET("opt_maxfiles")    == true) { printf("Backup piece MAX files: '%s'\n",           varGet(GVAR_CLUBACKUP_MAXFILES)); id++; };
   if (optionIsSET("opt_maxwalsize")  == true) { printf("WAL Backup piece MAX size: '%s'\n",        varGet(GVAR_CLUBACKUP_MAXWALSIZE)); id++; };
   if (optionIsSET("opt_maxwalfiles") == true) { printf("WAL Backup piece MAX files: '%s'\n",       varGet(GVAR_CLUBACKUP_MAXWALFILES)); id++; };
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
   if (optionIsSET("opt_repusr") == true) { char *query=malloc(1024);
                                            sprintf(query,"select rep_usr from %s.clusters where CID=%s",
                                                          varGet(GVAR_DEPUSER),
                                                          varGet(GVAR_CLUCID));
                                            int rows=DEPOquery(query,0);
                                            printf("replication user: '%s'\n",DEPOgetString(0,0));
                                            free(query);
                                            id++;
                                          };
   if (optionIsSET("opt_reppwd") == true) { char *query=malloc(1024);
                                            sprintf(query,"select rep_pwd,clu_addr,rep_opts from %s.clusters where CID=%s",
                                                          varGet(GVAR_DEPUSER),
                                                          varGet(GVAR_CLUCID));
                                            int rows=DEPOquery(query,0);
                                            printf("replication password: '%s'\n",unscramble(DEPOgetString(0,0)));
                                            free(query);
                                            DEPOqueryEnd();
                                            id++;
                                          };
   if (optionIsSET("opt_repopts") == true) { char *query=malloc(1024);
                                            sprintf(query,"select rep_opts from %s.clusters where CID=%s",
                                                          varGet(GVAR_DEPUSER),
                                                          varGet(GVAR_CLUCID));
                                            int rows=DEPOquery(query,0);
                                            printf("replication options: '%s'\n",DEPOgetString(0,0));
                                            free(query);
                                            DEPOqueryEnd();
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
      
//      printf("recm version %s.\n",varGet(RECM_VERSION));
      display_version();
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
              printOption("WAL Backup piece MAX size",             "/maxwalsize",       varGet(GVAR_CLUBACKUP_MAXWALSIZE  ));
              printOption("WAL Backup piece MAX files",            "/maxwalfiles",      varGet(GVAR_CLUBACKUP_MAXWALFILES ));
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
      
      if (DEPOisConnected(false) == true)
      {
         sprintf(query,"select clu_sts from %s.clusters where CID=%s",
                       varGet(GVAR_DEPUSER),
                       varGet(GVAR_CLUCID));
         rows=DEPOquery(query,0);
         sprintf(query,"select clu_sts,"
                       "       coalesce(to_char(lstfull,'%s'),'(never)'),"
                       "       coalesce(to_char(lstwal, '%s'),'(never)'),"
                       "       coalesce(to_char(lstcfg, '%s'),'(never)'),"
                       "       coalesce(to_char(lstmeta,'%s'),'(never)')"
                       "  from %s.clusters where CID=%s",
                       varGet(GVAR_CLUDATE_FORMAT),
                       varGet(GVAR_CLUDATE_FORMAT),
                       varGet(GVAR_CLUDATE_FORMAT),
                       varGet(GVAR_CLUDATE_FORMAT),
                       varGet(GVAR_DEPUSER),
                       varGet(GVAR_CLUCID));
         rows=DEPOquery(query,0);
         printOption("Status",            "",DEPOgetString(0,0));
         printOption("Last FULL backup",  "",DEPOgetString(0,1));
         printOption("Last WAL backup",   "",DEPOgetString(0,2));
         printOption("Last CONFIG backup","",DEPOgetString(0,3));
         printOption("Last META backup","",  DEPOgetString(0,4));
         printf("\nTo modify configuration, use qualifier enclosed in parentheses\n");
         printf("Example:\n");
         printf("\tRECM> modify cluster /waldir=\"<my_folder>\"\n");
      
         DEPOqueryEnd();
      }
      free(query);
   }
}

