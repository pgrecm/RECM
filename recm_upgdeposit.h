/* CHANGE: 0xA0008 upgrade deposit command */

void COMMAND_UPGRADEDEPOSIT(int idcmd,char *command_line)
{
   if (isDepositConnected() == false) return;
   char *query=malloc(1024);
   
   sprintf(query,"select deponame,version from %s.config limit 1",
                 varGet("qal_user"));
   int rows=DEPOquery(query,0);
   if (rows <= 0) 
   {
      ERROR(ERR_WRONGDEPOSIT,"Could not retreive deposit version.");
      free(query);
      return;
   };
   char *curDepoNam=strdup(DEPOgetString(0,0));
   char *curVersion=strdup(DEPOgetString(0,1));
   DEPOqueryEnd();
   if (strcmp(curVersion,RECM_VERSION) == 0)
   {
      WARN(WRN_BADVERSION,"Deposit already in version'%'. Nothing to do.\n",RECM_VERSION);
      free(curVersion);
      free(curDepoNam);
      free(query);
      DEPOqueryEnd();
      return;
   }
   
   // Upgrade from 1.1.0 to 1.2.0
   // changes : 
   // configuration parameter attached to deposit are saved into database
   // instead of configuration file.
   //   /backupmask : backupmask ==> clusters.opt_bckmsk varchar(10)
   //   /restoremask: restoremask==> clusters.opt_rstmsk varchar(10)
   //   /blksize    : blksize    ==> clusters.opt_blksiz long
   //   /delwal     : delwal     ==> clusters.opt_delwal int 
   //   /delaydelwal: delaydelwal==> clusters.opt_delaydel long
   //   /maxsize    : maxsize    ==> clusters.opt_maxsiz  varchar(20)
   //   /maxfiles   : maxfiles   ==> clusters.opt_maxfil  long
   //   /addfiles   : addfiles   ==> clusters.opt_addfil  text
   //   /exclude    : exclude    ==> clusters.opt_exclude text
   //   /full       : full       ==> clusters.opt_retfull varchar(20)
   //   /cfg        : cfg        ==> clusters.opt_retcfg  varchar(20)
   //   /meta       : meta       ==> clusters.opt_retmeta varchar(20)
   //   /waldir     : waldir     ==> clusters.opt_waldir  text
   //   /backupdir  : backupdir  ==> clusters.opt_bckdir  text
   //   /compression: compression==> clusters.opt_compr   int // 0-9

   if (strcmp(curDepoNam,"1.1.0") == 0 && strcmp(RECM_VERSION,"1.2.0") == 0)
   {
      // 1 : 
      sprintf(query,"alter table %s.backups add options bigint default 0",
                    varGet("qal_user"));
      int rc=DEPOquery(query,0);
      if (rc < 0) 
      { 
         ERROR(ERR_UPGRADEFAIL,"Upgrade to '%s' failed (0x01).\n",RECM_VERSION);
         free(curVersion);
         free(curDepoNam);
         free(query);
         DEPOqueryEnd();
         return;
      }
      DEPOqueryEnd();   
      
      sprintf(query,"update %s.backups set options=options | 1 where opt_ndx='Y'",
                    varGet("qal_user"));
      int rc=DEPOquery(query,0);
      if (rc < 0) 
      { 
         ERROR(ERR_UPGRADEFAIL,"Upgrade to '%s' failed (0x02).\n",RECM_VERSION);
         free(curVersion);
         free(curDepoNam);
         free(query);
         DEPOqueryEnd();
         return;
      }
      DEPOqueryEnd();   
      
   }
   if (PQresultStatus(sys_res) != PGRES_COMMAND_OK)
      
   int option_force=optionIsSET("opt_force");
   int option_verbose=optionIsSET("opt_verbose");
   int saved_verbosity=globalArgs.verbosity;

   globalArgs.verbosity=option_verbose;

   if (clu_cnx == NULL)                                                         // Must be connected on a cluster
   {
      ERROR(ERR_NOTCONNECTED,"Not connected. Connect first to a cluster.\n");
      globalArgs.verbosity=saved_verbosity;
      return;
   };

   
   // Find what already exist
   sprintf(query,"select a.db_ok,b.usr_ok from (select count(*) as db_ok from pg_database where datname='%s') a,(select count(*) as usr_ok"
                 " from pg_roles where rolname='%s') b",
                 varGet("qal_database"),
                 varGet("qal_user"));
   PGresult *sys_res = PQexec(clu_cnx, query);
   if (PQresultStatus(sys_res) != PGRES_TUPLES_OK) 
   { 
      ERROR(ERR_DBQRYERROR,"Precheck create error.\n"); 
      free(query);
      globalArgs.verbosity=saved_verbosity;
      free(query);
      return;
    };
   int is_rdb_ok=-1;
   int is_usr_ok=-1;
   int rows = PQntuples(sys_res);
   for(int i=0; i< rows; i++)
   {
      is_rdb_ok=atoi( PQgetvalue(sys_res, i,0));
      is_usr_ok=atoi( PQgetvalue(sys_res, i,1));
   };
   PQclear(sys_res);
   
   if (is_rdb_ok == 1 && option_force == false)
   {
      ERROR(ERR_DEPOEXIST,"Deposit '%s' already exist. Use '/force' to recreate it.\n",varGet("qal_database"));
      globalArgs.verbosity=saved_verbosity;
      free(query);
      return;
   }
   //# Drop database ################################################
   if (is_rdb_ok == 1 && option_force == true)
   {
      sprintf(query,"drop database %s",varGet("qal_database"));
      sys_res = PQexec(clu_cnx, query);
      if (PQresultStatus(sys_res) != PGRES_COMMAND_OK) 
      { 
        ERROR(ERR_DROPDB,"Could not drop database '%s'\n",varGet("qal_database")); 
        globalArgs.verbosity=saved_verbosity;
        free(query);
        return;
      };
      PQclear(sys_res);
   }
   sprintf(query,"create database %s",varGet("qal_database"));
   sys_res = PQexec(clu_cnx, query);
   if (PQresultStatus(sys_res) != PGRES_COMMAND_OK) 
   { 
      ERROR(ERR_CREATEDB,"Could not create database '%s'\nError:%s\n",varGet("qal_database"),PQerrorMessage(clu_cnx)); 
      globalArgs.verbosity=saved_verbosity;
      free(query);
      return;
   };
   PQclear(sys_res);
   //# Create user ##################################################
   if (is_usr_ok == 0)
   {
      sprintf(query,"create role %s with password '%s' login",varGet("qal_user"),varGet("qal_password"));
      sys_res= PQexec(clu_cnx,query);
      if (PQresultStatus(sys_res) != PGRES_COMMAND_OK)
      {
         PQclear(sys_res);
         free(query);
         ERROR(ERR_CREATEROLE,"Could not create role\nError:%s\n",PQerrorMessage(clu_cnx));
         globalArgs.verbosity=saved_verbosity;
         free(query);
         return;
      };
      PQclear(sys_res);
   };
   //# grants #######################################################
   sprintf(query,"grant create on database %s to %s",varGet("qal_database"),varGet("qal_user"));
   sys_res= PQexec(clu_cnx,query); 
   if (PQresultStatus(sys_res) != PGRES_COMMAND_OK)
   {
      PQclear(sys_res);
      free(query);
      ERROR(ERR_GRANTDB,"Could not grant create\nError:%s\n",PQerrorMessage(clu_cnx));
      globalArgs.verbosity=saved_verbosity;
      free(query);
      return;
   }; 
   sprintf(query,"alter role %s set search_path=\"$user\", %s",varGet("qal_user"),varGet("qal_user"));
   sys_res= PQexec(clu_cnx,query);
   if (PQresultStatus(sys_res) != PGRES_COMMAND_OK)
   {
      PQclear(sys_res);
      free(query);
      ERROR(ERR_GRANTROLE,"Could not grant create\nError:%s\n",PQerrorMessage(clu_cnx));
      globalArgs.verbosity=saved_verbosity;
      free(query);
      return;
   }; 

   //# Now, create objects #############################################
   char *connectString=malloc(1024);
   sprintf(connectString,"dbname=%s user=%s password=%s host=%s port=%s",
                         varGet("qal_database"),
                         varGet("qal_user"),
                         varGet("qal_password"),
                         varGet(GVAR_CLUHOST),
                         varGet(GVAR_CLUPORT));
   PGconn     *sys_cnx=NULL;

   sys_cnx = PQconnectdb(connectString);
   if (PQstatus(sys_cnx) != CONNECTION_OK) 
   { 
      ERROR(ERR_NOTCONNECTED,"Connection to host '%s' failed\n",varGet(GVAR_CLUHOST)); 
      globalArgs.verbosity=saved_verbosity;
      free(query);
      return;
   };

   // Now, we need to take care of the rights of the user to the database to create objects
   sprintf(query,"create schema %s authorization %s",varGet("qal_user"),varGet("qal_user"));
   sys_res= PQexec(sys_cnx,query); 
   if (PQresultStatus(sys_res) != PGRES_COMMAND_OK)
   {
      PQclear(sys_res);
      free(query);
      ERROR(ERR_CREATESCHEMA,"Could not create schema\nError:%s\n",PQerrorMessage(sys_cnx));
      globalArgs.verbosity=saved_verbosity;
      free(query);
      return;
   }; 
   PQclear(sys_res);
   sprintf(query,"grant create on schema %s to %s",varGet("qal_user"),varGet("qal_user"));
   sys_res= PQexec(sys_cnx,query);
   if (PQresultStatus(sys_res) != PGRES_COMMAND_OK)
   {
      PQclear(sys_res);
      free(query);
      ERROR(ERR_GRANTSCHEMA,"Could not grant usage\nError:%s\n",PQerrorMessage(sys_cnx));
      globalArgs.verbosity=saved_verbosity;
      free(query);
      return;
   }; 
   PQclear(sys_res);
   sprintf(query,"grant usage on schema %s to %s",varGet("qal_user"),varGet("qal_user"));
   sys_res= PQexec(sys_cnx,query);
   if (PQresultStatus(sys_res) != PGRES_COMMAND_OK)
   {
      PQclear(sys_res);
      free(query);
      ERROR(ERR_GRANTUSAGE,"Could not grant usage\nError:%s\n",PQerrorMessage(sys_cnx));
      globalArgs.verbosity=saved_verbosity;
      free(query);
      return;
   }; 
   PQclear(sys_res);
   sprintf(query,"create table %s.config ("
                                          "deponame varchar(32),"
                                          "version varchar(10),"
                                          "description text,"
                                          "created timestamp default current_timestamp,"
                                          "updated timestamp default current_timestamp)",
                                          varGet("qal_user"));
   sys_res= PQexec(sys_cnx,query); 
   if (PQresultStatus(sys_res) != PGRES_COMMAND_OK)
   {
      PQclear(sys_res);
      free(query);
      ERROR(ERR_CREATTABLE,"Could not create table 'config'\nError:%s\n",PQerrorMessage(sys_cnx));
      globalArgs.verbosity=saved_verbosity;
      free(query);
      return;
   };
   PQclear(sys_res);
   sprintf(query,"insert into %s.config (deponame,version,description) values ('%s','%s','Recovery Manager Deposit(PostgreSQL). Release installation')",
                  varGet("qal_user"),
                  varGet(GVAR_CLUNAME),
                  varGet(RECM_VERSION)
                  );
   sys_res= PQexec(sys_cnx,query); 
   if (PQresultStatus(sys_res) != PGRES_COMMAND_OK)
   {
      PQclear(sys_res);
      free(query);
      ERROR(ERR_CREATTABLE,"Could not create table 'config'\nError:%s\n",PQerrorMessage(sys_cnx));
      globalArgs.verbosity=saved_verbosity;
      free(query);
      return;
   };
   PQclear(sys_res);
   sprintf(query,"create table %s.clusters(CID serial,"
                                          "rep_nam varchar(128),"                         //    -- not used
                                          "clu_sts  varchar(15), "                        //    -- Cluster status 'CURRENT'
                                          "clu_nam  varchar(128),"                        //    -- name of the cluster
                                          "clu_cre  timestamp default current_timestamp," //    -- Creation date
                                          "clu_info text,"                                //    -- Description
                                          "lstfull timestamp,"                            //    -- last FULL backup date
                                          "lstwal  timestamp,"                            //    -- last WAL backup
                                          "lstcfg  timestamp)",varGet("qal_user"));               //    -- last CFG backup
   printf(".");fflush(stdout);
   sys_res= PQexec(sys_cnx,query); 
   if (PQresultStatus(sys_res) != PGRES_COMMAND_OK)
   {
      PQclear(sys_res);
      free(query);
      ERROR(ERR_CREATTABLE,"Could not create table 'clusters'\nError:%s\n",PQerrorMessage(sys_cnx));
      globalArgs.verbosity=saved_verbosity;
      free(query);
      return;
   };
   PQclear(sys_res);
   sprintf(query,"create table %s.cluster_members(CID integer,"
                                                 "CMID serial,"
                                                 "hostname  varchar(128),"
                                                 "port      varchar(10),"
                                                 "lsnpath   varchar(1024),"
                                                 "version   text)",varGet("qal_user"));
   printf(".");fflush(stdout);
   sys_res= PQexec(sys_cnx,query); 
   if (PQresultStatus(sys_res) != PGRES_COMMAND_OK)
   {
      PQclear(sys_res);
      free(query);
      ERROR(ERR_CREATTABLE,"Could not create table 'cluster_members'\nError:%s\n",PQerrorMessage(sys_cnx));
      globalArgs.verbosity=saved_verbosity;
      free(query);
      return;
   };
   PQclear(sys_res);
   sprintf(query,"create table %s.backups(CID       bigint not null,"                        //    -- Cluster ID
                                         "bcktyp    varchar(10) not null,"                   //    -- Type of backup : WAL|FULL|CFG
                                         "bck_id    varchar(30) not null,"                   //    -- Unique ID of the backup
                                         "bcksts    integer default 0,"                      //    -- Backup status '0=Available 1=obsolete 2=incomplete
                                         "bcktag    varchar(1024),"                          //    -- User TAG
                                         "hstname   varchar(128),"                           //    -- instance Name
                                         "pgdata    varchar(1024),"                          //    -- instance PGDATA
                                         "pgversion varchar(20),"                            //    -- Version of the cluster at backup time (PG_VERSION)
                                         "timeline  bigint,"                                 //    -- Keep in mind the timeline of the backup
                                         "opt_ndx   varchar(3),"                             //    -- option 'noindex'
                                         "bcksize   bigint,"                                 //    -- Total Size of the backup (in MB)
                                         "pcount    bigint,"                                 //    -- Number of files created
                                         "bckdir    varchar(1024),"                          //    -- Location of all backup files
                                         "bdate     timestamp default current_timestamp,"    //    -- Backup begin date
                                         "bwall     varchar(20),"                            //    -- write-ahead log location (result of pg_start_backup)
                                         "bwalf     varchar(128),"                           //    -- wal file
                                         "edate     timestamp default null,"                 //    -- Backup end date
                                         "ewall     varchar(20),"                            //    -- write-ahead log location (result of pg_stop_backup)
                                         "ewalf     varchar(128),"                           //    -- wal file at end of backup 
                                         "ztime     varchar(128),"                           //    -- timezone of the database
                                         "sdate     varchar(128))",varGet("qal_user"));      //    -- date style of the database at backup time
   printf(".");fflush(stdout);
   sys_res= PQexec(sys_cnx,query);
   if (PQresultStatus(sys_res) != PGRES_COMMAND_OK)
   {
      PQclear(sys_res);
      free(query);
      ERROR(ERR_CREATTABLE,"Could not create table 'backups'\nError:%s\n",PQerrorMessage(sys_cnx));
      globalArgs.verbosity=saved_verbosity;
      return;
   };
   PQclear(sys_res);
   sprintf(query,"create table %s.backup_pieces (CID      bigint not null,"
                                                "bck_id   varchar(30) not null,"             //        -- Backup ID 
                                                "pcid     bigint,"                           //        -- cluster owner
                                                "pname    varchar(1024), "                   //        -- Part file name
                                                "psize    bigint,"                           //        -- Total Size of the backup part(in MB)
                                                "csize    bigint,"                           //        -- Total Size of the backup part(in MB) compressed
                                                "pcount   bigint,"                           //        -- Number of files in the piece
                                                "bwal     timestamp,"                        //        -- For WAL backup, date of the first WAL
                                                "ewal     timestamp)",varGet("qal_user"));           //        -- or WAL backup, date of the last WAL  
   printf(".");fflush(stdout);
   sys_res= PQexec(sys_cnx,query);
   if (PQresultStatus(sys_res) != PGRES_COMMAND_OK)
   {
      PQclear(sys_res);
      free(query);
      ERROR(ERR_CREATTABLE,"Could not create table 'backup_pieces'\nError:%s\n",PQerrorMessage(sys_cnx));
      globalArgs.verbosity=saved_verbosity;
      return;
   };
   PQclear(sys_res);
   sprintf(query,"create table %s.backup_wals (CID bigint,"
                                              "bck_id varchar(30),"
                                              "walfile varchar(128),"
                                              "ondisk integer,"
                                              "bckcount integer,"
                                              "edate timestamp)",varGet("qal_user"));
   printf(".");fflush(stdout);
   sys_res= PQexec(sys_cnx,query);
   if (PQresultStatus(sys_res) != PGRES_COMMAND_OK)
   {
      PQclear(sys_res);
      ERROR(ERR_CREATTABLE,"Could not create table 'backup_wals'\nError:%s\n",PQerrorMessage(sys_cnx));
      globalArgs.verbosity=saved_verbosity;
      free(query);
      return;
   };
   PQclear(sys_res);
      
   sprintf(query,"create table %s.backup_dbs (CID      bigint,"
                                             "bck_id   varchar(30),        "          //      -- backup id
                                             "db_nam   varchar(30),        "          //      -- database name
                                             "db_oid   oid,                "          //      -- OID of the database (is the relfilenode of the db)
                                             "db_typ   int)",varGet("qal_user"));     //      -- 0=system db // 1=template // 2=user db
   printf(".");fflush(stdout);
   sys_res= PQexec(sys_cnx,query);
   if (PQresultStatus(sys_res) != PGRES_COMMAND_OK)
   {
      PQclear(sys_res);
      ERROR(ERR_CREATTABLE,"Could not create table 'backup_dbs'\nError:%s\n",PQerrorMessage(sys_cnx));
      globalArgs.verbosity=saved_verbosity;
      free(query);
      return;
   };
   PQclear(sys_res);

   sprintf(query,"create table %s.backup_tbl (CID        bigint,"
                                             "bck_id     varchar(30),"                 //      -- backup id
                                             "tab_dbid   oid,        "                 //      -- database name
                                             "tab_sch    varchar(30),"
                                             "tab_nam    varchar(30),"                 //      -- OID of the database (is the relfilenode of the db)
                                             "tab_fid    bigint,"                      //      -- 0=system db // 1=template // 2=user db
                                             "tbs_oid    bigint)",                     //      -- OID of the tablespace",
                  varGet("qal_user")); 
   printf(".");fflush(stdout);
   sys_res= PQexec(sys_cnx,query);
   if (PQresultStatus(sys_res) != PGRES_COMMAND_OK)
   {
      PQclear(sys_res);
      free(query);
      ERROR(ERR_CREATTABLE,"Could not create table 'backup_tbl'\nError:%s\n",PQerrorMessage(sys_cnx));
      globalArgs.verbosity=saved_verbosity;
      free(query);
      return;
   };
   PQclear(sys_res);
      
   sprintf(query,"create table %s.backup_ndx (CID bigint,"
                                             "bck_id   varchar(30) not null,"               //      -- Unique ID of the backup
                                             "bd_id    bigserial,"                          //
                                             "bd_ndx   varchar(128),"                      //      -- Part file name
                                             "bd_dbn   varchar(128),"                       //      -- Database name
                                             "bd_sch   varchar(128),"                       //      -- Schema
                                             "bd_tbl   varchar(128))",varGet("qal_user"));            //      -- Tablename
   printf(".");fflush(stdout);
   sys_res= PQexec(sys_cnx,query);
   if (PQresultStatus(sys_res) != PGRES_COMMAND_OK)
   {
      PQclear(sys_res);
      free(query);
      ERROR(ERR_CREATTABLE,"Could not create table 'backup_ndx'\nError:%s\n",PQerrorMessage(sys_cnx));
      globalArgs.verbosity=saved_verbosity;
      free(query);
      return;
   };
   PQclear(sys_res);

   sprintf(query,"create table %s.backup_tbs (CID       bigint,"
                                             "bck_id    varchar(30) not null,"               //      -- Unique ID of the backup
                                             "tbs_oid   oid,"                          //
                                             "tbs_nam   varchar(128),"                      //      -- Part file name
                                             "tbs_loc   text)",varGet("qal_user"));            //      -- Tablename
   printf(".");fflush(stdout);
   sys_res= PQexec(sys_cnx,query);
   if (PQresultStatus(sys_res) != PGRES_COMMAND_OK)
   {
      PQclear(sys_res);
      free(query);
      ERROR(ERR_CREATTABLE,"Could not create table 'backup_tbs'\nError:%s\n",PQerrorMessage(sys_cnx));
      globalArgs.verbosity=saved_verbosity;
      free(query);
      return;
   };
   PQclear(sys_res);

   sprintf(query,"create table %s.backup_rp (cid     bigint,"
                                            "rp_name varchar(30) not null,"               //      -- Unique ID of the backup
                                            "rp_typ  varchar(5),"                         // "T" for temporary / "P" for permanent"
                                            "lsn     varchar(30),"
                                            "tl      bigint,"
                                            "wal     varchar(30),"
                                            "crdate  timestamp default now())",
                                            varGet("qal_user"));
   printf(".");fflush(stdout);
   sys_res= PQexec(sys_cnx,query);
   if (PQresultStatus(sys_res) != PGRES_COMMAND_OK)
   {
      PQclear(sys_res);
      free(query);
      ERROR(ERR_CREATTABLE,"Could not create table 'backup_rp'\nError:%s\n",PQerrorMessage(sys_cnx));
      globalArgs.verbosity=saved_verbosity;
      free(query);
      return;
   };
   PQclear(sys_res);

      
   sprintf(query,"create index clusters_ndx0 on %s.clusters (clu_nam)",varGet("qal_user"));
   printf(".");fflush(stdout);
   sys_res= PQexec(sys_cnx,query); PQclear(sys_res);
   sprintf(query,"create index cluster_members_ndx0 on %s.cluster_members (CID,hostname)",varGet("qal_user"));
   printf(".");fflush(stdout);
   sys_res= PQexec(sys_cnx,query); PQclear(sys_res);
   sprintf(query,"create unique index backups_ix0 on %s.backups(cid,bck_id)",varGet("qal_user"));
   printf(".");fflush(stdout);
   sys_res= PQexec(sys_cnx,query); PQclear(sys_res);
   sprintf(query,"create        index backups_ix1 on %s.backups(bdate,bcktyp,bck_id)",varGet("qal_user"));
   printf(".");fflush(stdout);
   sys_res= PQexec(sys_cnx,query); PQclear(sys_res);
   sprintf(query,"create unique index backup_pieces_ix0 on %s.backup_pieces(cid,bck_id,pcid)",varGet("qal_user"));
   printf(".");fflush(stdout);
   sys_res= PQexec(sys_cnx,query); PQclear(sys_res);
   sprintf(query,"create unique index backup_ndx_ix0 on %s.backup_ndx(cid,bck_id,bd_id)",varGet("qal_user"));
   printf(".");fflush(stdout);
   sys_res= PQexec(sys_cnx,query); PQclear(sys_res);
   sprintf(query,"create unique index backup_wals_ix0 on %s.backup_wals(cid,walfile)",varGet("qal_user"));
   printf(".");fflush(stdout);
   sys_res= PQexec(sys_cnx,query); PQclear(sys_res);
   PQfinish(sys_cnx);
   printf(".\n");
   INFO("Deposit '%s' created.\n",varGet(GVAR_CLUNAME));
   globalArgs.verbosity=saved_verbosity;

   // Create/Update deposit reference
   varAdd(GVAR_DEPDESCR,varGet("qal_description"));
   varAdd(GVAR_DEPHOST ,varGet(GVAR_CLUHOST));
   varAdd(GVAR_DEPPORT ,varGet(GVAR_CLUPORT));
   varAdd(GVAR_DEPDB   ,varGet("qal_database"));
   varAdd(GVAR_DEPUSER ,varGet("qal_user"));
   varAdd(GVAR_DEPPWD  ,varGet("qal_password"));
   varAdd(GVAR_DEPNAME ,varGet(GVAR_CLUNAME));     
   globalArgs.configChanged=1;
   SaveConfigFile("DEPO",varGet(GVAR_DEPNAME),SECTION_DEPOSIT);
}
