/**********************************************************************************/
/* CREATE DEPOSIT command                                                         */
/* Usage:                                                                         */
/*    create deposit                                                              */                 
/*      Available Qualifier(s):                                                   */
/*         /usr=<STRING>              User to create to connect to DEPOSIT        */
/*         /pwd=<STRING>              Password                                    */
/*         /db=<STRING>               DEPOSIT database name to create             */
/**********************************************************************************/
void COMMAND_CREATEDEPOSIT(int idcmd,char *command_line)
{
   int option_force=optionIsSET("opt_force");
   int option_verbose=optionIsSET("opt_verbose");
   int saved_verbosity=globalArgs.verbosity;

   globalArgs.verbosity=option_verbose;
   
   if (clu_cnx == NULL)                                                                                                            // Must be connected on a cluster
   {
      ERROR(ERR_NOTCONNECTED,"Not connected. Connect first to a cluster.\n");
      globalArgs.verbosity=saved_verbosity;
      return;
   };
   memBeginModule();
   char *query=memAlloc(1024);

   
   // Find what already exist
   sprintf(query,"select a.db_ok,b.usr_ok from (select count(*) as db_ok from pg_database where datname='%s') a,(select count(*) as usr_ok"
                 " from pg_roles where rolname='%s') b",
                 varGet("qal_db"),
                 varGet("qal_usr"));
   PGresult *sys_res = PQexec(clu_cnx, query);
   if (PQresultStatus(sys_res) != PGRES_TUPLES_OK) 
   { 
      ERROR(ERR_DBQRYERROR,"Precheck create error.\n"); 
      globalArgs.verbosity=saved_verbosity;
      memEndModule();
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
      ERROR(ERR_DEPOEXIST,"Deposit '%s' already exist. Use '/force' to recreate it.\n",varGet("qal_db"));
      globalArgs.verbosity=saved_verbosity;
      memEndModule();
      return;
   }
   //# Drop database ################################################
   if (is_rdb_ok == 1 && option_force == true)
   {
      sprintf(query,"drop database %s",varGet("qal_db"));
      sys_res = PQexec(clu_cnx, query);
      if (PQresultStatus(sys_res) != PGRES_COMMAND_OK) 
      { 
        ERROR(ERR_DROPDB,"Could not drop database '%s'\n",varGet("qal_db")); 
        globalArgs.verbosity=saved_verbosity;
        memEndModule();
        return;
      };
      PQclear(sys_res);
   }
   sprintf(query,"create database %s",varGet("qal_db"));
   sys_res = PQexec(clu_cnx, query);
   if (PQresultStatus(sys_res) != PGRES_COMMAND_OK) 
   { 
      ERROR(ERR_CREATEDB,"Could not create database '%s'\nError:%s\n",varGet("qal_db"),PQerrorMessage(clu_cnx)); 
      globalArgs.verbosity=saved_verbosity;
      memEndModule();
      return;
   };
   PQclear(sys_res);
   //# Create user ##################################################
   if (is_usr_ok == 0)
   {
      sprintf(query,"create role %s with password '%s' login",varGet("qal_usr"),varGet("qal_pwd"));
      sys_res= PQexec(clu_cnx,query);
      if (PQresultStatus(sys_res) != PGRES_COMMAND_OK)
      {
         PQclear(sys_res);
         ERROR(ERR_CREATEROLE,"Could not create role\nError:%s\n",PQerrorMessage(clu_cnx));
         globalArgs.verbosity=saved_verbosity;
         memEndModule();
         return;
      };
      PQclear(sys_res);
   };
   //# grants #######################################################
   sprintf(query,"grant create on database %s to %s",varGet("qal_db"),varGet("qal_usr"));
   sys_res= PQexec(clu_cnx,query); 
   if (PQresultStatus(sys_res) != PGRES_COMMAND_OK)
   {
      PQclear(sys_res);
      ERROR(ERR_GRANTDB,"Could not grant create\nError:%s\n",PQerrorMessage(clu_cnx));
      globalArgs.verbosity=saved_verbosity;
      memEndModule();
      return;
   }; 
   sprintf(query,"alter role %s set search_path=\"$user\", %s",varGet("qal_usr"),varGet("qal_usr"));
   sys_res= PQexec(clu_cnx,query);
   if (PQresultStatus(sys_res) != PGRES_COMMAND_OK)
   {
      PQclear(sys_res);
      ERROR(ERR_GRANTROLE,"Could not grant create\nError:%s\n",PQerrorMessage(clu_cnx));
      globalArgs.verbosity=saved_verbosity;
      memEndModule();
      return;
   }; 

   //# Now, create objects #############################################
   char *connectString=memAlloc(1024);
   sprintf(connectString,"dbname=%s user=%s password=%s host=%s port=%s",
                         varGet("qal_db"),
                         varGet("qal_usr"),
                         varGet("qal_pwd"),
                         varGet(GVAR_CLUHOST),
                         varGet(GVAR_CLUPORT));
   PGconn     *sys_cnx=NULL;

   sys_cnx = PQconnectdb(connectString);
   if (PQstatus(sys_cnx) != CONNECTION_OK) 
   { 
      ERROR(ERR_NOTCONNECTED,"Connection to host '%s' failed\n",varGet(GVAR_CLUHOST)); 
      globalArgs.verbosity=saved_verbosity;
      memEndModule();
      return;
   };

   // Now, we need to take care of the rights of the user to the database to create objects
   sprintf(query,"create schema %s authorization %s",varGet("qal_usr"),varGet("qal_usr"));
   sys_res= PQexec(sys_cnx,query); 
   if (PQresultStatus(sys_res) != PGRES_COMMAND_OK)
   {
      PQclear(sys_res);
      ERROR(ERR_CREATESCHEMA,"Could not create schema\nError:%s\n",PQerrorMessage(sys_cnx));
      globalArgs.verbosity=saved_verbosity;
      memEndModule();
      return;
   }; 
   PQclear(sys_res);
   sprintf(query,"grant create on schema %s to %s",varGet("qal_usr"),varGet("qal_usr"));
   sys_res= PQexec(sys_cnx,query);
   if (PQresultStatus(sys_res) != PGRES_COMMAND_OK)
   {
      PQclear(sys_res);
      ERROR(ERR_GRANTSCHEMA,"Could not grant usage\nError:%s\n",PQerrorMessage(sys_cnx));
      globalArgs.verbosity=saved_verbosity;
      memEndModule();
      return;
   }; 
   PQclear(sys_res);
   sprintf(query,"grant usage on schema %s to %s",varGet("qal_usr"),varGet("qal_usr"));
   sys_res= PQexec(sys_cnx,query);
   if (PQresultStatus(sys_res) != PGRES_COMMAND_OK)
   {
      PQclear(sys_res);
      ERROR(ERR_GRANTUSAGE,"Could not grant usage\nError:%s\n",PQerrorMessage(sys_cnx));
      globalArgs.verbosity=saved_verbosity;
      memEndModule();
      return;
   }; 
   PQclear(sys_res);
   sprintf(query,"create table %s.config ( deponame varchar(32),"
                                          "version varchar(10),"
                                          "description text,"
                                          "created timestamp default current_timestamp,"
                                          "updated timestamp default current_timestamp)",
                                          varGet("qal_usr"));
   sys_res= PQexec(sys_cnx,query); 
   if (PQresultStatus(sys_res) != PGRES_COMMAND_OK)
   {
      PQclear(sys_res);
      ERROR(ERR_CREATTABLE,"Could not create table 'config'\nError:%s\n",PQerrorMessage(sys_cnx));
      globalArgs.verbosity=saved_verbosity;
      memEndModule();
      return;
   };
   PQclear(sys_res);
   sprintf(query,"insert into %s.config (deponame,version,description) values ('%s','%s','Recovery Manager Deposit(PostgreSQL). Release installation')",
                  varGet("qal_usr"),
                  varGet(GVAR_CLUNAME),
                  varGet(RECM_VERSION)
                  );
   sys_res= PQexec(sys_cnx,query); 
   if (PQresultStatus(sys_res) != PGRES_COMMAND_OK)
   {
      PQclear(sys_res);
      ERROR(ERR_CREATTABLE,"Could not create table 'config'\nError:%s\n",PQerrorMessage(sys_cnx));
      globalArgs.verbosity=saved_verbosity;
      memEndModule();
      return;
   };
   PQclear(sys_res);
   sprintf(query,"create table %s.clusters(CID          bigint not null,"
                                          "rep_nam      varchar(128),"                              // not used
                                          "clu_sts      varchar(15), "                              // Cluster status 'CURRENT'
                                          "clu_nam      varchar(128) not null,"                     // name of the cluster
                                          "clu_cre      timestamp default current_timestamp,"       // Creation date
                                          "clu_info     text,"                                      // Description
                                          "clu_addr     varchar(60),"                               // IP address of the cluster
                                          "clu_port     varchar(20),"                               // listening of the cluster
                                          "rep_usr      varchar(60),"                               // replication user
                                          "rep_pwd      text,"                                      // replication user's password
                                          "rep_opts     text,"                                      // replication options
                                          "opt_delwal   integer default -1,"                        // Options - delete WAL files when backuped (-1 = disabled)
                                          "opt_blksize  varchar(20) default '16M',"                 //         - block size at backup/restore time 
                                          "opt_compress integer default -1,"                        //         - compression -1 = disabled
                                          "opt_retfull  varchar(20) default 'count:7',"             //         - retention full
                                          "opt_retmeta  varchar(20) default 'days:30',"              //         - retention meta
                                          "opt_retcfg   varchar(20) default 'days:7',"               //         - retention cfg 
                                          "opt_maxsize  varchar(20) default '10G',"                 //         -  maxsize of a backup piece
                                          "opt_maxfiles bigint default 200,"                        //         - maximum files within a backup piece
                                          "opt_datfmt   varchar(40) default 'YYYY-MM-DD HH24:MI:SS'," //         - date format
                                          "opt_waldir   text  default '',"                          //         - WAL directory (not <PGDATA>/pg_wal)
                                          "opt_bkpdir   text  default '',"                          //         - Backup direcotry
                                          "opt_concurindex integer default 0,"
                                          "lstfull      timestamp,"                                 // last FULL backup date
                                          "lstwal       timestamp,"                                 // last WAL backup
                                          "lstcfg       timestamp)",varGet("qal_usr"));            // last CFG backup
   printf(".");fflush(stdout);
   sys_res= PQexec(sys_cnx,query); 
   if (PQresultStatus(sys_res) != PGRES_COMMAND_OK)
   {
      PQclear(sys_res);
      ERROR(ERR_CREATTABLE,"Could not create table 'clusters'\nError:%s\n",PQerrorMessage(sys_cnx));
      globalArgs.verbosity=saved_verbosity;
      memEndModule();
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
                                         "options   bigint,"                                 //    -- options 'compressed|noindex|exclusive'
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
                                         "sdate     varchar(128))",varGet("qal_usr"));      //    -- date style of the database at backup time
   printf(".");fflush(stdout);
   sys_res= PQexec(sys_cnx,query);
   if (PQresultStatus(sys_res) != PGRES_COMMAND_OK)
   {
      PQclear(sys_res);
      ERROR(ERR_CREATTABLE,"Could not create table 'backups'\nError:%s\n",PQerrorMessage(sys_cnx));
      globalArgs.verbosity=saved_verbosity;
      memEndModule();
      return;
   };
   PQclear(sys_res);
   sprintf(query,"create table %s.backup_pieces (CID      bigint not null,"
                                                "bck_id   varchar(30) not null,"             //        -- Backup ID 
                                                "pcid     bigint,"                           //        -- piece number
                                                "pname    varchar(1024), "                   //        -- Part file name
                                                "psize    bigint,"                           //        -- Total Size of the backup part(in MB)
                                                "csize    bigint,"                           //        -- Total Size of the backup part(in MB) compressed
                                                "pcount   bigint,"                           //        -- Number of files in the piece
                                                "bwal     timestamp,"                        //        -- For WAL backup, date of the first WAL
                                                "ewal     timestamp)",varGet("qal_usr"));           //        -- or WAL backup, date of the last WAL  
   printf(".");fflush(stdout);
   sys_res= PQexec(sys_cnx,query);
   if (PQresultStatus(sys_res) != PGRES_COMMAND_OK)
   {
      PQclear(sys_res);
      ERROR(ERR_CREATTABLE,"Could not create table 'backup_pieces'\nError:%s\n",PQerrorMessage(sys_cnx));
      globalArgs.verbosity=saved_verbosity;
      memEndModule();
      return;
   };
   PQclear(sys_res);
   sprintf(query,"create table %s.backup_wals (CID bigint not null,"
                                              "bck_id varchar(30) not null,"
                                              "walfile varchar(128),"
                                              "ondisk integer,"
                                              "bckcount integer,"
                                              "edate timestamp)",varGet("qal_usr"));
   printf(".");fflush(stdout);
   sys_res= PQexec(sys_cnx,query);
   if (PQresultStatus(sys_res) != PGRES_COMMAND_OK)
   {
      PQclear(sys_res);
      ERROR(ERR_CREATTABLE,"Could not create table 'backup_wals'\nError:%s\n",PQerrorMessage(sys_cnx));
      globalArgs.verbosity=saved_verbosity;
      memEndModule();
      return;
   };
   PQclear(sys_res);
      
   sprintf(query,"create table %s.backup_dbs (CID      bigint not null,"
                                             "bck_id   varchar(30) not null, "        //      -- backup id
                                             "db_nam   varchar(30),        "          //      -- database name
                                             "db_oid   oid,                "          //      -- OID of the database (is the relfilenode of the db)
                                             "db_typ   int)",varGet("qal_usr"));      //      -- 0=system db // 1=template // 2=user db
   printf(".");fflush(stdout);
   sys_res= PQexec(sys_cnx,query);
   if (PQresultStatus(sys_res) != PGRES_COMMAND_OK)
   {
      PQclear(sys_res);
      ERROR(ERR_CREATTABLE,"Could not create table 'backup_dbs'\nError:%s\n",PQerrorMessage(sys_cnx));
      globalArgs.verbosity=saved_verbosity;
      memEndModule();
      return;
   };
   PQclear(sys_res);

   sprintf(query,"create table %s.backup_tbl (CID        bigint not null,"
                                             "bck_id     varchar(30) not null,"                 //      -- backup id
                                             "tab_dbid   oid,        "                 //      -- database name
                                             "tab_sch    varchar(30),"
                                             "tab_nam    varchar(30),"                 //      -- OID of the database (is the relfilenode of the db)
                                             "tab_fid    bigint,"                      //      -- 0=system db // 1=template // 2=user db
                                             "tbs_oid    bigint)",                     //      -- OID of the tablespace",
                  varGet("qal_usr")); 
   printf(".");fflush(stdout);
   sys_res= PQexec(sys_cnx,query);
   if (PQresultStatus(sys_res) != PGRES_COMMAND_OK)
   {
      PQclear(sys_res);
      ERROR(ERR_CREATTABLE,"Could not create table 'backup_tbl'\nError:%s\n",PQerrorMessage(sys_cnx));
      globalArgs.verbosity=saved_verbosity;
      memEndModule();
      return;
   };
   PQclear(sys_res);
      
   sprintf(query,"create table %s.backup_ndx (CID      bigint not null,"
                                             "bck_id   varchar(30) not null,"               //      -- Unique ID of the backup
                                             "bd_id    bigserial,"                          //
                                             "bd_ndx   varchar(128),"                      //      -- Part file name
                                             "bd_dbn   varchar(128),"                       //      -- Database name
                                             "bd_sch   varchar(128),"                       //      -- Schema
                                             "bd_tbl   varchar(128))",varGet("qal_usr"));            //      -- Tablename
   printf(".");fflush(stdout);
   sys_res= PQexec(sys_cnx,query);
   if (PQresultStatus(sys_res) != PGRES_COMMAND_OK)
   {
      PQclear(sys_res);
      ERROR(ERR_CREATTABLE,"Could not create table 'backup_ndx'\nError:%s\n",PQerrorMessage(sys_cnx));
      globalArgs.verbosity=saved_verbosity;
      memEndModule();
      return;
   };
   PQclear(sys_res);

   sprintf(query,"create table %s.backup_tbs (CID       bigint not null,"
                                             "bck_id    varchar(30) not null,"               //      -- Unique ID of the backup
                                             "tbs_oid   oid,"                          //
                                             "tbs_nam   varchar(128),"                      //      -- Part file name
                                             "tbs_loc   text)",varGet("qal_usr"));            //      -- Tablename
   printf(".");fflush(stdout);
   sys_res= PQexec(sys_cnx,query);
   if (PQresultStatus(sys_res) != PGRES_COMMAND_OK)
   {
      PQclear(sys_res);
      ERROR(ERR_CREATTABLE,"Could not create table 'backup_tbs'\nError:%s\n",PQerrorMessage(sys_cnx));
      globalArgs.verbosity=saved_verbosity;
      memEndModule();
      return;
   };
   PQclear(sys_res);

   sprintf(query,"create table %s.backup_rp (cid     bigint not null,"
                                            "rp_name varchar(30) not null,"               //      -- Unique ID of the backup
                                            "rp_typ  varchar(5),"                         // "T" for temporary / "P" for permanent"
                                            "lsn     varchar(30),"
                                            "tl      bigint,"
                                            "wal     varchar(30),"
                                            "crdate  timestamp default now())",
                                            varGet("qal_usr"));
   printf(".");fflush(stdout);
   sys_res= PQexec(sys_cnx,query);
   if (PQresultStatus(sys_res) != PGRES_COMMAND_OK)
   {
      PQclear(sys_res);
      ERROR(ERR_CREATTABLE,"Could not create table 'backup_rp'\nError:%s\n",PQerrorMessage(sys_cnx));
      globalArgs.verbosity=saved_verbosity;
      memEndModule();
      return;
   };
   PQclear(sys_res);

   sprintf(query,"create index clusters_ndx0 on %s.clusters (clu_nam)",varGet("qal_usr"));
   printf(".");fflush(stdout);   sys_res= PQexec(sys_cnx,query); PQclear(sys_res);
   sprintf(query,"create unique index backups_ix0 on %s.backups(cid,bck_id)",varGet("qal_usr"));
   printf(".");fflush(stdout);   sys_res= PQexec(sys_cnx,query); PQclear(sys_res);
   sprintf(query,"create        index backups_ix1 on %s.backups(bdate,bcktyp,bck_id)",varGet("qal_usr"));
   printf(".");fflush(stdout);   sys_res= PQexec(sys_cnx,query); PQclear(sys_res);
   sprintf(query,"create unique index backup_pieces_ix0 on %s.backup_pieces(cid,bck_id,pcid)",varGet("qal_usr"));
   printf(".");fflush(stdout);   sys_res= PQexec(sys_cnx,query); PQclear(sys_res);
   sprintf(query,"create unique index backup_ndx_ix0 on %s.backup_ndx(cid,bck_id,bd_id)",varGet("qal_usr"));
   printf(".");fflush(stdout);   sys_res= PQexec(sys_cnx,query); PQclear(sys_res);
   sprintf(query,"create unique index backup_wals_ix0 on %s.backup_wals(cid,bck_id,walfile)",varGet("qal_usr"));
   printf(".");fflush(stdout);   sys_res= PQexec(sys_cnx,query); PQclear(sys_res);
   
   sprintf(query,"create index backup_db_ndx0 on %s.backup_dbs (cid,bck_id,db_nam)",varGet("qal_usr"));
   printf(".");fflush(stdout);   sys_res= PQexec(sys_cnx,query); PQclear(sys_res);

   sprintf(query,"create unique index backup_rp_ndx0 on %s.backup_rp (cid,rp_name)",varGet("qal_usr"));
   printf(".");fflush(stdout);   sys_res= PQexec(sys_cnx,query); PQclear(sys_res);

   sprintf(query,"create index backup_tbl_ndx0 on %s.backup_tbl (cid,bck_id,tab_nam)",varGet("qal_usr"));
   printf(".");fflush(stdout);   sys_res= PQexec(sys_cnx,query); PQclear(sys_res);
   
   PQfinish(sys_cnx);
   printf(".\n");
   INFO("Deposit '%s' created.\n",varGet(GVAR_CLUNAME));

   // Create/Update deposit reference
   varAdd(GVAR_DEPDESCR,varGet("qal_description"));
   varAdd(GVAR_DEPHOST ,varGet(GVAR_CLUHOST));
   varAdd(GVAR_DEPPORT ,varGet(GVAR_CLUPORT));
   varAdd(GVAR_DEPDB   ,varGet("qal_db"));
   varAdd(GVAR_DEPUSER ,varGet("qal_usr"));
   varAdd(GVAR_DEPPWD  ,varGet("qal_pwd"));
   varAdd(GVAR_DEPNAME ,varGet(GVAR_CLUNAME));     
   globalArgs.configChanged=1;
   SaveConfigFile("DEPO",varGet(GVAR_DEPNAME),SECTION_DEPOSIT);
   globalArgs.verbosity=saved_verbosity;
   memEndModule();
}
