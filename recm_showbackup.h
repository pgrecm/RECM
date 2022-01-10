/**********************************************************************************/
/* SHOW BACKUP command                                                            */
/* Usage:                                                                         */
/*    show backup                                                                 */                 
/*      Available Option(s):                                                      */
/*         /index                    used wit '/content', display indexes         */
/*         /table                    Used with '/content', display tables         */
/*         /content                  Display content of a backup                  */
/*         /verbose                                                               */
/*      Available Qualifier(s):                                                   */
/*         /cid=<STRING>             use a backup from a different cluster        */
/*         /uid=<STRING>             Delete a backup by it's UID                  */
/**********************************************************************************/                                      
void listDBs(int cluster_id,char *bck_id)
{
   char *query=malloc(1024);
   sprintf(query,"select db_oid,db_nam from %s.backup_dbs where cid=%d and bck_id='%s' order by db_nam",
                  varGet(GVAR_DEPUSER),
                  cluster_id,
                  bck_id);
   PGresult   *sres= PQexec(rep_cnx,query);
   if (PQresultStatus(sres) == PGRES_TUPLES_OK)
   {
      printf("%5s Databases:\n","");
      char *sdb_oid =malloc(20);
      char *sdb_nam =malloc(120);
      int rowsq = PQntuples(sres);
      for(int j=0; j < rowsq; j++)
      {
         strcpy(sdb_oid,  PQgetvalue(sres, j,0));
         strcpy(sdb_nam,  PQgetvalue(sres, j,1));
         printf("%10s %s (oid %s)\n","",sdb_nam,sdb_oid); 
      }
      free(sdb_oid);
      free(sdb_nam);
   }
   PQclear(sres);
   free(query);
}

void listDB_Tables(int cluster_id,char *bck_id)
{
   char *query=malloc(1024);
   sprintf(query,"select db_oid,db_nam from %s.backup_dbs where cid=%d and bck_id='%s' order by db_nam",
                  varGet(GVAR_DEPUSER),
                  cluster_id,
                  bck_id);
   PGresult   *sres= PQexec(rep_cnx,query);
   if (PQresultStatus(sres) == PGRES_TUPLES_OK)
   {
      printf("%5s Database Tables:\n","");
      char *sdb_oid =malloc(20);
      char *sdb_nam =malloc(120);
      int rowsq = PQntuples(sres);
      for(int j=0; j < rowsq; j++)
      {
         strcpy(sdb_oid,  PQgetvalue(sres, j,0));
         strcpy(sdb_nam,  PQgetvalue(sres, j,1));
         printf("%10s Tables of database %s (oid %s)\n","",sdb_nam,sdb_oid);
         sprintf(query,"select t.tab_sch,t.tab_nam,t.tab_fid,t.tbs_oid,case t.tbs_oid when 0 then '' else tbs.tbs_nam end "
                       "from %s.backup_tbl t "
                       "left join %s.backup_tbs tbs on (tbs.cid=t.cid and tbs.bck_id=t.bck_id) "
                       " where t.cid=%d" 
                       " and t.bck_id='%s' and t.tab_dbid=%s",
                       varGet(GVAR_DEPUSER),
                       varGet(GVAR_DEPUSER),
                       cluster_id,
                       bck_id,
                       sdb_oid);

         PGresult   *sres2= PQexec(rep_cnx,query);
         if (PQresultStatus(sres2) == PGRES_TUPLES_OK)
         {
            char *tab_sch=malloc(128);
            char *tab_nam=malloc(128);
            char *tab_fid=malloc(128);
            char *tbs_oid=malloc(128);
            char *tbs_nam=malloc(128);
            
            int rowsq2 = PQntuples(sres2);
            for(int k=0; k < rowsq2; k++)
            {
               strcpy(tab_sch,  PQgetvalue(sres2, k,0));
               strcpy(tab_nam,  PQgetvalue(sres2, k,1));
               strcpy(tab_fid,  PQgetvalue(sres2, k,2));
               strcpy(tbs_oid,  PQgetvalue(sres2, k,3));
               strcpy(tbs_nam,  PQgetvalue(sres2, k,4));
               printf("%15s %s.%s (fid %s)","",tab_sch,tab_nam,tab_fid);
               if (strcmp(tbs_oid,"0") != 0) { printf (" [tablespace %s (%s)]\n",tbs_nam,tbs_oid); }
                                        else { printf("\n"); }
            }
            free(tbs_nam);
            free(tbs_oid);
            free(tab_fid);
            free(tab_nam);
            free(tab_sch);
         }
         PQclear(sres2);
      }
      free(sdb_oid);
      free(sdb_nam);
   }
   PQclear(sres);
   free(query);
}

void listDB_Indexes(int cluster_id,char *bck_id)
{
   char *query=malloc(1024);
   sprintf(query,"select db_oid,db_nam from %s.backup_dbs where cid=%d and bck_id='%s' order by db_nam",
                  varGet(GVAR_DEPUSER),
                  cluster_id,
                  bck_id);
   PGresult   *sres= PQexec(rep_cnx,query);
   if (PQresultStatus(sres) == PGRES_TUPLES_OK)
   {
      printf("%5s Database Indexes:\n","");
      char *sdb_oid =malloc(20);
      char *sdb_nam =malloc(120);
      int rowsq = PQntuples(sres);
      for(int j=0; j < rowsq; j++)
      {
         strcpy(sdb_oid,  PQgetvalue(sres, j,0));
         strcpy(sdb_nam,  PQgetvalue(sres, j,1));
         printf("%10s Indexes of database %s (oid %s)\n","",sdb_nam,sdb_oid);
         sprintf(query,"select i.bd_sch,i.bd_tbl,i.bd_ndx  "
                       "from %s.backup_ndx i"
                       " where i.cid=%d" 
                       " and i.bck_id='%s' and i.bd_dbn='%s'",
                       varGet(GVAR_DEPUSER),
                       cluster_id,
                       bck_id,
                       sdb_nam);
         PGresult   *sres2= PQexec(rep_cnx,query);
         if (PQresultStatus(sres2) == PGRES_TUPLES_OK)
         {
            char *ndx_sch=malloc(128);
            char *ndx_tbl=malloc(128);
            char *ndx_nam=malloc(128);
            
            int rowsq2 = PQntuples(sres2);
            for(int k=0; k < rowsq2; k++)
            {
               strcpy(ndx_sch,  PQgetvalue(sres2, k,0));
               strcpy(ndx_tbl,  PQgetvalue(sres2, k,1));
               strcpy(ndx_nam,  PQgetvalue(sres2, k,2));
               
               printf("%15s (%s.%s) %s\n","",ndx_sch,ndx_tbl,ndx_nam);
            }
            free(ndx_sch);
            free(ndx_tbl);
            free(ndx_nam);
         }
         PQclear(sres2);
      }
      free(sdb_oid);
      free(sdb_nam);
   }
   PQclear(sres);
   free(query);
}


void listMembers(int cluster_id,char *bck_id,int with_content,const char *backup_location)
{
   char *query=malloc(1024);
   char *sf_info=malloc(20);
   char *sf_id =malloc(30); 
   char *sf_nam=malloc(1024);
   char *sf_psiz=malloc(30);
   char *sf_csiz=malloc(30);
   char *sf_cnt=malloc(30);   
   char *sf_dbn=malloc(128);
   char *sf_sch=malloc(128);
   char *sf_tbl=malloc(128);
   char *sf_ndx=malloc(128);
   char *sf_ddl=malloc(2048);   
   char *bts1=malloc(128);
   char *bts2=malloc(128);

   sprintf(query,"select p.pcid,p.pname,p.psize,p.csize,p.pcount from %s.backup_pieces p where p.cid=%d and p.bck_id='%s' order by pcid",
                  varGet(GVAR_DEPUSER),
                  cluster_id,
                  bck_id);
   TRACE("query:%s\n",query);
   PGresult   *sres= PQexec(rep_cnx,query);
   if (PQresultStatus(sres) == PGRES_TUPLES_OK)
   {
      int rowsq = PQntuples(sres);
      TRACE("rows=%d\n",rowsq);
      long bck_total_csize=0;
      long bck_total_psize=0;
      long bck_total_file=0;
      float percent=0.0;
      for(int j=0; j < rowsq; j++)
      {
         strcpy(sf_id,   PQgetvalue(sres, j,0));
         sprintf(sf_nam,"%s/%s",backup_location,PQgetvalue(sres, j,1));
         strcpy(sf_psiz, PQgetvalue(sres, j,2));
         strcpy(sf_csiz, PQgetvalue(sres, j,3));
         strcpy(sf_cnt,  PQgetvalue(sres, j,4));
         if (j == 0)
         {
/*            RecmFile *rf=RECMGetProperties(sf_nam);
            if (rf == NULL)
            {
               ERROR(ERR_INVRECMFIL,"Corrupted or invalid RECM file '%s'\n",sf_nam);
            }
            
            TRACE("## RecmFile Structure: piece name='%s'\n",rf->filename);
            TRACE("##   - Backup ID.........: '%s' (piece %d / %ld)\n",rf->bck_id,rf->piece,rf->pcount);
            TRACE("##   - Backup TYPE.......: '%s'\n",rf->bcktyp);
            TRACE("##   - PG version........: %d'\n",rf->version);
            TRACE("##   - PG timeline(TL)...: %d\n",rf->timeline);
            TRACE("##   - Backup TAG........: '%s'\n",rf->bcktag);
            TRACE("##   - Backup TYPE.......: '%s'\n",rf->bcktyp);
            TRACE("##   - Backup Size.......: %ld\n",rf->bcksize);
            TRACE("##   - Begin backup Date.: '%s'\n",rf->begin_date);
            TRACE("##   -       LSN.........: '%s'\n",rf->begin_wall);
            TRACE("##   -       WAL file....: '%s'\n",rf->begin_walf);
            TRACE("##   - End backup date...: '%s'\n",rf->end_date);
            TRACE("##   -       LSN.........: '%s'\n",rf->end_wall);
            TRACE("##   -       WAL file....: '%s'\n",rf->end_walf);
            TRACE("##   - Option: /noindex..: '%s'\n",rf->opt_noindex);
*/
            printf("\n");
            printf("%5s %3s  %10s %10s %s\n","","id","Files","Size","Piece");
            printf("%5s %3s %10s %10s %s\n","","---","----------","----------","--------------------------------");
         };
         long psize=atol(sf_psiz);
         long csize=atol(sf_csiz);
         TRACE("psize=%ld csize=%ld\n",psize,csize);
         strcpy(bts1,DisplayprettySize(psize));
         sf_info[0]=0x00;
         if (file_exists(sf_nam)==true) { sf_info[0]=0x00; } else { strcpy(sf_info,"(missing)"); };
         if (csize > 0) { percent=csize;
                          percent=(1-(percent / psize))*100;
                          strcpy(bts2,DisplayprettySize(csize));
                          printf("%5s %2s] %10s %10s (%s) %s %s [%02.2f %%]\n","",sf_id,sf_cnt,bts1,bts2,sf_nam,sf_info,percent);
                        }
                        else 
                        { printf("%5s %2s] %10s %10s %s %s\n","",sf_id,sf_cnt,bts1,sf_nam,sf_info); }
 
         if (with_content == true)
         {
            if (RECMopenRead(sf_nam)==true)
            {   
               VERBOSE("Reading piece '%s'\n",sf_nam); 
               RECMlistContent();
               RECMcloseRead();
            }
         }
         bck_total_psize=bck_total_psize+psize;
         bck_total_csize=bck_total_csize+csize;
         bck_total_file+=atol(sf_cnt);
      }
      printf("          %-20s %10s %s\n","--------------------","----------",""); 
      if (bck_total_csize == 0)
      {
         strcpy(bts1,DisplayprettySize(bck_total_psize));
         printf("               %9ld files %10s\n",bck_total_file,bts1);
      }
      else
      {
         percent=bck_total_csize;
         percent=(1-(percent / bck_total_psize))*100;
         strcpy(bts1,DisplayprettySize(bck_total_psize));
         strcpy(bts2,DisplayprettySize(bck_total_csize));
         printf("%5s %3s %10ld %10s (%s) [%2.2f%%]\n","","",bck_total_file,bts1,bts2,percent);
         };
   }
   PQclear(sres);   
   free(bts2);
   free(bts1);
   free(sf_ddl);
   free(sf_ndx);
   free(sf_tbl);
   free(sf_sch);
   free(sf_dbn);
   free(sf_cnt);
   free(sf_csiz);
   free(sf_psiz);
   free(sf_nam);
   free(sf_id);
   free(sf_info);
   free(query);   
}

// list backup/source="clustername"
void COMMAND_SHOWBACKUP(int idcmd,char *command_line)
{
   if (DEPOisConnected() == false) 
   {
      ERROR(ERR_NOTCONNECTED,"Not connected to any deposit.\n");
      return;
   }
   if ((CLUisConnected() == false) && qualifierIsUNSET("qal_source") == true &&qualifierIsUNSET("qal_cid") == true)
   {
      ERROR(ERR_NOTCONNECTED,"Not connected to any cluster.\n");
      return;
   };

   // Change verbosity
   int opt_verbose=optionIsSET("opt_verbose");
   int saved_verbose=globalArgs.verbosity;
   globalArgs.verbosity=opt_verbose;

   memBeginModule();
   
   int cluster_id;
   char *cluster_name=memAlloc(128); 
   if (CLUisConnected() == true)
   {
      if (strcmp(varGet(GVAR_CLUCID),VAR_UNSET_VALUE) != 0) cluster_id=varGetInt(GVAR_CLUCID);
      if (strcmp(varGet(GVAR_CLUNAME),VAR_UNSET_VALUE) != 0) strcpy(cluster_name,varGet(GVAR_CLUNAME));
   }
   else
   {
      cluster_id=-1;
      strcpy(cluster_name,"(null)");
   };
   
   char *backup_location=memAlloc(1024);
   char *query=memAlloc(1024);
   
// Display a backup from an other cluster by its NAMEe
   if (qualifierIsUNSET("qal_source") == false)
   {
      sprintf(query,"select count(*) from %s.clusters where clu_nam='%s'",
                    varGet(GVAR_DEPUSER),
                    varGet("qal_source"));
      int rows=DEPOquery(query,0);
      if (DEPOgetInt(0,0) == 0)
      {
         ERROR(ERR_BADCLUNAME,"Unknown cluster source name '%s'\n",varGet("qal_source"));
         DEPOqueryEnd();
         globalArgs.verbosity=saved_verbose;
         memEndModule();
         return;
      }
      DEPOqueryEnd();
      sprintf(query,"select CID from %s.clusters where clu_nam='%s'",
                       varGet(GVAR_DEPUSER),
                       varGet("qal_source"));
      rows=DEPOquery(query,0);
      cluster_id=DEPOgetInt(0,0);
      DEPOqueryEnd();
      strcpy(cluster_name,varGet("qal_source"));
      varAdd("qal_cid",QAL_UNSET);                                              // Take care to not try to use /CID after /SOURCE
   }

   // Display a backup from an other cluster by it's ID
   if (qualifierIsUNSET("qal_cid") == false)
   {
      sprintf(query,"select count(*) from %s.clusters where cid=%s",
                    varGet(GVAR_DEPUSER),
                    varGet("qal_cid"));
      int rows=DEPOquery(query,0);
      if (DEPOgetInt(0,0) == 0)
      {
         ERROR(ERR_BADCLUNAME,"Unknown cluster ID %s\n",varGet("qal_cid"));
         DEPOqueryEnd();
         globalArgs.verbosity=saved_verbose;
         memEndModule();
         return;
      }
      DEPOqueryEnd();      
      sprintf(query,"select clu_nam from %s.clusters where cid=%s",
                       varGet(GVAR_DEPUSER),
                       varGet("qal_cid"));
      rows=DEPOquery(query,0);
      strcpy(cluster_name,DEPOgetString(0,0));
      cluster_id=varGetInt("qal_cid");
      DEPOqueryEnd();
   };
   
   sprintf(query,"select bcktyp,bck_id,bcksts,case bcksts when 0 then 'AVAILABLE' when 1 then 'OBSOLETE' when 2 then 'INCOMPLETE' when 3 then 'RUNNING' else 'FAILED' end,bcktag,hstname,"
                 "       pgdata,pgversion,timeline,options,bcksize,pcount,"
                 "       coalesce(to_char(bdate,'%s'),'0000-00-00 00:00:00'),coalesce(bwall,''),coalesce(bwalf,''),"
                 "       coalesce(to_char(edate,'%s'),'0000-00-00 00:00:00'),coalesce(ewall,''),coalesce(ewalf,''), "
                 "       coalesce(ztime,''),coalesce(sdate,''),bckdir,"
                 "       trunc(coalesce(EXTRACT(EPOCH FROM (edate - bdate)),0))"
                 " from %s.backups  where cid=%d and bck_id='%s'",
                  varGet(GVAR_CLUDATE_FORMAT),
                  varGet(GVAR_CLUDATE_FORMAT),
                  varGet(GVAR_DEPUSER),
                  cluster_id,
                  varGet("qal_uid"));
   
   char *f_bcktyp=memAlloc(20);
   char *f_bck_id=memAlloc(20);
   char *f_bcksts=memAlloc(15);
   char *f_bcktag=memAlloc(256);
   char *f_hstname=memAlloc(256);
   char *f_pgdata=memAlloc(1024);
   char *f_pgversion=memAlloc(24);
   char *f_pgtimeline=memAlloc(24);
   long bck_options;
   char *f_bcksize=memAlloc(30);
   char *f_pcount=memAlloc(20);
   char *f_bdate=memAlloc(40);
   char *f_bwall=memAlloc(30);
   char *f_bwalf=memAlloc(30);
   char *f_edate=memAlloc(40);
   char *f_ewall=memAlloc(30);
   char *f_ewalf=memAlloc(30);
   char *f_ztime=memAlloc(100);
   char *f_sdate=memAlloc(100);
   char *opts=memAlloc(128);
   long f_elapsed=0;
   long bck_full=0;
   long bck_wal=0;
   long bck_cfg=0;
   long bck_meta=0;
   
   long bck_stat_available=0;
   long bck_stat_obsolete=0;
   long bck_stat_incomplete=0;
   long bck_stat_running=0;
   long bck_stat_failed=0;
   int bcksts_n;
   
   int rows=DEPOquery(query,0);
   TRACE("query=%s\n",query);
   TRACE("rows=%d\n",rows);
   if (rows == 0)
   {
      INFO("Backup '%s' not found for cluster '%s'\n",varGet("qal_uid"),cluster_name);
   }
   else
   {
      int n=0;
      printf("Backup UID '%s'   Cluster '%s' (CID=%d)\n",varGet("qal_uid"),cluster_name,cluster_id);
      strcpy(f_bcktyp,DEPOgetString(n,0)); TRACE("f_bcktyp       : '%s'\n",DEPOgetString(n,0)); 
      strcpy(f_bck_id,       DEPOgetString(n,1)); TRACE("f_bck_id       : '%s'\n",DEPOgetString(n,1)); 
      bcksts_n=              DEPOgetInt(n,2);     TRACE("n              : '%d'\n",DEPOgetInt(n,2));     
      strcpy(f_bcksts,       DEPOgetString(n, 3));TRACE("f_bcksts       : '%s'\n",DEPOgetString(n, 3));
      strcpy(f_bcktag,       DEPOgetString(n, 4));TRACE("f_bcktag       : '%s'\n",DEPOgetString(n, 4));
      strcpy(f_hstname,      DEPOgetString(n, 5));TRACE("f_hstname      : '%s'\n",DEPOgetString(n, 5));
      strcpy(f_pgdata,       DEPOgetString(n, 6));TRACE("f_pgdata       : '%s'\n",DEPOgetString(n, 6));
      strcpy(f_pgversion,    DEPOgetString(n, 7));TRACE("f_pgversion    : '%s'\n",DEPOgetString(n, 7));
      strcpy(f_pgtimeline,   DEPOgetString(n, 8));TRACE("f_pgtimeline   : '%s'\n",DEPOgetString(n, 8));
      bck_options=DEPOgetLong(n, 9); TRACE("bck_options      : '%ld'\n",DEPOgetLong(n, 9)); /* Change : 0xA000A */
      strcpy(f_bcksize,      DEPOgetString(n,10));TRACE("f_bcksize      : '%s'\n",DEPOgetString(n,10));
      strcpy(f_pcount,       DEPOgetString(n,11));TRACE("f_pcount       : '%s'\n",DEPOgetString(n,11));
      strcpy(f_bdate,        DEPOgetString(n,12));TRACE("f_bdate        : '%s'\n",DEPOgetString(n,12));
      strcpy(f_bwall,        DEPOgetString(n,13));TRACE("f_bwall        : '%s'\n",DEPOgetString(n,13));
      strcpy(f_bwalf,        DEPOgetString(n,14));TRACE("f_bwalf        : '%s'\n",DEPOgetString(n,14));
      strcpy(f_edate,        DEPOgetString(n,15));TRACE("f_edate        : '%s'\n",DEPOgetString(n,15));
      strcpy(f_ewall,        DEPOgetString(n,16));TRACE("f_ewall        : '%s'\n",DEPOgetString(n,16));
      strcpy(f_ewalf,        DEPOgetString(n,17));TRACE("f_ewalf        : '%s'\n",DEPOgetString(n,17));
      strcpy(f_ztime,        DEPOgetString(n,18));TRACE("f_ztime        : '%s'\n",DEPOgetString(n,18));
      strcpy(f_sdate,        DEPOgetString(n,19));TRACE("f_sdate        : '%s'\n",DEPOgetString(n,19));
      strcpy(backup_location,DEPOgetString(n,20));TRACE("backup_location: '%s'\n",DEPOgetString(n,20));
      
      int nopt=0;
      strcpy(opts,"Options: ");
      if ((bck_options & BACKUP_OPTION_COMPRESSED) == BACKUP_OPTION_COMPRESSED)   { nopt++; strcat(opts,"compressed(C)"); };
      if ((bck_options & BACKUP_OPTION_NOINDEX)   == BACKUP_OPTION_NOINDEX)       { if (nopt > 0) strcat(opts,","); nopt++; strcat(opts," no_indexes(I)"); };
      if ((bck_options & BACKUP_OPTION_EXCLUSIVE) == BACKUP_OPTION_EXCLUSIVE)     { if (nopt > 0) strcat(opts,","); nopt++; strcat(opts," exclusive(E)"); };
      if ((bck_options & BACKUP_OPTION_KEEPFOREVER) == BACKUP_OPTION_KEEPFOREVER) { if (nopt > 0) strcat(opts,","); nopt++; strcat(opts," locked(L)"); };

      
         
      f_elapsed=DEPOgetLong(n,21);

      printf("%-5s Backup type..: %-10s     Version.....: %-3s           Status:%s\n","" ,f_bcktyp,f_pgversion,f_bcksts);
      
      if (strcmp(DEPOgetString(n, 0),RECM_BCKTYP_FULL) == 0)
      {
         printf("%-5s TimeLine(TL).: %-10s     Tag.........: '%s'\n","",            f_pgtimeline,f_bcktag);
         printf("%-5s DateStyle(DS): %-10s     TimeZone(TZ): '%s'\n","",            f_sdate,f_ztime);
         printf("%-5s Size.........: %ld Bytes (%ld MB) in %s piece(s)\n" ,"",atol(f_bcksize),(atol(f_bcksize)/1024/1024), f_pcount);
         printf("%-5s Started: %-20s  WAL: %s(%s)  Elapsed Time: %s\n","",              f_bdate,f_bwalf,f_bwall,DisplayElapsed(f_elapsed));
         printf("%-5s Ended..: %-20s  WAL: %s(%s)\n","",              f_edate,f_ewalf,f_ewall);
         if (nopt > 0) { printf("%-5s %s\n","",opts); };

         listMembers(cluster_id,f_bck_id,optionIsSET("opt_content"),backup_location);
         if (optionIsSET("opt_verbose") == true) 
         {
            if (optionIsSET("opt_table") == false && optionIsSET("opt_index") == false) 
            {
               listDBs(cluster_id,f_bck_id); 
            }
            else
            {
               if (optionIsSET("opt_table") == true) { listDB_Tables(cluster_id,f_bck_id); };
               if (optionIsSET("opt_index") == true) { listDB_Indexes(cluster_id,f_bck_id); };
            };                                          
         }
      };
      if (strcmp(DEPOgetString(n, 0),RECM_BCKTYP_WAL) == 0)
      {
         printf("%-5s TimeLine(TL).: %-10s     Tag.........: '%s'\n","",      f_pgtimeline,f_bcktag);
         printf("%-5s DateStyle(DS): %-10s     TimeZone(TZ): '%s'\n","",      f_sdate,f_ztime);
         printf("%-5s Size.........: %ld Bytes (%ld MB) in %s piece(s)\n" ,"",atol(f_bcksize),(atol(f_bcksize)/1024/1024), f_pcount);
         printf("%-5s Started......: %-20s  Elapsed Time: %s\n","",           f_bdate,DisplayElapsed(f_elapsed));
         printf("%-5s Ended........: %-20s\n","",                             f_edate);
         if (nopt > 0) { printf("%-5s %s\n","",opts); };
         listMembers(cluster_id,f_bck_id,optionIsSET("opt_content"),backup_location);
      };
      if (strcmp(DEPOgetString(n, 0),RECM_BCKTYP_CFG) == 0)
      {
         printf("%-5s Size...: %ld Bytes (%ld MB) in %s piece(s)\n" ,"",atol(f_bcksize),(atol(f_bcksize)/1024/1024), f_pcount);
         printf("%-5s Started:%-20s  Elapsed Time:%s\n","",                  f_bdate,DisplayElapsed(f_elapsed));
         printf("%-5s Ended..:%-20s\n","",              f_edate);
         if (nopt > 0) { printf("%-5s %s\n","",opts); };
         listMembers(cluster_id,f_bck_id,optionIsSET("opt_content"),backup_location);
      };  
      if (strcmp(DEPOgetString(n, 0),RECM_BCKTYP_META) == 0)
      {
         printf("%-5s Size...: %ld Bytes (%ld MB) in %s piece(s)\n" ,"",atol(f_bcksize),(atol(f_bcksize)/1024/1024), f_pcount);
         printf("%-5s Started: %-20s  Elapsed Time:%s\n","",                  f_bdate,DisplayElapsed(f_elapsed));
         printf("%-5s Ended..: %-20s\n","",              f_edate);
         if (nopt > 0) { printf("%-5s %s\n","",opts); };
         listMembers(cluster_id,f_bck_id,optionIsSET("opt_content"),backup_location);
      };  
   }
   DEPOqueryEnd();
   // Restore verbosity
   globalArgs.verbosity=saved_verbose;
   memEndModule();
}
