/**********************************************************************************/
/* SHOW BACKUP command                                                            */
/* Usage:                                                                         */
/*    show backup /uid=NNN                                                        */                 
/*    show backup /uid=NNN /index                                                 */                 
/*    show backup /uid=NNN /table                                                 */                 
/*    show backup /uid=NNN /content                                               */                 
/*    show backup /uid=NNN /tablespace                                            */                 
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

void show_backup_Databases(int cluster_id,char *bck_id)
{
   char *query=malloc(1024);
   sprintf(query,"select db_oid,db_nam from %s.backup_dbs where cid=%d and bck_id='%s' order by db_nam",
                  varGet(GVAR_DEPUSER),
                  cluster_id,
                  bck_id);
   TRACE("query='%s'\n",query);
   PGresult   *sres= PQexec(rep_cnx,query);
   if (PQresultStatus(sres) == PGRES_TUPLES_OK)
   {
      int rowsq = PQntuples(sres);
      for(int j=0; j < rowsq; j++)
      {
         if (j == 0) printf("Databases\n");
         printf("   %2d) '%s' [%s]:\n",(j+1),PQgetvalue(sres, j,1),PQgetvalue(sres, j,0));
      };
   }
   PQclear(sres);
   free(query);
}

void show_backup_Tables(int cluster_id,char *bck_id)
{
   char *query=malloc(1024);
   sprintf(query,"select db_oid,db_nam from %s.backup_dbs where cid=%d and bck_id='%s' order by db_nam",
                  varGet(GVAR_DEPUSER),
                  cluster_id,
                  bck_id);
   TRACE("query='%s'\n",query);
   PGresult   *sres= PQexec(rep_cnx,query);
   if (PQresultStatus(sres) == PGRES_TUPLES_OK)
   {
      char *sdb_oid =malloc(20);
      char *sdb_nam =malloc(120);
      char *tab_sch=malloc(128);
      char *tab_nam=malloc(128);
      char *tab_fid=malloc(128);
      char *tbs_oid=malloc(128);
      char *tbs_nam=malloc(128);
      int rowsq = PQntuples(sres);
      for(int j=0; j < rowsq; j++)
      {
         strcpy(sdb_oid,  PQgetvalue(sres, j,0));
         strcpy(sdb_nam,  PQgetvalue(sres, j,1));
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
         TRACE("query2='%s'\n",query);

         PGresult   *sres2= PQexec(rep_cnx,query);
         if (PQresultStatus(sres2) == PGRES_TUPLES_OK)
         {
            int rowsq2 = PQntuples(sres2);
            for(int k=0; k < rowsq2; k++)
            {
               if (k == 0 && j == 0)  printf("Tables\n");
               if (k == 0)            printf("   Database '%s' [%s]:\n",sdb_nam,sdb_oid);
               strcpy(tab_sch,  PQgetvalue(sres2, k,0));
               strcpy(tab_nam,  PQgetvalue(sres2, k,1));
               strcpy(tab_fid,  PQgetvalue(sres2, k,2));
               strcpy(tbs_oid,  PQgetvalue(sres2, k,3));
               strcpy(tbs_nam,  PQgetvalue(sres2, k,4));
               printf("      %s.%s (fid %s)",tab_sch,tab_nam,tab_fid);
               if (strcmp(tbs_oid,"0") != 0) { printf (" [tablespace %s (%s)]\n",tbs_nam,tbs_oid); }
                                        else { printf("\n"); }
            }
         }
         PQclear(sres2);
      };
      free(tbs_nam);
      free(tbs_oid);
      free(tab_fid);
      free(tab_nam);
      free(tab_sch);
      free(sdb_oid);
      free(sdb_nam);
   }
   PQclear(sres);
   free(query);
}

void show_backup_Indexes(int cluster_id,char *bck_id)
{
   char *query=malloc(1024);
   sprintf(query,"select db_oid,db_nam from %s.backup_dbs where cid=%d and bck_id='%s' order by db_nam",
                  varGet(GVAR_DEPUSER),
                  cluster_id,
                  bck_id);
   TRACE("query='%s'\n",query);
   PGresult   *sres= PQexec(rep_cnx,query);
   if (PQresultStatus(sres) == PGRES_TUPLES_OK)
   {
      char *sdb_oid =malloc(20);
      char *sdb_nam =malloc(120);
      char *ndx_sch=malloc(128);
      char *ndx_tbl=malloc(128);
      char *ndx_nam=malloc(128);
      int rowsq = PQntuples(sres);
      for(int j=0; j < rowsq; j++)
      {
         strcpy(sdb_oid,  PQgetvalue(sres, j,0));
         strcpy(sdb_nam,  PQgetvalue(sres, j,1));
         sprintf(query,"select i.bd_sch,i.bd_tbl,i.bd_ndx  "
                       "from %s.backup_ndx i"
                       " where i.cid=%d" 
                       " and i.bck_id='%s' and i.bd_dbn='%s'",
                       varGet(GVAR_DEPUSER),
                       cluster_id,
                       bck_id,
                       sdb_nam);
         TRACE("query2='%s'\n",query);
         PGresult   *sres2= PQexec(rep_cnx,query);
         if (PQresultStatus(sres2) == PGRES_TUPLES_OK)
         {
            int rowsq2 = PQntuples(sres2);
            for(int k=0; k < rowsq2; k++)
            {
               if (k == 0 && j == 0)  printf("Indexes\n"); 
               if (k == 0)            printf("   Database '%s' [%s]:\n",sdb_nam,sdb_oid);               
               strcpy(ndx_sch,  PQgetvalue(sres2, k,0));
               strcpy(ndx_tbl,  PQgetvalue(sres2, k,1));
               strcpy(ndx_nam,  PQgetvalue(sres2, k,2));
               printf("      %s.%s) %s\n",ndx_sch,ndx_tbl,ndx_nam);
            }

         }
         PQclear(sres2);
      }
      free(ndx_sch);
      free(ndx_tbl);
      free(ndx_nam);
      free(sdb_oid);
      free(sdb_nam);
   }
   PQclear(sres);
   free(query);
}

void show_backup_Tablespace(int cluster_id,char *bck_id)
{
   char *dblist[1024];
   /*
select tbs.tbs_nam,tbs.tbs_oid,tbs.tbs_loc,array_agg(dbs.db_nam) dbs
  from backup_dbs dbs,
       backup_tbs tbs, 
       backup_tbl tbl 
 where dbs.cid=tbl.cid         
   and dbs.cid=tbs.cid 
   and dbs.cid=1
   and tbl.tbs_oid=tbs.tbs_oid
   and tbl.tab_dbid=dbs.db_oid
   and dbs.bck_id=tbs.bck_id
   and dbs.bck_id=tbl.bck_id 
   and dbs.bck_id='00016211ff9514b7e0f8' 
   group by tbs.tbs_nam,tbs.tbs_oid,tbs.tbs_loc;

   */
   char *query=malloc(1024);
/*   sprintf(query,"select tbs_nam,tbs_oid,tbs_loc from %s.backup_tbs where cid=%d and bck_id='%s' order by tbs_nam",
                  varGet(GVAR_DEPUSER),
                  cluster_id,
                  bck_id);
*/
   sprintf(query,"select tbs.tbs_nam,tbs.tbs_oid,tbs.tbs_loc,array_agg(dbs.db_nam) dbs"
                 " from %s.backup_dbs dbs,%s.backup_tbs tbs,%s.backup_tbl tbl "
                 "where dbs.cid=tbl.cid and dbs.cid=tbs.cid and dbs.cid=%d"
                 "  and tbl.tbs_oid=tbs.tbs_oid and tbl.tab_dbid=dbs.db_oid and dbs.bck_id=tbs.bck_id"
                 "  and dbs.bck_id=tbl.bck_id  and dbs.bck_id='%s' "
                 "group by tbs.tbs_nam,tbs.tbs_oid,tbs.tbs_loc",
                 varGet(GVAR_DEPUSER),
                 varGet(GVAR_DEPUSER),
                 varGet(GVAR_DEPUSER),
                 cluster_id,
                 bck_id);
   PGresult   *sres= PQexec(rep_cnx,query);
   if (PQresultStatus(sres) == PGRES_TUPLES_OK)
   {
      int rowsq = PQntuples(sres);
      for(int j=0; j < rowsq; j++)
      {
         if (j == 0) printf("Tablespace directories\n"); 
         sprintf((char *)dblist,"%s",PQgetvalue(sres, j,3));
         if (strcmp((char *)dblist,"{}") == 0) strcpy((char *)dblist,"[ not used ]");
         char *bracket=strchr((char *)dblist,'{');if (bracket != NULL) bracket[0]='[';
               bracket=strchr((char *)dblist,'}');if (bracket != NULL) bracket[0]=']';
         printf("   %s (oid %s) '%s' %s\n",PQgetvalue(sres, j,0),PQgetvalue(sres, j,1),PQgetvalue(sres, j,2),(char *)dblist);
      }
   }
   PQclear(sres);
   free(query);
}

void show_backup_pieces(int cluster_id,char *bck_id)
{
   char *query=malloc(1024);
   sprintf(query,"select p.pcid,p.pname,p.pcount,p.psize from %s.backup_pieces p where p.cid=%d and p.bck_id='%s' order by pcid",
                  varGet(GVAR_DEPUSER),
                  cluster_id,
                  bck_id);
   PGresult   *sres= PQexec(rep_cnx,query);
   if (PQresultStatus(sres) == PGRES_TUPLES_OK)
   {
      int rowsq = PQntuples(sres);
      for(int j=0; j < rowsq; j++)
      {
         if (j == 0) printf("Backup Pieces\n");
         printf ("   %3s) %-40s %s files [%s byte(s)]\n",PQgetvalue(sres, j,0),PQgetvalue(sres, j,1),PQgetvalue(sres, j,2),PQgetvalue(sres, j,3));
      };
   }
   PQclear(sres);   
   free(query);
}

int show_backup_header(const char *cluster_name,int cluster_id,char *bck_id,int summary)
{
   int piece_count=0;
   char *query=malloc(1024); 
#define FLD_BCKTYP 0
#define FLD_BCKID  1
#define FLD_BCKSTS 2
#define FLD_BCKTAG 3
#define FLD_HSTNAM 4
#define FLD_PGDATA 5
#define FLD_VERSION 6
#define FLD_TIMELINE 7
#define FLD_OPTIONS 8
#define FLD_BCKSIZE 9
#define FLD_PCOUNT 10
#define FLD_BDATE  11
#define FLD_BWALL  12
#define FLD_BWALF  13
#define FLD_EDATE  14
#define FLD_EWALL  15
#define FLD_EWALF  16
#define FLD_TZ     17
#define FLD_SDATE  18
#define FLD_BCKDIR 19
#define FLD_ELAPSE 20

   sprintf(query,"select bcktyp,bck_id,bcksts,bcktag,hstname,"
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
                  bck_id);
   int rows=DEPOquery(query,0);
   if (rows == 1)
   {
      piece_count=DEPOgetInt(0,FLD_PCOUNT);
      char bcksts[12];
      char opts[80];
      switch (DEPOgetInt(0,FLD_BCKSTS))
      {
         case RECM_BACKUP_STATE_AVAILABLE  : strcpy(bcksts,RECM_BACKUP_STATE_AVAILABLE_S );break;
         case RECM_BACKUP_STATE_OBSOLETE   : strcpy(bcksts,RECM_BACKUP_STATE_OBSOLETE_S  );break;
         case RECM_BACKUP_STATE_INCOMPLETE : strcpy(bcksts,RECM_BACKUP_STATE_INCOMPLETE_S);break;
         case RECM_BACKUP_STATE_RUNNING    : strcpy(bcksts,RECM_BACKUP_STATE_RUNNING_S   );break;
         case RECM_BACKUP_STATE_FAILED     : strcpy(bcksts,RECM_BACKUP_STATE_FAILED_S    );break;
      };   
      printf("Backup '%s'   UID:'%s' [%s]      Cluster:'%s' (CID=%d)\n",DEPOgetString(0,FLD_BCKTYP),bck_id,bcksts,cluster_name,cluster_id);
      if (summary == false)
      {
         long bck_options=DEPOgetLong(0, FLD_OPTIONS);  
         
         int nopt=0;
         strcpy(opts,"Options.: ");
         if ((bck_options & BACKUP_OPTION_COMPRESSED) == BACKUP_OPTION_COMPRESSED)   { nopt++; strcat(opts,"compressed(C)"); };
         if ((bck_options & BACKUP_OPTION_NOINDEX)   == BACKUP_OPTION_NOINDEX)       { if (nopt > 0) strcat(opts,","); nopt++; strcat(opts," no_indexes(I)"); };
         if ((bck_options & BACKUP_OPTION_EXCLUSIVE) == BACKUP_OPTION_EXCLUSIVE)     { if (nopt > 0) strcat(opts,","); nopt++; strcat(opts," exclusive(E)"); };
         if ((bck_options & BACKUP_OPTION_KEEPFOREVER) == BACKUP_OPTION_KEEPFOREVER) { if (nopt > 0) strcat(opts,","); nopt++; strcat(opts," locked(L)"); };
         long f_bksize=DEPOgetLong(0, FLD_BCKSIZE);  
         long f_elapsed=DEPOgetLong(0,FLD_ELAPSE);
         
         printf("   TimeLine(TL).: %-15s     Tag.........: '%s'\n",     DEPOgetString(0,FLD_TIMELINE),DEPOgetString(0,FLD_BCKTAG));
         printf("   DateStyle(DS): %-15s     TimeZone(TZ): '%s'\n",     DEPOgetString(0,FLD_SDATE),   DEPOgetString(0,FLD_TZ));
         if (strcmp(DEPOgetString(0,FLD_BCKTYP),RECM_BCKTYP_FULL) == 0)
         {
            printf("   Started: %-25s WAL: %s (%s)  Elapsed Time: %s\n",   DEPOgetString(0,FLD_BDATE),   DEPOgetString(0,FLD_BWALL),DEPOgetString(0,FLD_BWALF),DisplayElapsed(f_elapsed));
            printf("   Ended..: %-25s WAL: %s (%s)\n",                     DEPOgetString(0,FLD_EDATE),   DEPOgetString(0,FLD_EWALL),DEPOgetString(0,FLD_EWALF));
         }
         
         if (strcmp(DEPOgetString(0,FLD_BCKTYP),RECM_BCKTYP_WAL) == 0)
         {
            printf("   Started: %-25s Elapsed Time: %s\n",                DEPOgetString(0,FLD_BDATE),DisplayElapsed(f_elapsed));
            printf("   Ended..: %-25s\n",                                  DEPOgetString(0,FLD_EDATE));
         };
         if (strcmp(DEPOgetString(0,FLD_BCKTYP),RECM_BCKTYP_CFG) == 0)
         {
            printf("   Started: %-25s Elapsed Time:%s\n",            DEPOgetString(0,FLD_BDATE),DisplayElapsed(f_elapsed));
            printf("   Ended..: %-25s\n",                            DEPOgetString(0,FLD_EDATE));
         };  
         if (strcmp(DEPOgetString(0,FLD_BCKTYP),RECM_BCKTYP_META) == 0)
         {
            printf("   Started: %-25s Elapsed Time:%s\n",           DEPOgetString(0,FLD_BDATE),DisplayElapsed(f_elapsed));
            printf("   Ended..: %-25s\n",                             DEPOgetString(0,FLD_EDATE));
         };  
         printf("   Location: '%s'\n",DEPOgetString(0,FLD_BCKDIR));
         printf("   Size....: %ld Bytes (%ld MB) in %d piece(s)\n",f_bksize,(f_bksize/1024/1024), piece_count);
         if (nopt > 0) { printf("   %s\n",opts); };
      };
   }
   else
   {
      printf("No backup '%s' found.\n",bck_id);
   }
   DEPOqueryEnd();
   if (summary == false) printf("\n");
   return(piece_count);
}


void show_backup_piece_content(int cluster_id,char *bck_id,int piece_number)
{

   char *query=malloc(1024);
   char *sf_id =malloc(30); 
   char *sf_nam=malloc(1024);
   char *sf_psiz=malloc(30);
   char *sf_csiz=malloc(30);
   char *sf_cnt=malloc(30);
   if (piece_number == -1)
   {
      sprintf(query,"select p.pcid,p.pname,p.psize,p.csize,p.pcount,b.bckdir from %s.backup_pieces p,%s.backups b "
                     "where p.cid=%d and p.bck_id='%s'"
                     "  and b.bck_id=p.bck_id and b.cid=p.cid "
                     "order by pcid",
                     varGet(GVAR_DEPUSER),
                     varGet(GVAR_DEPUSER),
                     cluster_id,
                     bck_id);
   }
   else
   {
      sprintf(query,"select p.pcid,p.pname,p.psize,p.csize,p.pcount,b.bckdir from %s.backup_pieces p,%s.backups b "
                    "where p.pcid=%d"
                    "  and p.cid=%d and p.bck_id='%s'"
                    "  and b.bck_id=p.bck_id and b.cid=p.cid "
                    "order by pcid",
                    varGet(GVAR_DEPUSER),
                    varGet(GVAR_DEPUSER),
                    piece_number,
                    cluster_id,
                    bck_id);
   }
   TRACE("query:%s\n",query);
   PGresult   *sres= PQexec(rep_cnx,query);
   
   if (PQresultStatus(sres) == PGRES_TUPLES_OK)
   {
      int rowsq = PQntuples(sres);
      for (int numrow=0;numrow < rowsq;numrow++)
      {
         strcpy(sf_id,   PQgetvalue(sres, numrow,0));
         sprintf(sf_nam,"%s/%s",PQgetvalue(sres, numrow,5),PQgetvalue(sres, numrow,1));
         strcpy(sf_psiz, PQgetvalue(sres, numrow,2));
         strcpy(sf_csiz, PQgetvalue(sres, numrow,3));
         strcpy(sf_cnt,  PQgetvalue(sres, numrow,4));
         RecmFile *rf=RECMGetProperties(sf_nam);
         if (rf == NULL)
         {
            ERROR(ERR_INVRECMFIL,"Corrupted or invalid RECM file '%s'\n",sf_nam);
         }
         else
         {
            printf("Piece '%s' Content:\n",sf_nam);
            RECMDataFile *hDF=RECMopenRead(sf_nam);
            if (hDF != NULL)
            {   
               VERBOSE("Piece '%s' content\n",sf_nam); 
               RECMlistContent(hDF);
               RECMcloseRead(hDF);
            }
         }
      }
   }
   PQclear(sres);   
   free(sf_cnt);
   free(sf_csiz);
   free(sf_psiz);
   free(sf_nam);
   free(sf_id);
   free(query);   
}

// show backup /uid=xxx /source="clustername" /cid=CCCC
void COMMAND_SHOWBACKUP(int idcmd,char *command_line)
{
   if (DEPOisConnected(true) == false) return;
   if ((CLUisConnected(false) == false) && qualifierIsUNSET("qal_source") == true &&qualifierIsUNSET("qal_cid") == true)
   {
      ERROR(ERR_NOTCONNECTED,"Not connected to any cluster.\n");
      return;
   };

   if (optionIsSET("opt_verbose") == true) globalArgs.verbosity=true;                                                              // Set Verbosity

   memBeginModule();
   
   int cluster_id;
   char *cluster_name=memAlloc(128); 
   if (CLUisConnected(false) == true)
   {
      if (strcmp(varGet(GVAR_CLUCID),VAR_UNSET_VALUE) != 0) cluster_id=varGetInt(GVAR_CLUCID);
      if (strcmp(varGet(GVAR_CLUNAME),VAR_UNSET_VALUE) != 0) strcpy(cluster_name,varGet(GVAR_CLUNAME));
   }
   else
   {
      cluster_id=-1;
      strcpy(cluster_name,"(null)");
   };
   
//   char *backup_location=memAlloc(1024);
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
   char *bck_id=memAlloc(40);
   strcpy(bck_id,varGet("qal_uid"));
   if (strlen(bck_id) < 20)
   {
      char *full_bck_id=autocompleteUID(cluster_id,bck_id);
      if (full_bck_id != NULL) strcpy(bck_id,full_bck_id);
   };
   
   /*
show backup/uid=xxx /table /index /directories /files              
show backup/uid=xxx /table /index /directories /piece=1 /pieces              
   */
   int subopts=0;
   if (optionIsSET("opt_databases") == true)   subopts++;
   if (optionIsSET("opt_tables") == true)      subopts++;
   if (optionIsSET("opt_indexes") == true)     subopts++;
   if (optionIsSET("opt_directories") == true) subopts++;
   if (optionIsSET("opt_files") == true)       subopts++;
   if (optionIsSET("qal_piece") == true)       subopts++;
   
   int piece_count=show_backup_header(cluster_name,cluster_id,bck_id,(subopts != 0) );
   
   if (optionIsSET("opt_databases") == true)                                        // show backup/uid=XXXX /table
   {
      show_backup_Databases(cluster_id,bck_id);
   }

   if (optionIsSET("opt_tables") == true)                                        // show backup/uid=XXXX /table
   {
      show_backup_Tables(cluster_id,bck_id);
   }

   if (optionIsSET("opt_indexes") == true)                                        // show backup/uid=XXXX /index
   {
      show_backup_Indexes(cluster_id,bck_id);
   }
   if (optionIsSET("opt_directories") == true)                                   // show backup/uid=XXXX /tablespace
   {
      show_backup_Tablespace(cluster_id,bck_id);
   }
   if (optionIsSET("opt_files") == true)                                        // show backup/uid=XXXX /files
   {
      show_backup_pieces(cluster_id,bck_id);
   }
   if (qualifierIsUNSET("qal_piece") == false)
   {
      int piece_number=varGetInt("qal_piece");
      if (strcmp(varGet("qal_piece"),"all") == 0) { piece_number=-1; }
      if (strcmp(varGet("qal_piece"),"ALL") == 0) { piece_number=-1; }
      if (piece_number == 0 || piece_number > piece_count)
      {
         ERROR(ERR_BADPIECE,"Bad piece number '%d'. Accepted value is [1..%d]\n",piece_number,piece_count);
         return;
      }
      show_backup_piece_content(cluster_id,bck_id,piece_number);
   }
   qualifierUNSET("qal_piece");
   qualifierUNSET("qal_uid");
   UNSETOption("opt_files");
   UNSETOption("opt_directories");
   UNSETOption("opt_indexes");
   UNSETOption("opt_tables");
   
}


/*

// list backup/source="clustername"
void COMMAND_SHOWBACKUPxx(int idcmd,char *command_line)
{
   if (DEPOisConnected(true) == false) return;

   if ((CLUisConnected(false) == false) && qualifierIsUNSET("qal_source") == true &&qualifierIsUNSET("qal_cid") == true)
   {
      ERROR(ERR_NOTCONNECTED,"Not connected to any cluster.\n");
      return;
   };

   if (optionIsSET("opt_verbose") == true) globalArgs.verbosity=true;                                                              // Set Verbosity

   memBeginModule();
   
   int cluster_id;
   char *cluster_name=memAlloc(128); 
   if (CLUisConnected(false) == true)
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
      bck_options=DEPOgetLong(n, 9); TRACE("bck_options      : '%ld'\n",DEPOgetLong(n, 9));
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
   memEndModule();
}
*/