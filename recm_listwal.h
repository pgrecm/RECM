/**********************************************************************************/
/* list wal command                                                               */
/* Usage:                                                                         */
/*      list wal                                                                  */
/*      options:                                                                  */
/*            /available      list available WAL only                             */
/*            /incomplete     list incomplete WAL only                            */
/*            /obsolete       list oboslete WAL only                              */
/*      Qualifiers:                                                               */
/*            /before=<DATE>  list WALs before a date                             */
/*            /after=<DATE>   list WALs after a date                              */
/*            /cid=<CID>      list WALs of an other cluster                       */
/*            /uid=<UID>      list WALs of a specific backup                      */
/**********************************************************************************/
void COMMAND_LISTWAL(int idcmd,char *command_line)
{
   if (DEPOisConnected(true) == false) return;

   if ((CLUisConnected(false) == false) && 
       qualifierIsUNSET("qal_source") == true && 
       qualifierIsUNSET("qal_cid") == true)
   {
      ERROR(ERR_NOTCONNECTED,"Not connected to any cluster.\n");
      return;
   };

   memBeginModule();

   if (optionIsSET("opt_verbose") == true) globalArgs.verbosity=true;                                                              // Set Verbosity
   
   long cluster_id;
   char *cluster_name=memAlloc(128); 
   
   if (CLUisConnected(false) == true)
   {
      if (strcmp(varGet(GVAR_CLUCID),VAR_UNSET_VALUE) != 0) cluster_id=varGetLong(GVAR_CLUCID);
      if (strcmp(varGet(GVAR_CLUNAME),VAR_UNSET_VALUE) != 0) strcpy(cluster_name,varGet(GVAR_CLUNAME));
   }
   else
   {
      cluster_id=-1;
      strcpy(cluster_name,"(null)");
   };
   
   char *query=memAlloc(1024);

   if (qualifierIsUNSET("qal_source") == false)
   {
      sprintf(query,"select count(*) from %s.clusters where clu_nam='%s'",
                    varGet(GVAR_DEPUSER),
                    varGet("qal_source"));
      int rows=DEPOquery(query,0);
      if (DEPOgetInt(0,0) == 0)
      {
         ERROR(ERR_BADCLUNAME,"Unknown cluster source name '%s'•\n",varGet("qal_source"));
         DEPOqueryEnd();
         memEndModule();
         return;
      }
      DEPOqueryEnd();
      sprintf(query,"select CID from %s.clusters where clu_nam='%s'",
                       varGet(GVAR_DEPUSER),
                       varGet("qal_source"));
      rows=DEPOquery(query,0);
      cluster_id=DEPOgetLong(0,0);
      DEPOqueryEnd();
      strcpy(cluster_name,varGet("qal_source"));
      varAdd("qal_cid",QAL_UNSET);                                              // Take care to not try to use /CID after /SOURCE
   }
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
      cluster_id=varGetLong("qal_cid");
      DEPOqueryEnd();
   };
   if (IsClusterEnabled(cluster_id) == false)
   {
      WARN(WRN_CLUDISABLED,"Cluster '%s' is 'DISABLED'.\n",cluster_name);
   }
      
   char *qry_before=memAlloc(128); qry_before[0]=0x00; 
   char *qry_after=memAlloc(128);  qry_after[0]=0x00; 
   char *qry_id=memAlloc(128);     qry_id[0]=0x00; 
   char *qry_sts=memAlloc(128);    qry_sts[0]=0x00;

   if (optionIsSET("opt_available")  == true) { strcpy(qry_sts," and b.bcksts=0"); };
   if (optionIsSET("opt_obsolete")   == true) { strcpy(qry_sts," and b.bcksts=1"); };
   if (optionIsSET("opt_incomplete") == true) { strcpy(qry_sts," and b.bcksts=2"); };

   if (qualifierIsUNSET("qal_before") == false)
   { sprintf(qry_before," and w.edate < to_timestamp('%s','%s')",varGet("qal_before"),varGet(GVAR_CLUDATE_FORMAT)); };

   if (qualifierIsUNSET("qal_after") == false)
   { sprintf(qry_after," and w.edate > to_timestamp('%s','%s')",varGet("qal_after"),varGet(GVAR_CLUDATE_FORMAT)); };

   if (qualifierIsUNSET("qal_uid") == false)
   { sprintf(qry_id," and b.bck_id='%s'",varGet("qal_uid")); }; 

   sprintf(query,"select w.bck_id,case b.bcksts when 0 then 'AVAILABLE'"
                 "                              when 1 then 'OBSOLETE'"
                 "                              when 2 then 'INCOMPLETE'"
                 "                              when 3 then 'RUNNING'"
                 "                              when 4 then 'FAILED' else '?'||b.bcksts end,"
                 "        w.walfile,to_char(w.edate,'%s')"
                 " from %s.backup_wals w,%s.backups b where w.cid=b.cid and w.cid=%ld and w.bck_id=b.bck_id "
                 " and w.walfile not like '%%.backup' "
                 " and w.walfile not like '%%.history'"
                 " and w.walfile not like '%%.partial'",
                    varGet(GVAR_CLUDATE_FORMAT),
                    varGet(GVAR_DEPUSER),
                    varGet(GVAR_DEPUSER),
                    cluster_id);
   strcat(query,qry_before);
   strcat(query,qry_after);
   strcat(query,qry_id);
   strcat(query,qry_sts);
   strcat(query," order by w.walfile");
   
   printf("List WALs of cluster '%s' (CID=%ld)\n",cluster_name,cluster_id);
   TRACE ("query:=%s\n",query);
   int wal_rows=DEPOquery(query,1);
   char *prev_bckid=memAlloc(30);
   char *bckid=memAlloc(30);
   prev_bckid[0]=0x00;
   bckid[0]=0x00;
   
   char *curWAL=memAlloc(64);curWAL[0]=0x00;
   char *prvWAL=memAlloc(64);prvWAL[0]=0x00;

   for (int i=0;i < wal_rows;i++)
   {
      strcpy(prev_bckid,bckid);
      strcpy(bckid,DEPOgetString(i,0));
      if (strstr(curWAL,".backup") == NULL && strstr(curWAL,".history") == NULL && strstr(curWAL,".partial") == NULL) strcpy(prvWAL,curWAL);
      strcpy(curWAL,DEPOgetString(i,2));
       char *extension=strchr(curWAL,'.');
      if (extension != NULL) { extension[0]=0x00;};                             // Take care if WAL file is compressed...we remove any extension
      TRACE("bckid='%s' curWAL='%s'\n",bckid,curWAL);
      long cTL=getWALtimeline(prvWAL);
      long nTL=getWALtimeline(curWAL);
      if (i == 0)
      {
         printf("%-22s %-10s %-5s %-25s %-30s\n","Bck ID","Status","TL","WAL","Date");
         printf("%-22s %-10s %-5s %-25s %-30s\n","----------------------","----------","-----","-------------------------","------------------------------");
      };
      if (i > 0 && strcmp(curWAL,ComputeNextWAL(prvWAL)) != 0 && strcmp(prvWAL,curWAL) != 0)
      {
         if (nTL != cTL)
         {
            printf("%22s %-10s %5ld %-25s <--- - - - - - - -  ## TL CHANGE ##\n"," . . . . . ... ","",nTL,curWAL);
         }
         else
         {
            printf("%22s %-10s %s ? %s ## GAP ##\n"," . . . . . ... ","",prvWAL,curWAL);
         }
      };
      if (strcmp(bckid,prev_bckid) == 0)
      {
         printf("%-22s %-10s %5ld %-25s %-30s\n"," + - - - - -------->",DEPOgetString(i,1),nTL,DEPOgetString(i,2),DEPOgetString(i,3));
      }
      else
      {
         printf("%-22s %-10s %5ld %-25s %-30s\n",DEPOgetString(i,0),DEPOgetString(i,1),nTL,DEPOgetString(i,2),DEPOgetString(i,3));
      }
   }
   DEPOqueryEnd();
   
   // Now, display all WAL files not yet backuped.
   char *fullname=memAlloc(1024);
   struct stat st;
   int not_backuped=1;
   while (not_backuped == 1)
   {
      prvWAL=ComputeNextWAL(curWAL);
      strcpy(curWAL,prvWAL);
      sprintf(fullname,"%s/%s",varGet(GVAR_WALDIR),curWAL);
      int rc=stat (fullname, &st);
      char *extension=strchr(curWAL,'.');
      if (rc != 0 && extension == NULL)
      {
         char* RECM_WALCOMP_EXT=getenv (ENVIRONMENT_RECM_WALCOMP_EXT);
         if (RECM_WALCOMP_EXT == NULL) { strcat(fullname,".gz"); }
                                  else { strcat(fullname,RECM_WALCOMP_EXT); };
         rc=stat (fullname, &st);
      };
      if (rc == 0) 
      { 
         long nTL=getWALtimeline(curWAL);
         char wrkstr[35];
         strftime((char *)&wrkstr, 35, "%Y-%m-%d %H:%M:%S", localtime(&st.st_mtime));      
         printf("%-22s %-10s %5ld %-25s %-30s\n","*** NOT BACKUPED ***","ON DISK",nTL,curWAL,(char *)&wrkstr);
      }
      else
      {
         not_backuped++;
      }
   }

   memEndModule();
   return;
};

   