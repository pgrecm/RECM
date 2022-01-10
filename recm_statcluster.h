/**********************************************************************************/
/* STATISTICS CLUSTER command                                                     */
/* Usage:                                                                         */
/*    statistics cluster                                                          */                 
/**********************************************************************************/
void COMMAND_STATCLUSTER(int idcmd,char *command_line)
{
   if (CLUisConnected() == false)
   {
      ERROR(ERR_NOTCONNECTED,"Not connected to any cluster.\n");
      return;
   };
   if (DEPOisConnected() == false)
   {
      ERROR(ERR_NOTCONNECTED,"Not connected to any deposit.\n");
      return;
   };
   if (varGetLong(GVAR_CLUCID) == 0)
   {
     ERROR(ERR_NOTREGISTERED,"Not registered to any deposit.\n");
     return;
   };

   memBeginModule();
   
   long cluster_id;
   char *cluster_name=memAlloc(128); 
   char *query=memAlloc(1024);  

   if (strcmp(varGet(GVAR_CLUCID),VAR_UNSET_VALUE) != 0)  cluster_id=varGetLong(GVAR_CLUCID);
   if (strcmp(varGet(GVAR_CLUNAME),VAR_UNSET_VALUE) != 0) strcpy(cluster_name,varGet(GVAR_CLUNAME));
   
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
   else
   {
      if (CLUisConnected() == false) 
      {
         memEndModule();
         return;
      };
      strcpy(cluster_name,varGet(GVAR_CLUNAME));
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
   


   sprintf(query,"select cid,clu_nam,clu_sts,clu_info from %s.clusters where cid=%ld",
                 varGet(GVAR_DEPUSER),
                 cluster_id);
   int rows=DEPOquery(query,0);
   if (rows > 0)
   {
      printf("Cluster Statistics\n");
      printf("==================\n");
      printf("   Cluster Name : '%s' [%s](CID=%d)\n",DEPOgetString(0,1),varGet(GVAR_CLUIP),DEPOgetInt(0,0));
      printf("    Description : '%s'\n",DEPOgetString(0,3));
      printf("         PGDATA : '%s'\n",varGet(GVAR_CLUPGDATA));
      printf("         Status : '%s'\n",DEPOgetString(0,2));
   };
   DEPOqueryEnd();

   printf("\n");   
   printf("Retention\n");
   printf("   +-%20s-+-%6s-+-%10s-+\n", "--------------------","------","----------");
   printf("   | %-20s | %6s | %10s |\n", "Type of backup","Mode","Value");
   printf("   +-%20s-+-%6s-+-%10s-+\n", "--------------------","------","----------");
   printf("   | %-20s | %6s | %10d |\n", "FULL and WAL", retentionModeString(varGet(GVAR_CLURETENTION_FULL)),  retentionCount(varGet(GVAR_CLURETENTION_FULL))); 
   printf("   | %-20s | %6s | %10d |\n", "CONFIG",       retentionModeString(varGet(GVAR_CLURETENTION_CONFIG)),retentionCount(varGet(GVAR_CLURETENTION_CONFIG)));
   printf("   | %-20s | %6s | %10d |\n", "META",         retentionModeString(varGet(GVAR_CLURETENTION_META)),  retentionCount(varGet(GVAR_CLURETENTION_META)));
   printf("   +-%20s-+-%6s-+-%10s-+\n", "--------------------","------","----------");
   sprintf(query,"select bcktyp,count(*) as count,sum(bcksize /1024/1024) as size_MB,"
                 "       max(coalesce(to_char(DATE_PART('second',edate-bdate),'99999999990'),to_char(0,'99999999990')))"       
                 " from %s.backups"
                 " where cid=%ld and bcksts=0 group by 1 order by 1",
                 varGet(GVAR_DEPUSER),
                 cluster_id);
   TRACE("q=%s\n",query);
   rows=DEPOquery(query,0);
   if (rows > 0)
   {
      int row=0;
      while (row < rows)
      {
          if (row == 0)
          {
             printf ("\nStatistics backups\n");
             printf ("   +%-20s+%10s-+%10s-+%10s-+%15s--%15s-+\n", "--------------------","----------","----------","----------","---------------","---------------");
             printf ("   |%-20s|%10s |%10s |%10s |          %10s             |\n", "",    "Backup",   "Size","Elapsed", "Rate/Sec");
             printf ("   |%-20s|%10s |%10s |%10s |%15s |%15s |\n", "Type of Backup","Count","(MB)",   "(Sec)",    "Bytes","Human View");
             printf ("   +%-20s+%10s-+%10s-+%10s-+%15s-+%15s-+\n", "--------------------","----------","----------","----------","---------------","---------------");
             
          };
          long bckcnt=DEPOgetLong(row,1);
          long bcksiz=DEPOgetLong(row,2);
          long maxtim=DEPOgetLong(row,3);
          long siz_sec=0;
          if (maxtim > 0 && bckcnt > 0) siz_sec=(bcksiz*1024*1024/maxtim)/bckcnt;
          printf ("   |%-20s|%10ld |%10ld |%10ld |%15ld |%15s |\n", DEPOgetString(row,0), bckcnt, bcksiz,maxtim,siz_sec,DisplayprettySize(siz_sec));
          row++;
      };
      printf ("   +%-20s+%10s-+%10s-+%10s-+%15s-+%15s-+\n", "--------------------","----------","----------","----------","---------------","---------------");
   };
   DEPOqueryEnd();

   sprintf(query,"select bckdir,sum(bcksize) from %s.backups where cid=%ld and bcksts < 2 group by 1 order by 1",       
                 varGet(GVAR_DEPUSER),
                 cluster_id);
   TRACE("query=%s\n",query);
   rows=DEPOquery(query,0);
   if (rows > 0)
   {
      int row=0;
      while (row < rows)
      {
          if (row == 0)
          {
             printf ("\nBackup location statistics\n");
             printf ("   +%-20s+%15s-+\n", "--------------------","---------------");
             printf ("   |%-20s|%15s |\n", "Backup Location",   "Used space");
             printf ("   +%-20s+%15s-+\n", "--------------------","---------------");
          };
          long bcksiz=DEPOgetLong(row,1);
          printf ("   |%-20s|%15s |\n", DEPOgetString(row,0),DisplayprettySize(bcksiz));
          row++;
      };
      printf ("   +%-20s+%15s-+\n", "--------------------","---------------");
   };
   DEPOqueryEnd();
   
   
   sprintf(query,"select to_char(min(bdate),'%s'),to_char(max(bdate),'%s') from %s.backups where cid=%ld and bcksts=0 and bcktyp='FULL'",
                 varGet(GVAR_CLUDATE_FORMAT),
                 varGet(GVAR_CLUDATE_FORMAT),
                 varGet(GVAR_DEPUSER),
                 cluster_id);
   rows=DEPOquery(query,0);
   if (rows > 0)
   {
      printf("   Oldest FULL backup : '%s'\n",DEPOgetString(0,0));
      printf("     Last FULL backup : '%s'\n",DEPOgetString(0,1));
   }
   DEPOqueryEnd();
   
   memEndModule();
}
