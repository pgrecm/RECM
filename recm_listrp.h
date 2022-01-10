/**********************************************************************************/
/* list restore point command                                                     */
/* Usage:                                                                         */
/*      list restore point                                                        */
/*      options:                                                                  */
/*            /nocid          list all restore of ALL registered clusters         */
/*      Qualifiers:                                                               */
/*            /before=<DATE>  list RP before a date                               */
/*            /after=<DATE>   list RP after a date                                */
/*            /cid=<CID>      list RP of an other cluster                         */
/**********************************************************************************/
void COMMAND_LISTRP(int idcmd,char *command_line)
{
   if (DEPOisConnected() == false) 
   {
      ERROR(ERR_NOTCONNECTED,"Not connected to any deposit.\n");
      return;
   }
   if ((CLUisConnected() == false) &&
        qualifierIsUNSET("qal_source") == true && 
        qualifierIsUNSET("qal_cid") == true)
   {
      ERROR(ERR_NOTCONNECTED,"Not connected to any cluster.\n");
      return;
   };
   
   memBeginModule();

   long cluster_id;
   char *cluster_name=memAlloc(128); 
   
   if (CLUisConnected() == true)
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
      //printf("xx CID=%d ,name='%s'/n",cluster_id,cluster_name);
   };
   
   if (IsClusterEnabled(cluster_id) == false)
   {
      WARN(WRN_CLUDISABLED,"Cluster '%s' is 'DISABLED'.\n",cluster_name);
   }
   
   sprintf(query,"with lfull as (select min(bdate) as fulldate from %s.backups where cid=%ld and bcksts=0 and bcktyp='FULL')"
                 "select rp.rp_name,to_char(rp.crdate,'%s'),case rp.rp_typ when 'P' then 'PERMANENT' else 'TEMPORARY' end,lsn,tl,wal,"
                 "       case when rp.crdate < lf.fulldate then '(OBSOLETE)' else '(AVAILABLE)' end,c.clu_nam"
                 " from %s.backup_rp rp , lfull lf, %s.clusters c where c.cid=rp.cid",
                 varGet(GVAR_DEPUSER),
                 cluster_id,
                 varGet(GVAR_CLUDATE_FORMAT),
                 varGet(GVAR_DEPUSER),
                 varGet(GVAR_DEPUSER));
   if (optionIsSET("opt_nocid") == true)
   {
      strcat(query," order by crdate");
      printf("List ALL restore points\n\n"); 
      int rows=DEPOquery(query,0);
      int row=0;
      while (row < rows)
      {
          
          if (row == 0)
          {
             printf("%-20s %-20s %-20s %-10s %-15s %-6s %s\n","Cluster","Name","Date","Type","LSN","TL","WALfile");
             printf("%-20s %-20s %-20s %-10s %-15s %-6s %s\n","--------------------","--------------------","--------------------","----------","---------------","------","------------------------");
          };
          printf("%-20s %-20s %-20s %-10s %-15s %-6s %s %s\n",
                      DEPOgetString(row,7),
                      DEPOgetString(row,0),
                      DEPOgetString(row,1),
                      DEPOgetString(row,2),
                      DEPOgetString(row,3),
                      DEPOgetString(row,4),
                      DEPOgetString(row,5),
                      DEPOgetString(row,6));
         row++;
      };
      DEPOqueryEnd();
   }
   else
   {
      char * where=malloc(50);
      sprintf(where," and rp.cid=%ld order by crdate",cluster_id);
      strcat(query,where);
      free(where);
      printf("List restore point of cluster '%s' (CID=%ld)\n",cluster_name,cluster_id); 
      int rows=DEPOquery(query,0);
      int row=0;
      while (row < rows)
      {
          
          if (row == 0)
          {
             printf("%-20s %-20s %-10s %-15s %-6s %s\n","Name","Date","Type","LSN","TL","WALfile");
             printf("%-20s %-20s %-10s %-15s %-6s %s\n","--------------------","--------------------","----------","---------------","------","------------------------");
          };
          printf("%-20s %-20s %-10s %-15s %-6s %s %s\n",
                      DEPOgetString(row,0),
                      DEPOgetString(row,1),
                      DEPOgetString(row,2),
                      DEPOgetString(row,3),
                      DEPOgetString(row,4),
                      DEPOgetString(row,5),
                      DEPOgetString(row,6));
         row++;
      };
      DEPOqueryEnd();
   }
   memEndModule();
}
