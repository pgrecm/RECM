/**********************************************************************************/
/* STATISTICS DEPOSIT command                                                     */
/* Usage:                                                                         */
/*    statistics deposit                                                          */                 
/**********************************************************************************/
void COMMAND_STATDEPOSIT(int idcmd,char *command_line)
{
   if (DEPOisConnected(true) == false) return;

   memBeginModule();
   char *query=memAlloc(1024);

   sprintf(query,"select deponame,version,description,to_char(created,'%s'),to_char(updated,'%s') from %s.config",
                 varGet(GVAR_CLUDATE_FORMAT),
                 varGet(GVAR_CLUDATE_FORMAT),
                 varGet(GVAR_DEPUSER));
   int rows=DEPOquery(query,0);
   if (rows > 0)
   {
      printf("Deposit Statistics\n");
      printf("==================\n");
      printf("     Deposit Name : '%s'\n",DEPOgetString(0,0));
      printf("          Version : '%s'\n",DEPOgetString(0,1));
      printf("      Description : '%s'\n",DEPOgetString(0,2));
      printf("    Creation date : '%s'\n",DEPOgetString(0,3));
      printf("      Last change : '%s'\n",DEPOgetString(0,4));
   };
   DEPOqueryEnd();
 
   sprintf(query,"select b.cid,c.clu_nam||case c.clu_sts when 'ENABLED' then '' else '(D)' end,"
                 "       sum(bcksize),sum(1) as bckcount,to_char(min(bdate),'%s'),to_char(max(bdate),'%s')"
                 "  from %s.clusters c, %s.backups b where c.cid=b.cid "
                 " group by b.cid,b.cid,c.clu_nam||case c.clu_sts when 'ENABLED' then '' else '(D)' end order by 2",
                 varGet(GVAR_CLUDATE_FORMAT),
                 varGet(GVAR_CLUDATE_FORMAT),
                 varGet(GVAR_DEPUSER),
                 varGet(GVAR_DEPUSER));
   TRACE("Query=%s\n",query);
   rows=DEPOquery(query,0);
   if (rows > 0)
   {
      long total=0;
      int row=0;
      while (row < rows)
      {
          if (row == 0)
          {
             printf ("\nStatistics backups\n");
             printf ("   +%10s-+%15s-+%15s-+%15s-+%22s-+%22s-+\n", "----------","---------------","---------------","---------------","----------------------","----------------------");
             printf ("   |%10s |%15s |%15s |%15s |%-22s |%-22s |\n", "cid", "Name","Used space", "Count", " Oldest Backup"," Youngest Backup");
             printf ("   +%10s-+%15s-+%15s-+%15s-+%22s-+%22s-+\n", "----------","---------------","---------------","---------------","----------------------","----------------------");
          };
          total+=DEPOgetLong(row,2);
          printf ("   |%10ld |%-15s |%15s |%15s |%22s |%22s |\n",DEPOgetLong(row,0),
                                                                DEPOgetString(row,1),
                                                                DisplayprettySize(DEPOgetLong(row,2)),
                                                                DEPOgetString(row,3),
                                                                DEPOgetString(row,4),
                                                                DEPOgetString(row,5));
          row++;
      };
      printf ("   +%10s-+%15s-+%15s-+%15s-+%22s-+%22s-+\n", "----------","---------------","---------------","---------------","----------------------","----------------------");
      printf ("   Total space used : %s\n",DisplayprettySize(total));
   };
   DEPOqueryEnd();
   memEndModule();
}
