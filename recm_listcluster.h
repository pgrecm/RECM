/**********************************************************************************/
/* LIST CLUSTER command                                                           */
/* Usage:                                                                         */
/*    list cluster                                                                */                 
/*      Available Option(s):                                                      */
/*         /verbose                                                               */
/**********************************************************************************/
/*
@command list cluster
@definition
Display all registered clusters in current DEPOSIT.
@option "/verbose" "Display more details"

@example
@in recm -t CLU12
@inrecm list cluster 
@out List of registered clusters
@out 
@out CID        State           Name                 IP              Description
@out ---------- --------------- ---------------      -------------------- ------------------------------
@out 1          ENABLED         CLU12                192.168.1.62    (null)
@out CLU12> list cluster /verbose 
@inrecm List of registered clusters
@out 
@out CID        State           Name                 IP                    Full        WAL     Config Description
@out ---------- --------------- -------------------- --------------- ---------- ---------- ---------- ------------------------------
@out 1          ENABLED         CLU12                192.168.1.62             7          6          0 (null)
@inrecm
@end
 
*/
void COMMAND_LISTCLUSTER(int idcmd,char *command_line)
{
   if (DEPOisConnected(true) == false) return;

   if (optionIsSET("opt_verbose") == true) globalArgs.verbosity=true;                                                              // Set Verbosity

   char *query=malloc(1024);

//   if (strcmp(varGet("opt_available"), VAR_UNSET_VALUE) != 0) { strcpy(qry_sts," and b.bcksts=0"); };
  
   sprintf(query,"with clus as (select cid,clu_sts,clu_nam,clu_info,clu_addr from %s.clusters order by clu_nam),"
                 " bcks as(select cid,sum(case bcktyp when 'FULL' then 1 else 0 end) as FULL,"
                                     "sum(case bcktyp when 'WAL'  then 1 else 0 end) as WAL,"
                                     "sum(case bcktyp when 'CFG'  then 1 else 0 end) as CFG from %s.backups where bcksts=0 group by cid)"
                 " select c.cid,c.clu_sts,c.clu_nam,coalesce(s.FULL,0),coalesce(s.WAL,0),coalesce(s.CFG,0),c.clu_info,coalesce(c.clu_addr,'') from clus c"
                 " left join bcks s on (s.cid=c.cid) order by c.clu_nam",                    
                 varGet(GVAR_DEPUSER),
                 varGet(GVAR_DEPUSER));
   printf("List of registered clusters\n\n");
   int wal_rows=DEPOquery(query,1);
   for (int i=0;i < wal_rows;i++)
   {
      if (globalArgs.verbosity == true)
      {
         if (i == 0)
         {
            printf("%-10s %-15s %-20s %-15s %10s %10s %10s %s\n","CID","State","Name","IP","Full","WAL","Config","Description");
            printf("%-10s %-15s %-20s %-15s %10s %10s %10s %s\n","----------","---------------","--------------------","---------------","----------","----------","----------","------------------------------");
         };
         printf("%-10s %-15s %-20s %-15s %10d %10d %10d %s\n",
                DEPOgetString(i,0),
                DEPOgetString(i,1),
                DEPOgetString(i,2),
                DEPOgetString(i,7),
                DEPOgetInt(i,3),
                DEPOgetInt(i,4),
                DEPOgetInt(i,5),
                DEPOgetString(i,6));
      }
      else
      {
         if (i == 0)
         {
            printf("%-10s %-15s %-20s %-15s %s\n","CID","State","Name","IP","Description");
            printf("%-10s %-15s %-20s %-15s %s\n","----------","---------------","---------------","--------------------","------------------------------");
         };
         printf("%-10s %-15s %-20s %-15s %s\n",DEPOgetString(i,0),DEPOgetString(i,1),DEPOgetString(i,2),DEPOgetString(i,7),DEPOgetString(i,6));
      }
   }
   DEPOqueryEnd();
   free(query);
   return;
};

