/*
@command cancel backup
@definition
Cancel a backup. If your process failed, sometimes, the status of the last running backup may be not up-to-date.

@option "/verbose" "Display more details"

@example
@inrecm list backup/full 
@out List backups of cluster 'CLU12' (CID=1)
@out UID                    Date                 Type   Status       TL          Size  Tag
@out ---------------------- -------------------- ------ ------------ ----   ---------- -------------------------
@out 0001631cc99828946998   2022-09-10 19:30:01  FULL   OBSOLETE     36         796 MB FULL_daily
@out ...
@out 0001633fb1b114d2a870   2022-10-07 06:57:21  FULL   RUNNING      36        0 Bytes 20221007_CLU12_FULL
@inrecm cancel backup /verbose 
@out recm-inf: NO backup is currently running
@inrecm list backup 
@out List backups of cluster 'CLU12' (CID=1)
@out UID                    Date                 Type   Status       TL          Size  Tag
@out ---------------------- -------------------- ------ ------------ ----   ---------- -------------------------
@out 0001631cc99828946998   2022-09-10 19:30:01  FULL   OBSOLETE     36         796 MB FULL_daily
@out ...
@out 0001633fb1b114d2a870   2022-10-07 06:57:21  FULL   FAILED       36        0 Bytes 20221007_CLU12_FULL
@inrecm
@end
*/

void COMMAND_CANCELBACKUP(int idcmd,char *command_line)
{
   if (isConnectedAndRegistered() == false) return;

   if (optionIsSET("opt_verbose") == true) globalArgs.verbosity=true;                                                              // Set Verbosity
   
   memBeginModule();

   char *query=memAlloc(1024);
   sprintf(query,"update %s.backups set bcksts=%d where cid=%s and bcksts=%d",
                 varGet(GVAR_DEPUSER),
                 RECM_BACKUP_STATE_FAILED,
                 varGet(GVAR_CLUCID),
                 RECM_BACKUP_STATE_RUNNING);
   int rc=DEPOquery(query,0);
   DEPOqueryEnd();
   
   rc=CLUquery("SELECT pg_is_in_backup()",0);
   if (rc != 1)
   {
      ERROR(ERR_ENDBCKFAIL,"Could not initiate stop backup : %s",CLUgetErrorMessage());
      CLUqueryEnd();
      memEndModule();
      return;
   };
   if (strcmp(CLUgetString(0,0),"f") == 0)
   {
      INFO("NO backup is currently running\n");
      CLUqueryEnd();
      memEndModule();
      return;
   }
   CLUqueryEnd();

   rc=CLUquery("SELECT pg_backup_start_time(),now()-pg_backup_start_time()",0);

   VERBOSE("Backup is running since '%s' (%s).\n",CLUgetString(0,0),CLUgetString(0,1));
   CLUqueryEnd();

   rc=CLUquery("SELECT pg_stop_backup()",0);
   if (rc != 1)
   {
      ERROR(ERR_ENDBCKFAIL,"Could not initiate stop backup : %s",CLUgetErrorMessage());
      CLUqueryEnd();
      memEndModule();
      return;
   };
   CLUqueryEnd();
   INFO("Backup canceled.\n");
   memEndModule();
   return;
}
