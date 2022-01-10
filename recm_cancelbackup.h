

void COMMAND_CANCELBACKUP(int idcmd,char *command_line)
{
   if (isConnectedAndRegistered() == false) return;

   int opt_verbose=optionIsSET("opt_verbose");
   int saved_verbose=globalArgs.verbosity;
   globalArgs.verbosity=opt_verbose;
   
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
      globalArgs.verbosity=saved_verbose;
      CLUqueryEnd();
      memEndModule();
      return;
   };
   if (strcmp(CLUgetString(0,0),"f") == 0)
   {
      INFO("NO backup is currently running\n");
      globalArgs.verbosity=saved_verbose;
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
      globalArgs.verbosity=saved_verbose;
      CLUqueryEnd();
      memEndModule();
      return;
   };
   CLUqueryEnd();
   INFO("Backup canceled.\n");
   globalArgs.verbosity=saved_verbose;
   memEndModule();
   return;
}
