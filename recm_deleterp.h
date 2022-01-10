/**********************************************************************************/
/* DELETE RESTORE POINT command                                                   */
/* Usage:                                                                         */
/*    delete retore point                                                         */                 
/*      Available Option(s):                                                      */
/*         /verbose                                                               */
/*      Available Qualifier(s):                                                   */
/*         /name=<STRING>             name of the restore point to delete         */
/**********************************************************************************/
void COMMAND_DELETERP(int idcmd,char *command_line)
{
   if (isConnectedAndRegistered() == false) return;
   
   // Change verbosity
   int opt_verbose=optionIsSET("opt_verbose");
   int saved_verbose=globalArgs.verbosity;
   globalArgs.verbosity=opt_verbose;

   if (qualifierIsUNSET("qal_name") == true)
   { 
      ERROR(ERR_MISQUALVAL,"Missing /name=xxx qualifier.\n");
      return;
   };
   if (isValidName(varGet("qal_name")) == false)
   {
      ERROR(ERR_INVCHARINNAME,"Invalid character in name '%s'\n",varGet("qal_name"));
      return;
   }
   memBeginModule();
   char *query=memAlloc(1024);
   sprintf(query,"delete from %s.backup_rp where cid=%s and lower(rp_name)=lower('%s')",
                 varGet(GVAR_DEPUSER),
                 varGet(GVAR_CLUCID),
                 varGet("qal_name"));
   TRACE("DELETE:%s\n",query);
   int row=DEPOquery(query,0);
   if (row != 0)
   {
      ERROR(ERR_DELRPFAILED,"Could not delete restore point '%s'\n",varGet("qal_name"));
      DEPOqueryEnd();
      memEndModule();
      return;
   }
   DEPOqueryEnd();
   memEndModule();
   globalArgs.verbosity=saved_verbose;
   return;
};

   