/**********************************************************************************/
/* DELETE RESTORE POINT command                                                   */
/* Usage:                                                                         */
/*    delete retore point                                                         */                 
/*      Available Option(s):                                                      */
/*         /verbose                                                               */
/*      Available Qualifier(s):                                                   */
/*         /name=<STRING>             name of the restore point to delete         */
/**********************************************************************************/
/*
@command delete restore point
@definition
Remove a restore point definition from the deposit. This does not do anything on the current connected cluster.

@option "/verbose"     "Display more details"
@option "/name=NAME"   "Restore point name to remove (Mandatory)"

@example
@inrecm List restore point of cluster 'CLU12' (CID=1)
@out Name                 Date                 Type       LSN             TL     WALfile
@out -------------------- -------------------- ---------- --------------- ------ ------------------------
@out first_rp             2022-10-13 20:15:40  TEMPORARY  73/3E0000C8     36     00000024000000730000003E (AVAILABLE)
@out
@inrecm delete restore point /name=first_rp /verbose 
@out
@inrecm list restore point 
@out List restore point of cluster 'CLU12' (CID=1)
@inrecm exit

@end
*/

 void COMMAND_DELETERP(int idcmd,char *command_line)
{
   if (isConnectedAndRegistered() == false) return;
   
   if (optionIsSET("opt_verbose") == true) globalArgs.verbosity=true;                                                              // Set Verbosity

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
   return;
};
