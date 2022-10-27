

void COMMAND_UPGRADEDEPOSIT(int idcmd,char *command_line)
{
   if (DEPOisConnected(true) == false) return;
   
   if (optionIsSET("opt_verbose") == true) globalArgs.verbosity=true;                                                              // Set Verbosity

   memBeginModule();
   
   char *query=memAlloc(1024);
   
   sprintf(query,"select deponame,version from %s.config limit 1",
                 varGet(GVAR_DEPUSER));
   int rows=DEPOquery(query,0);
   if (rows <= 0) 
   {
      ERROR(ERR_WRONGDEPOSIT,"Could not retreive deposit version.");
      memEndModule();
      return;
   };
   char *curDepoNam=memAlloc(80);strcpy(curDepoNam,DEPOgetString(0,0));
   char *curVersion=memAlloc(80);strcpy(curVersion,DEPOgetString(0,1));
   DEPOqueryEnd();
   if (strcmp(curVersion,RECM_VERSION) == 0)
   {
      WARN(WRN_BADVERSION,"Deposit already in version '%s'. Nothing to do.\n",RECM_VERSION);
      memEndModule();
      return;
   }
   // Translate versions if necessary
   TRACE("DEPOSIT Name='%s' current Version='%s'\n",curDepoNam,curVersion);
   if (strcmp(curVersion,"1.1") == 0) strcpy(curVersion,VERSION_11);
   if (strcmp(curVersion,"1.2") == 0) strcpy(curVersion,VERSION_12);
   int rc;
   long changes=0;
   if (strcmp(curVersion,VERSION_11) == 0 && strcmp(curVersion,CURRENT_VERSION) != 0)
   {
#include "recm_upgdep_1112.h"
   }
   
   // At the End, should be at current release version
   if (strcmp(curVersion,CURRENT_VERSION) == 0)
   {
      if (changes == 0)
      { 
         INFO("DEPOSIT is already at version '%s'. No need to upgrade it.\n",curVersion); 
      }
      else
      {
         if (optionIsSET("opt_apply") == false)
         { 
            INFO("DEPOSIT can be Upgraded.  invoke 'upgrade deposit/apply'.\n"); 
         }
         else
         { 
            INFO("DEPOSIT Upgrade successful.\n");
         } 
      }
   }
   else
   {
      ERROR(ERR_WRONGDEPOSIT,"Upgrade no successful. Re-create the DEPOSIT is possible. See documentation for this purpose.\n");
   }
   memEndModule();
}
