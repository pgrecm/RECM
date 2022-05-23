/* CONNECT DEPOSIT command                                                        */
/* Usage:                                                                         */
/*    connect deposit                                                             */                 
/*      Available Qualifier(s):                                                   */
/*         /host=<String>             Host                                        */
/*         /port=<PORT>               port                                        */
/*         /usr=<STRING>              User to connect to                          */
/*         /pwd=<STRING>              Password                                    */
/*         /db=<STRING>               Database to connect to                      */
/**********************************************************************************/
void COMMAND_CNXDEPOSIT(int idcmd,char *command_line)
{
   // We have 2 ways to connect to a  deposit.
   // First, 'qal_name' if you give only the name of the deposit, we try to load an existing configuration to connect to
   // or     using full connection string
   
   if (qualifierIsUNSET("qal_name") == false)
   {
      int rc=LoadConfigFile("DEPO",varGet("qal_name"));
      if (rc == ERR_SECNOTFND)
      {
         ERROR(ERR_SECNOTFND,"Deposit name unknown.  Cannot connect to '%s'\n",varGet("qal_name"));
         return;
      }
   }
   else
   {
      varAdd(GVAR_DEPDESCR,varGet("qal_description"));
      varAdd(GVAR_DEPHOST ,varGet("qal_host"));
      varAdd(GVAR_DEPPORT ,varGet("qal_port"));
      varAdd(GVAR_DEPDB   ,varGet("qal_db"));
      varAdd(GVAR_DEPUSER ,varGet("qal_usr"));
      varAdd(GVAR_DEPPWD  ,varGet("qal_pwd"));
   };
   if (DEPOconnect(varGet(GVAR_DEPDB),
                   varGet(GVAR_DEPUSER),
                   varGet(GVAR_DEPPWD),
                   varGet(GVAR_DEPHOST),
                   varGet(GVAR_DEPPORT)) == true)
   {
      int rows;
      rows=DEPOquery("select count(*) from pg_class where relkind='r' and relname='config'",0);
      if (rows == -1)
      {
         ERROR(ERR_NOTDEPOSIT,"Not connected to a DEPOSIT.\n");
         return;
      }
      if (DEPOgetLong(0,0) == 0)
      {
         ERROR(ERR_NOTDEPOSIT,"Not connected to a DEPOSIT.\n");
         DEPOqueryEnd();
         return;
      }
      DEPOqueryEnd();
      rows=DEPOquery("select deponame,version from config",0);
      if (rows == -1)
      {
         ERROR(ERR_NOTDEPOSIT,"Not connected to a DEPOSIT.\n");
         return;
      }
      varAdd(GVAR_DEPNAME,DEPOgetString(0,0));
      if (strcmp(DEPOgetString(0,1),CURRENT_VERSION) != 0)
      {
         ERROR(ERR_BADDEPOVER,"Wrong Deposit version. Have %s, expected '%s'. Invoke Upgrade\n",
                                DEPOgetString(0,1),
                                CURRENT_VERSION);
         return;
      }
      DEPOqueryEnd();

      // Save deposit confiruration to configuration file
      globalArgs.configChanged=1;
      SaveConfigFile("DEPO",varGet(GVAR_DEPNAME),SECTION_DEPOSIT);
      INFO("Connected to deposit '%s'\n",varGet(GVAR_DEPNAME));
      
      // now, check if we are already registred ?
      if (CLUisConnected(false) == true)
      {
         char *query=malloc(1024);
         sprintf(query,"select count(*) from %s.clusters where clu_nam='%s'",
                       varGet(GVAR_DEPUSER),
                       varGet(GVAR_CLUNAME));
         rows=DEPOquery(query,0);
         if (DEPOgetLong(0,0) == 0)
         {
            ERROR(ERR_NOTREGISTERED,"Not registerd into this deposit. Use 'register' command.\n");
            return;
         };
         sprintf(query,"select CID from %s.clusters where clu_nam='%s'",
                       varGet(GVAR_DEPUSER),
                       varGet(GVAR_CLUNAME));
         rows=DEPOquery(query,0);
         varAdd(GVAR_CLUCID,DEPOgetString(0,0));
         varAdd(GVAR_REGDEPO ,varGet("qal_name"));
         globalArgs.configChanged=1;
         SaveConfigFile("CLU",varGet(GVAR_CLUNAME),SECTION_CLUSTER);
         INFO("Cluster '%s' already registered to deposit '%s'\n",varGet(GVAR_CLUNAME),varGet(GVAR_DEPNAME));
      };
   };
}
