/**********************************************************************************/
/* SHOW DEPOSIT command                                                           */
/* Usage:                                                                         */
/*    show deposit                                                                */                 
/*      Available Option(s):                                                      */
/*         /description          Display description                              */
/*         /version              Display DEPOSIT version                          */
/*         /host                 Display hostname                                 */
/*         /pwd                  Display user's password                          */
/*         /port                 Display Port used to connect to DEPOSIT          */
/*         /usr                  Display User used to connect to DEPOSIT          */
/*         /db                   Display database used to connect to DEPOSIT      */
/**********************************************************************************/
void COMMAND_SHOWDEPOSIT(int idcmd,char *command_line)
{
   if (DEPOisConnected() == false) 
   {
      ERROR(ERR_NOTCONNECTED,"Not connected to any deposit.\n");
      return;
   };
   
   int id=0;
   if (optionIsSET("opt_name")        == true) { printf("Deposit Name='%s'\n", varGet(GVAR_DEPNAME)); id++; }; 
   if (optionIsSET("opt_description") == true) { printf("Description='%s'\n",  varGet(GVAR_DEPDESCR));id++; }; 
   if (optionIsSET("opt_host")        == true) { printf("host='%s'\n",         varGet(GVAR_DEPHOST)); id++; }; 
   if (optionIsSET("opt_port")        == true) { printf("port='%s'\n",         varGet(GVAR_DEPPORT)); id++; }; 
   if (optionIsSET("opt_usr")         == true) { printf("usr='%s'\n",         varGet(GVAR_DEPUSER)); id++; }; 
   if (optionIsSET("opt_pwd")         == true) { printf("pwd='%s'\n",     varGet(GVAR_DEPPWD));  id++; }; 
   if (optionIsSET("opt_db")          == true) { printf("db='%s'\n",     varGet(GVAR_DEPDB));   id++; }; 
   if (optionIsSET("opt_version")     == true) 
   { 
      char *query=malloc(1024);
      sprintf(query,"select version from %s.config",varGet(GVAR_DEPUSER));
      int rows=DEPOquery(query,0);
      printf("Version='%s'\n",     DEPOgetString(0,0));   
      DEPOqueryEnd();
      free(query);
      id++;
   };
   if (id == 0)
   {   
      char *query=malloc(1024);
      sprintf(query,"select description,version,to_char(created,'%s'),to_char(updated,'%s') from %s.config",
                    varGet(GVAR_CLUDATE_FORMAT),
                    varGet(GVAR_CLUDATE_FORMAT),
                    varGet(GVAR_DEPUSER));
      int rows=DEPOquery(query,0);
      
      printf("%s ( %s )\n",DEPOgetString(0,0),varGet(RECM_VERSION));
      printf("Deposit parameters\n");
      printSection("Default");
      printOption("Deposit Name",   "",            varGet(GVAR_DEPNAME));
      printOption("Description",    "/description",varGet(GVAR_DEPDESCR));
      printOption("Host",           "/host",       varGet(GVAR_DEPHOST));
      printOption("Port",           "/port",       varGet(GVAR_DEPPORT));
      printOption("User",           "/usr",        varGet(GVAR_DEPUSER));
      printOption("Password",       "/pwd",        "********");
      printOption("Database",       "/db",         varGet(GVAR_DEPDB));    
      
      printSection("Deposit Release");
      printOption("Version",    "",DEPOgetString(0,1));
      printOption("Created",    "",DEPOgetString(0,2));
      printOption("Last Update","",DEPOgetString(0,3));
      printf("\nTo modify configuration, use qualifier enclosed in parentheses\n");
      printf("Example:\n");
      printf("\tRECM> modify deposit /pwd=\"<my_new_password>\"\n");
      
      DEPOqueryEnd();
      free(query);
   }
}


/**********************************************************************************/
/* MODIFY DEPOSIT command                                                         */
/* Usage:                                                                         */
/*    set deposit                                                                 */                 
/*      Available Option(s):                                                      */
/*         /session              Apply modifications at session level only        */
/*      Available Qualifier(s):                                                   */
/*         /description          Change description                               */
/*         /host=<STRING>        Change hostname                                  */
/*         /pwd=<STRING>         Change user's password                           */
/*         /port=<NUMBER>        Change Port used to connect to DEPOSIT           */
/*         /usr=<STRING>         Change User used to connect to DEPOSIT           */
/*         /db=<STRING>          Change database used to connect to DEPOSIT       */
/**********************************************************************************/
void COMMAND_MODIFYDEPOSIT(int idcmd,char *command_line)
{
   if (isConnectedAndRegistered() == false) return;

   if (qualifierIsUNSET("qal_description") == false)
   { varAdd(GVAR_DEPDESCR,varGet("qal_description"));globalArgs.configChanged=1; };

   if (qualifierIsUNSET("qal_host") == false)
   { varAdd(GVAR_DEPDESCR,varGet("qal_host"));globalArgs.configChanged=1; };

   if (qualifierIsUNSET("qal_port") == false)
   { varAdd(GVAR_DEPDESCR,varGet("qal_port"));globalArgs.configChanged=1; };

   if (qualifierIsUNSET("qal_usr") == false)
   { varAdd(GVAR_DEPDESCR,varGet("qal_user"));globalArgs.configChanged=1; };

   if (qualifierIsUNSET("qal_pwd") == false)
   { varAdd(GVAR_DEPDESCR,varGet("qal_pwd"));globalArgs.configChanged=1; };

   if (qualifierIsUNSET("qal_db") == false)
   { varAdd(GVAR_DEPDESCR,varGet("qal_db"));globalArgs.configChanged=1; };

   if (optionIsSET("opt_session") == false && globalArgs.configChanged == 1) 
   {
      SaveConfigFile("DEPO",varGet(GVAR_DEPNAME),SECTION_DEPOSIT);
   }
   else
   {
      globalArgs.configChanged=0; // Disable
   };
}
