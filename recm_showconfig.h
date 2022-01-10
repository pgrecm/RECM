/**********************************************************************************/
/* SHOW CONFIG command                                                            */
/* Usage:                                                                         */
/*    show config                                                                 */                 
/*      Available Option(s):                                                      */
/*         /timezone                Defult PostgreSQL timezone                    */
/*         /date_format             Default date format                           */
/*         /prompt                  Prompt string                                 */
/*         /history                 History size (number of command lines)        */
/*         /bindir                  PostgreSQL binary location                    */
/**********************************************************************************/
void COMMAND_SHOWCONFIG(int idcmd,char *command_line)
{
   int id=0;
   if (optionIsSET("opt_date_format")   == true) { printf("Date format='%s'\n",varGet(SYSVAR_DATE_FORMAT)); id++; };
   if (optionIsSET("opt_prompt")        == true) { printf("Prompt string='%s'\n",varGet(SYSVAR_PROMPT)); id++; };
   if (optionIsSET("opt_history")       == true) { printf("Command line history='%s'\n",varGet(SYSVAR_HISTDEEP)); id++; };
   if (optionIsSET("opt_bindir")        == true) { printf("postgreSQL binary directory ='%s'\n",varGet(SYSVAR_PGBIN)); id++; };
   if (optionIsSET("opt_timezone")      == true) { printf("Default timezone='%s'\n",varGet(SYSVAR_TIMEZONE));          id++; };
   if (id ==0)
   {
      printf("recm version %s.\n",varGet(RECM_VERSION));
      printf("Configuration parameters\n");
      printSection("Default"); 
         printOption("Date format",                       "/date_format",    varGet(SYSVAR_DATE_FORMAT));
         printOption("Prompt string",                     "/prompt",         varGet(SYSVAR_PROMPT));
         printOption("Command line history",              "/history",        varGet(SYSVAR_HISTDEEP));
         printOption("postgreSQL binary directory",       "/bindir",         varGet(SYSVAR_PGBIN));
         printOption("Default timezone",                  "/timezone",       varGet(SYSVAR_TIMEZONE));
      printf("\nTo modify configuration, use qualifier enclosed in parentheses\n");
      printf("Example:\n");
      printf("\tRECM> set config /prompt=\"my_prompt\"\n");
   }
}

/**********************************************************************************/
/* SET CONFIG command                                                             */
/* Usage:                                                                         */
/*    set config                                                                  */                 
/*      Available Qualifier(s):                                                   */
/*         /timezone=<STRING>     Default PostgreSQL timezone                     */
/*         /date_format=<STRING>  Default date format                             */
/*         /prompt=<STRING>       Change prompt string                            */
/*         /history=<NUMBER>      Change History size(Number of command lines)    */
/*         /bindir=<PATh>         Change PostgreSQL binary location               */
/**********************************************************************************/
void COMMAND_SETCONFIG(int idcmd,char *command_line)
{
   if (qualifierIsUNSET("qal_prompt") == false)
   { varAdd(SYSVAR_PROMPT,varGet("qal_prompt"));                globalArgs.configChanged=1; };   

   if (qualifierIsUNSET("qal_date_format") == false)
   { varAdd(SYSVAR_DATE_FORMAT,varGet("qal_date_format"));    globalArgs.configChanged=1; };

   if (qualifierIsUNSET("qal_bindir") == false)
   {
      TRACE("set PGBIN='%s'\n",varGet("qal_bindir"));
      char *pgctl=malloc(1024);
      sprintf(pgctl,"%s/pg_ctl",varGet("qal_bindir"));
      if (file_exists(pgctl) == false)
      {
         ERROR(ERR_INVALIDPGBIN,"PostgreSQL binary directory is not valid.\n");
      }
      else
      {
         varAdd(SYSVAR_PGBIN,varGet("qal_bindir"));               globalArgs.configChanged=1;
      };
   }
   
   if (qualifierIsUNSET("qal_history") == false)
   {
      TRACE("set HISTORY='%s'\n",varGet("qal_history"));
      varAdd(SYSVAR_HISTDEEP,varGet("qal_history"));  globalArgs.configChanged=1;
   }
   
   if (qualifierIsUNSET("qal_timezone") == false)
   {
      TRACE("set TIMEZONE='%s'\n",varGet("qal_timezone"));
      varAdd(SYSVAR_TIMEZONE,varGet("qal_timezone")); globalArgs.configChanged=1;
   }
   
   if (optionIsSET("opt_session") == false) 
   {
      SaveConfigFile(NULL,"default",SECTION_DEFAULT);
   }
   else
   {
      globalArgs.configChanged=0; // Disable
   };
}

