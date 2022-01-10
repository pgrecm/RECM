/**********************************************************************************/
/* SET SOURCE command                                                             */
/* Usage:                                                                         */
/*    set source                                                                  */                 
/*      Available Qualifier(s):                                                   */
/*         /directory=<PATH>          use different source path                   */
/*         /blksize=<SIZE>            use different buffer size                   */

/**********************************************************************************/
void COMMAND_SETSOURCE(int idcmd,char *command_line)
{
   // Change verbosity
   int opt_verbose=optionIsSET("opt_verbose");
   int saved_verbose=globalArgs.verbosity;
   globalArgs.verbosity=opt_verbose;

   if (qualifierIsUNSET("qal_directory") == false)
   { 
      varAdd(SOURCE_DIRECTORY,varGet("qal_directory"));
      VERBOSE("Set SOURCE DIRECTORY to '%s'\n",varGet(SOURCE_DIRECTORY));
   };
   if (qualifierIsUNSET("qal_blksize") == false)
   { 
      varAdd(SOURCE_BLKSIZE,varGet("qal_blksize"));
   };

   globalArgs.verbosity=saved_verbose;
   return;
}
