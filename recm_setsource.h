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
   if (optionIsSET("opt_verbose") == true) globalArgs.verbosity=true;                                                              // Set Verbosity

   if (qualifierIsUNSET("qal_directory") == false)
   { 
      varAdd(SOURCE_DIRECTORY,varGet("qal_directory"));
      VERBOSE("Set SOURCE DIRECTORY to '%s'\n",varGet(SOURCE_DIRECTORY));
   };
   if (qualifierIsUNSET("qal_blksize") == false)
   { 
      varAdd(SOURCE_BLKSIZE,varGet("qal_blksize"));
   };
   return;
}
