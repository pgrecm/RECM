/**********************************************************************************/
/* SWITCH WAL command                                                             */
/* Usage:                                                                         */
/*    switch wal                                                                  */                 
/*      Available Option(s):                                                      */
/*         /verbose             More detailed messages                            */
/*         /nochkpt             Do not initiate CHECKPOINT                        */
/**********************************************************************************/
void COMMAND_SWITCHWAL(int idcmd,char *command_line)
{
   if (CLUisConnected() == false)
   {
      ERROR(ERR_NOTCONNECTED,"Not connected to any cluster.\n");
      return;
   };
   int opt_verbose=optionIsSET("opt_verbose");
   int saved_verbose=globalArgs.verbosity;
   globalArgs.verbosity=opt_verbose;
   
   cluster_doSwitchWAL( optionIsSET("opt_nochkpt")==false );

   globalArgs.verbosity=saved_verbose;
}
