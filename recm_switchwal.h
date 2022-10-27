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
   if (CLUisConnected(true) == false) return;

   if (optionIsSET("opt_verbose") == true) globalArgs.verbosity=true;                                                              // Set Verbosity
   
   cluster_doSwitchWAL( optionIsSET("opt_nochkpt")==false );

}
