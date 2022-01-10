/* 

RELOAD command 

Write modifications to the configuration file

*/

 
void COMMAND_RELOAD(int idcmd,char *command_line)
{
   globalArgs.configChanged=1;
   LoadConfigFile("CLU",varGet(GVAR_CLUNAME));
   varDump(1);
}
