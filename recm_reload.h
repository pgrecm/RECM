/* 

RELOAD command 

Write modifications to the configuration file

*/
/*
@command reload
@definition
Reload configuration files from disk.
@end

*/
 
void COMMAND_RELOAD(int idcmd,char *command_line)
{
   TRACE("(COMMAND_RELOAD)\n");
   globalArgs.configChanged=1;
   LoadConfigFile("CLU",varGet(GVAR_CLUNAME));
   //varDump(1);
}
