/* 

SAVE command 

Write modifications to the configuration file

/*
@command save
@definition
Write changes to the configuration file.
Configuration file is located in directory '[HOME]/.recm/recm.config' by default.
You can change the location by using environment variable <b>RECM_CONFIG</b>.
@end
 
*/

void COMMAND_SAVE(int idcmd,char *command_line)
{
   TRACE("(COMMAND_SAVE)\n");
   globalArgs.configChanged=1;
   SaveConfigFile(NULL,"default",SECTION_DEFAULT);
   //varDump(1);
}
