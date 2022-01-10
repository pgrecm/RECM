/* 

SAVE command 

Write modifications to the configuration file

*/

void COMMAND_SAVE(int idcmd,char *command_line)
{
   printf("(COMMAND_SAVE)A\n");
   globalArgs.configChanged=1;
   SaveConfigFile(NULL,"default",SECTION_DEFAULT);
   varDump(1);
}
