/**********************************************************************************/
/* CONNECT CLUSTER command                                                        */
/* Usage:                                                                         */
/*    connect cluster                                                             */                 
/*      Available Qualifier(s):                                                   */
/*         /host=<String>             Host                                        */
/*         /port=<PORT>               port                                        */
/*         /usr=<STRING>              User to connect to                          */
/*         /pwd=<STRING>              Password                                    */
/*         /db=<STRING>               Database to connect to                      */
/**********************************************************************************/
void COMMAND_CNXCLUSTER(int idcmd,char *command_line)
{
   CLUdisconnect();
   if (CLUconnect(varGet("qal_db"),
                  varGet("qal_usr"),
                  varGet("qal_pwd"),
                  varGet("qal_host"),
                  varGet("qal_port")) == true)
   {
      if (strlen(varGet(GVAR_CLUNAME)) == 0)
      {
         WARN(WRN_CLUNONAME,"Cluster '%s' has no  name. Please set parameter 'cluster_name'.\n",varGet("qal_host"));
      }
      else
      {
         varAdd(GVAR_CLUDB  ,varGet("qal_db"));
         varAdd(GVAR_CLUUSER,varGet("qal_usr"));
         varAdd(GVAR_CLUPWD ,varGet("qal_pwd"));
         varAdd(GVAR_CLUHOST,varGet("qal_host"));
         varAdd(GVAR_CLUPORT,varGet("qal_port"));
         CLUloadData();

         globalArgs.configChanged=1;
         SaveConfigFile("CLU",varGet(GVAR_CLUNAME),SECTION_CLUSTER);
         return;
      }
   }
}

