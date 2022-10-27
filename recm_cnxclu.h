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
/*
@command connect cluster
@definition
Establish a connection to the PostgreSQL server(cluster).

@option "/verbose"    "Display more details"
@option "/host=HOST"  "Host name to connect to"
@option "/port=PORT"  "port number to connect to"
@option "/usr=USER"   "Username to connect to"
@option "/pwd=PASSWD" "HUser's password to connect to"
@option "/db=DBNAME"  "Database name to connect to"

@example
@inrecm connect cluster /usr=postgres /pwd=postgres /db=postgres /port=5432 /host=localhost
@out recm-inf: Connected to cluster 'CLU12'
@out CLU12>
@inrecm
@end
*/
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

