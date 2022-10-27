/**********************************************************************************/
/* REGISTER CLUSTER command                                                       */
/* Usage:                                                                         */
/*      register cluster                                                                  */
/*      options:                                                                  */
/*            /force          Force registration again                            */
/*      Qualifiers:                                                               */
/*            /cid=<CID>           Cluster ID to modify                           */
/* Conditions :                                                                   */
/* - 1 - You must be connected to a deposit first.                                */
/* - 2 - You must be connected LOCALLY to the cluster you want to register.       */
/* - 3 - Configuration parameter 'cluster_name' must be set.                      */
/*                                                                                */
/**********************************************************************************/
/*
@command register cluster
@definition
Register a PostgreSQL cluster into the deposit.
If you recreate the deposit, youmay have to register each cluster to the deposit.
The option '/cid' allow you to register each cluster exactly with the same ID that it was having. 
This could be very usefull to register with the same ID, to register old backups.
'/force' and '/cid' are mutually exclusive.

@option "/force"   "Force registration, even if the cluster was already registered."
@option "/cid=CID" "Force the cluster ID to have a specific ID"
 
@example
@inrecm register cluster /force /cid=1 
@out recm-err(2804): You cannot use '/cid' with '/force'.
@inrecm register cluster /cid=1 
@out recm-err(b04): Cluster 'CLU12' already registered. Us option '/force' to register again.
@inrecm
@end
 
*/
void COMMAND_REGCLUSTER(int idcmd,char *command_line)
{
   if (DEPOisConnected(true) == false) return;

   memBeginModule();
   char *query=memAlloc(1024);
   
   // Need to retreive the IP Address of the host if '/ip' is not set
   if (qualifierIsUNSET("qal_ip") == true)
   {
      struct ifaddrs * ifAddrStruct=NULL;
      struct ifaddrs * ifa=NULL;
      void * tmpAddrPtr=NULL;
      getifaddrs(&ifAddrStruct);
      ifa = ifAddrStruct;
      int fnd=0;
      while (ifa != NULL && fnd==0) 
      {
         if (!ifa->ifa_addr) { continue; }
         if (ifa->ifa_addr->sa_family == AF_INET) 
         { 
            tmpAddrPtr=&((struct sockaddr_in *)ifa->ifa_addr)->sin_addr;
            char addressBuffer[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, tmpAddrPtr, addressBuffer, INET_ADDRSTRLEN);
            //printf("%s\n",ifa->ifa_name);
            if (strncmp(ifa->ifa_name,"lo",2) != 0) { varAdd("qal_ip",addressBuffer); fnd++;};
         };
         ifa = ifa->ifa_next;
      }
   }
   
   int rows=CLUquery("select coalesce(setting,'<NOT_SET>') from pg_settings where name='cluster_name'",0);
   varAdd(GVAR_CLUNAME,CLUgetString(0,0));
   CLUqueryEnd();
   if (varIsSET(GVAR_CLUNAME) == false)
   {
      ERROR(ERR_BADCLUNAME,"Configuration 'cluster_name' not set. Cannot register cluster.\n");
      memEndModule();
      return;
   }
   // now, check if we are already registred ?
   sprintf(query,"select count(*) from %s.clusters where clu_nam='%s'",
                 varGet(GVAR_DEPUSER),
                 varGet(GVAR_CLUNAME));
   rows=DEPOquery(query,0);
   if (DEPOgetLong(0,0) == 1)
   {
      if (optionIsSET("opt_force") == false)
      {
         ERROR(ERR_ALREADYREG,"Cluster '%s' already registered. Us option '/force' to register again.\n",
                              varGet(GVAR_CLUNAME));
         DEPOqueryEnd();
         memEndModule();
         return;
      }
      if (qualifierIsUNSET("qal_cid") == false && optionIsSET("opt_force") == true)
      {
         ERROR(ERR_BADOPTION,"You cannot use '/cid' with '/force'.\n");
         DEPOqueryEnd();
         memEndModule();
         return;
      };
      DEPOqueryEnd();
      sprintf(query,"select CID from %s.clusters where clu_nam='%s'",
                    varGet(GVAR_DEPUSER),
                    varGet(GVAR_CLUNAME));
      rows=DEPOquery(query,0);
      varAdd(GVAR_CLUCID,DEPOgetString(0,0));
      DEPOqueryEnd();
      varAdd(GVAR_REGDEPO ,varGet(GVAR_DEPNAME));
      globalArgs.configChanged=1;
      SaveConfigFile("CLU",varGet(GVAR_CLUNAME),SECTION_CLUSTER);
      globalArgs.configChanged=1;
      SaveConfigFile("DEPO",varGet(GVAR_DEPNAME),SECTION_DEPOSIT);
      memEndModule();
      return;
   }
   DEPOqueryEnd();

   INFO("Registering cluster '%s' (IP %s)\n",varGet(GVAR_CLUNAME),varGet("qal_ip"));

   long NEW_CID;

   if (qualifierIsUNSET("qal_cid") == false && optionIsSET("opt_force") == true)
   {
      ERROR(ERR_BADOPTION,"You cannot use '/cid' with '/force'.\n");
      memEndModule();
      return;
   };
   if (qualifierIsUNSET("qal_cid") == false)
   {
      int idIsValid=false;
      // - 1 - Check if ID not already used  !
      sprintf(query,"select count(*) from %s.clusters where cid=%s",varGet(GVAR_DEPUSER),varGet("qal_cid"));
      rows=DEPOquery(query,0);
      if (rows == 1 && DEPOgetInt(0,0) == 0) { idIsValid=true; };
      DEPOqueryEnd();
      
      if (idIsValid == false)
      {
         ERROR(ERR_CIDALREADYUSED,"CID '%s' already used.  Choose an other id.\n",varGet("qal_cid"));
         memEndModule();
         return;
      };
      NEW_CID=varGetLong("qal_cid");
   }
   else
   {
      sprintf(query,"select coalesce(max(cid)+1,1) from %s.clusters",varGet(GVAR_DEPUSER));
      rows=DEPOquery(query,0);
      if (rows == 1) { NEW_CID=DEPOgetInt(0,0); }
                else { NEW_CID=1; };
      DEPOqueryEnd();
   }
   INFO("Cluster CID will be %ld.\n",NEW_CID);
   sprintf(query,"insert into %s.clusters (cid,clu_sts,clu_nam,clu_info,clu_addr,clu_port,rep_usr,rep_pwd) values(%ld,'%s','%s','%s','%s','%s','(null)','(null)')",
                 varGet(GVAR_DEPUSER),
                 NEW_CID,
                 CLUSTER_STATE_ENABLED,
                 varGet(GVAR_CLUNAME),
                 varGet(GVAR_CLUDESCR),
                 varGet("qal_ip"),
                 varGet(GVAR_CLUPORT));
   rows=DEPOquery(query,0);
   DEPOqueryEnd();

   // Load default values now !
   CLUloadDepositOptions(NEW_CID);

   sprintf(query,"select CID from %s.clusters where clu_nam='%s'",
                 varGet(GVAR_DEPUSER),
                 varGet(GVAR_CLUNAME));
   rows=DEPOquery(query,0);
   varAdd(GVAR_REGDEPO ,varGet(GVAR_DEPNAME));
   varAdd(GVAR_CLUCID,DEPOgetString(0,0));
   DEPOqueryEnd();
   globalArgs.configChanged=1;
   SaveConfigFile("CLU",varGet(GVAR_CLUNAME),SECTION_CLUSTER);
   globalArgs.configChanged=1;
   SaveConfigFile("DEPO",varGet(GVAR_DEPNAME),SECTION_DEPOSIT);
   INFO("Cluster '%s' registered in deposit '%s' (cid=%s)\n",varGet(GVAR_CLUNAME),varGet(GVAR_DEPNAME),varGet(GVAR_CLUCID));
   memEndModule();
}
