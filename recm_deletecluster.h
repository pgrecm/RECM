/**********************************************************************************/
/* DELETE CLUTSER command                                                         */
/* Usage:                                                                         */
/*    delete cluster                                                              */                 
/*      Available Option(s):                                                      */
/*         /verbose                                                               */
/*         /force              Cluster must be 'disabled' to be deleted           */
/*         /includefiles       Delete backup files with cluster entry             */
/*      Available Qualifier(s):                                                   */
/*         /cid=<CID>          ID of the cluster to delete                  */
/**********************************************************************************/
void COMMAND_DELETECLUSTER(int idcmd,char *command_line)
{
   if (DEPOisConnected(true) == false) return; 

   if (optionIsSET("opt_verbose") == true) globalArgs.verbosity=true;                                                              // Set Verbosity

   // Refuse to delete currently connected cluster.
   long current_CID=varGetLong(GVAR_CLUCID);
   if (current_CID == varGetLong("qal_cid") && current_CID != 0)
   {
      ERROR(ERR_INVCLUID,"Cannot delete current connected cluster ID %ld\n",current_CID);
      return;
   }
   memBeginModule();
   char *query=memAlloc(1024);
   sprintf(query,"select clu_nam,clu_sts from %s.clusters where cid=%s",
                 varGet(GVAR_DEPUSER),
                 varGet("qal_cid"));
   int row=DEPOquery(query,0);
   if (row == 0)
   {
      ERROR(ERR_CIDNOTFOUND,"CID '%s' not found in DEPOSIT.\n",varGet("qal_cid"));
      DEPOqueryEnd();
      memEndModule();
      return;
   }
   char *cluster_name=memAlloc(128);  strcpy(cluster_name,  DEPOgetString(0,0));
   char *cluster_status=memAlloc(128);strcpy(cluster_status,DEPOgetString(0,1));
   DEPOqueryEnd();
   TRACE("cluster status='%s' State='%s' force='%s'\n",cluster_status,CLUSTER_STATE_ENABLED,VAR_SET_VALUE);                        // CLUSTER must be disabled to be deleted
   if (strcmp(cluster_status,CLUSTER_STATE_ENABLED) == 0 && optionIsSET("opt_force")==false)
   {
      ERROR(ERR_CLUSTERENABLED,"Cluster '%s' is enabled. Use '/force' option or disable first the cluster.\n",
                               cluster_name);
      memEndModule();
      return;
   };

   int has_error=0;

   if (optionIsSET("opt_includefiles") == true)                                                                                         // Delete physical files also
   {
      char *msg=memAlloc(1024);
      char *file_to_delete=memAlloc(1024);
      sprintf(query,"select b.bck_id,b.bckdir,p.pname piece from %s.backups b,%s.backup_pieces p "
                    "where p.cid=b.cid and p.bck_id=b.bck_id and b.cid=%s order by bck_id,p.pcid"
                    ,varGet(GVAR_DEPUSER)
                    ,varGet(GVAR_DEPUSER)
                    ,varGet("qal_cid"));
      row=DEPOquery(query,0);
      int idrow=0;
      while (idrow < row)
      {
         if (varExist(SOURCE_DIRECTORY) == true) { sprintf(file_to_delete,"%s/%s",varGet(SOURCE_DIRECTORY),DEPOgetString(0,2)); }  // used 'set source/directory' 
                                            else { sprintf(file_to_delete,"%s/%s",DEPOgetString(0,1),DEPOgetString(0,2)); };       // Use default directory
         int rcd=unlink(file_to_delete);
         if (rcd == 0) { sprintf(msg,"Deleting file '%s' ...Ok",file_to_delete); }
                  else { int e=errno;
                         sprintf(msg,"Deleting file '%s' ...failed (%d : %s)",file_to_delete,e,strerror(e)); 
                         has_error++;
                       };
         VERBOSE("%s\n",msg);
         idrow++;
      }
      DEPOqueryEnd();
   };

   if (has_error > 0 && optionIsSET("opt_force") == false)                                                                         // If some files are missing, 
   {                                                                                                                               // option '/force' may help to delete the cluster entry from the DEPOSIT.
      ERROR(ERR_DELFAILED,"'/includefiles' failed. Remove the option '/includefiles'. You will have to delete files by yourself.\n");
      memEndModule();
      return;
   }

   sprintf(query,"delete from %s.backup_dbs where cid=%s\n",varGet(GVAR_DEPUSER),varGet("qal_cid"));
   row=DEPOquery(query,0);
   DEPOqueryEnd();
   printf(".");
   sprintf(query,"delete from %s.backup_ndx where cid=%s\n",varGet(GVAR_DEPUSER),varGet("qal_cid"));
   row=DEPOquery(query,0);
   DEPOqueryEnd();
   printf(".");
   sprintf(query,"delete from %s.backup_pieces where cid=%s\n",varGet(GVAR_DEPUSER),varGet("qal_cid"));
   row=DEPOquery(query,0);
   DEPOqueryEnd();
   printf(".");
   sprintf(query,"delete from %s.backup_rp where cid=%s\n",varGet(GVAR_DEPUSER),varGet("qal_cid"));
   row=DEPOquery(query,0);
   DEPOqueryEnd();
   printf(".");
   sprintf(query,"delete from %s.backup_tbl where cid=%s\n",varGet(GVAR_DEPUSER),varGet("qal_cid"));
   row=DEPOquery(query,0);
   DEPOqueryEnd();
   printf(".");
   sprintf(query,"delete from %s.backup_tbs where cid=%s\n",varGet(GVAR_DEPUSER),varGet("qal_cid"));
   row=DEPOquery(query,0);
   DEPOqueryEnd();
   printf(".");
   sprintf(query,"delete from %s.backup_wals where cid=%s\n",varGet(GVAR_DEPUSER),varGet("qal_cid"));
   row=DEPOquery(query,0);
   DEPOqueryEnd();
   printf(".");
   sprintf(query,"delete from %s.backups where cid=%s\n",varGet(GVAR_DEPUSER),varGet("qal_cid"));
   row=DEPOquery(query,0);
   DEPOqueryEnd();
   printf(".");
   sprintf(query,"delete from %s.clusters where cid=%s\n",varGet(GVAR_DEPUSER),varGet("qal_cid"));
   row=DEPOquery(query,0);
   DEPOqueryEnd();
   printf(".\nCluster '%s' deleted. (All Physical backup files may not be removed).\n",cluster_name);
   memEndModule();
   return;
};

   