/**********************************************************************************/
/* modify backup command                                                               */
/* Usage:                                                                         */
/*      list wal                                                                  */
/*      options:                                                                  */
/*            /available      Change status to 'available'                        */
/*            /incomplete     Change status to 'incomplete'                       */
/*            /obsolete       Change status to 'oboslete'                         */
/*            /lock           disable retention policy                            */
/*            /unlock         enable retention policy                             */
/*      Qualifiers:                                                               */
/*            /before=<DATE>  list WALs before a date                             */
/*            /after=<DATE>   list WALs after a date                              */
/*            /cid=<CID>      Modify a backup of an other cluster                 */
/*            /uid=<UID>      UID of the backup to change                         */
/*            /tag=<STRING>   Change TAG of a backup                              */
/**********************************************************************************/
void COMMAND_MODIFYBACKUP(int idcmd,char *command_line)
{
   if (DEPOisConnected() == false) 
   {
      ERROR(ERR_NOTCONNECTED,"Not connected to any deposit.\n");
      return;
   }
   
   // Change verbosity
   int opt_verbose=optionIsSET("opt_verbose");
   int saved_verbose=globalArgs.verbosity;
   globalArgs.verbosity=opt_verbose;
   memBeginModule();

   int cluster_id;
   if (strcmp(varGet(GVAR_CLUCID),VAR_UNSET_VALUE) != 0) cluster_id=varGetInt(GVAR_CLUCID);

   char *query=memAlloc(1024);
   char *cluster_name=memAlloc(128); 

   if (qualifierIsUNSET("qal_cid") == false)
   {
      sprintf(query,"select clu_nam from %s.clusters where cid=%s",
                    varGet(GVAR_DEPUSER),
                    varGet("qal_cid"));
      int rows=DEPOquery(query,0);
      if (rows <= 0)
      {
         ERROR(ERR_BADCLUNAME,"Unknown cluster ID '%s'\n",varGet("qal_cid"));
         DEPOqueryEnd();
         globalArgs.verbosity=saved_verbose;
         memEndModule();
         return;
      }
      strcpy(cluster_name,DEPOgetString(0,0));
      cluster_id=atol(varGet("qal_cid"));
      DEPOqueryEnd();
   }
   else
   {
      if (CLUisConnected() == false) return;
      strcpy(cluster_name,varGet(GVAR_CLUNAME));
      cluster_id=varGetInt(GVAR_CLUCID);
   }
   
   char virgule=' ';

   char *qry_where=memAlloc(512); 
   char *qry_tag=memAlloc(512);   qry_tag[0]=0x00;
   char *qry_state=memAlloc(512); qry_state[0]=0x00;

   // Allow multiple UID separated by comma
   char *UIDlist=memAlloc(strlen(varGet("qal_uid"))+2);
   strcpy(UIDlist,varGet("qal_uid"));
   strcat(UIDlist,",");
   char *UID=UIDlist;
   char *comma=strchr(UIDlist,',');
   int stop=false;
   while (comma != NULL && stop==false)
   {
      comma[0]=0x00;
      virgule=' ';
      sprintf(qry_where," where cid=%d and bck_id='%s'",cluster_id,UID);
      if (qualifierIsUNSET("qal_tag") == false)
      {  sprintf(qry_tag,"%c bcktag='%s'",virgule,asLiteral(varGet("qal_tag")));
         virgule=',';
      };
      int state_changed=0;
      if (qualifierIsUNSET("qal_location") == false)
      {  sprintf(qry_state,"%c bckdir='%s'",virgule,varGet("qal_location"));
         virgule=',';
         state_changed++;
      };
         
      if (optionIsSET("opt_available") == true)
      {  sprintf(qry_state,"%c bcksts=%d",virgule,RECM_BACKUP_STATE_AVAILABLE);
         virgule=',';
         state_changed++;
      };
      if (optionIsSET("opt_obsolete") == true && state_changed == 0)
      {  sprintf(qry_state,"%c bcksts=%d",virgule,RECM_BACKUP_STATE_OBSOLETE);
         virgule=',';
         state_changed++;
      };
      if (optionIsSET("opt_lock") == true && state_changed == 0)
      {  sprintf(qry_state,"%c options=(options|%d),bcksts=%d",virgule,BACKUP_OPTION_KEEPFOREVER,RECM_BACKUP_STATE_AVAILABLE);
         virgule=',';
         state_changed++;
      };
      if (optionIsSET("opt_unlock") == true && state_changed == 0)
      {  sprintf(qry_state,"%c options=(options & (%d-%d))",virgule,BACKUP_OPTION_MASK,BACKUP_OPTION_KEEPFOREVER);
         virgule=',';
         state_changed++;
      };   
      if (optionIsSET("opt_incomplete") == true && state_changed == 0)
      {  sprintf(qry_state,"%c bcksts=%d",virgule,RECM_BACKUP_STATE_INCOMPLETE);
         virgule=',';
         state_changed++;
      };
      if (virgule == ' ')
      {
         WARN(WRN_NOCHANGE,"No change to do.\n");
         stop=true;
      }
      else
      {
         sprintf(query,"update %s.backups set ",
                    varGet(GVAR_DEPUSER));
         strcat(query,qry_tag);
         strcat(query,qry_state);
         strcat(query,qry_where);
         int row=DEPOquery(query,0);
         if (DEPOrowCount() == 0) { ERROR(ERR_BACKUPNOTFND,"Backup UID '%s' not found in DEPOSIT.\n",UID); }
                             else { INFO("Backup UID '%s' changed.\n",UID); };
         DEPOqueryEnd();
      };
      UID=comma;
      UID++;
      comma=strchr(UID,',');           // Reach next delimiter
   }
   globalArgs.verbosity=saved_verbose;
   memEndModule();
   return;
};

   