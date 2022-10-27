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
/*
@command modify backup
@definition
Modify a backup. You can change the status, the TAG of abackup.
You can also exclude the backup from the retention plicy (/lock).

@option "/available"      "Change status to 'available'"       
@option "/incomplete"     "Change status to 'incomplete'"      
@option "/obsolete"       "Change status to 'oboslete'"        
@option "/lock            "disable retention policy"           
@option "/unlock"         "enable retention policy"                                             
@option "/before=DATE"    "list WALs before a date"            
@option "/after=DATE"     "list WALs after a date"             
@option "/cid=CID"        "Modify a backup of an other cluster"
@option "/uid=UID"        "UID of the backup to change"        
@option "/tag=STRING"     "Change TAG of a backup"             

@example
@inrecm modify backup/uid=0001634be3052e3329e8/tag="Must_keep_this" 
@out recm-inf: Backup UID '0001634be3052e3329e8' changed.
@inrecm list backup/wal 
@out List backups of cluster 'CLU12' (CID=1)
@out UID                    Date                 Type   Status       TL          Size  Tag
@out ---------------------- -------------------- ------ ------------ ----   ---------- -------------------------
@out 000163487668106933d0   2022-10-13 22:34:48  WAL    AVAILABLE    36         112 MB 20221013_CLU12_WAL
@out 0001634876943425da80   2022-10-13 22:35:32  WAL    AVAILABLE    36          32 MB 20221013_CLU12_WAL
@out 00016349a620316c7358   2022-10-14 20:10:40  WAL    AVAILABLE    36         208 MB 20221014_CLU12_WAL
@out 00016349a65c0f23fa00   2022-10-14 20:11:40  WAL    AVAILABLE    36          32 MB 20221014_CLU12_FULL
@out 00016349b08533b7e228   2022-10-14 20:55:01  WAL    AVAILABLE    36          64 MB WAL_cron1
@out 0001634be2931ec906f8   2022-10-16 12:53:07  WAL    AVAILABLE    36          48 MB 20221016_CLU12_FULL
@out 0001634be3052e3329e8   2022-10-16 12:55:01  WAL    AVAILABLE    36          16 MB Must_keep_this
@inrecm
@end
*/
void COMMAND_MODIFYBACKUP(int idcmd,char *command_line)
{
   if (DEPOisConnected(true) == false) return;

   if (optionIsSET("opt_verbose") == true) globalArgs.verbosity=true;                                                              // Set Verbosity

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
         memEndModule();
         return;
      }
      strcpy(cluster_name,DEPOgetString(0,0));
      cluster_id=atol(varGet("qal_cid"));
      DEPOqueryEnd();
   }
   else
   {
      if (CLUisConnected(true) == false) 
      {
         memEndModule();
         return;
      }
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
         TRACE("[rc=%d]\n",row);
         if (DEPOrowCount() == 0) { ERROR(ERR_BACKUPNOTFND,"Backup UID '%s' not found in DEPOSIT.\n",UID); }
                             else { INFO("Backup UID '%s' changed.\n",UID); };
         DEPOqueryEnd();
      };
      UID=comma;
      UID++;
      comma=strchr(UID,',');           // Reach next delimiter
   }
   memEndModule();
   return;
};

   