/**********************************************************************************/
/* DELETE BACKUP command                                                          */
/* Usage:                                                                         */
/*    delete backup                                                               */                 
/*      Available Option(s):                                                      */
/*         /incomplete               Delete INCOMPLETE backups                    */
/*         /obsolete                 Delete OBSOLETE backups                      */
/*         /failed                   Delete FAILED backups                        */
/*         /noremove                 Delete entries from DEPOSIT only             */
/*      Available Qualifier(s):                                                   */
/*         /uid=<STRING>             Delete a backup by it's UID                  */
/*         /uid=<UID1>,<UID2>...     Delete multipe backups by UIDs               */
/*         /before=<STRING>          Delete all backup where UID is older         */
/*         /after=<STRING>           Delete all backup where UID is younger       */
/**********************************************************************************/
void COMMAND_DELETEBACKUP (int idcmd,const char *command_line)
{
   if (isConnectedAndRegistered() == false) return;
   
   if (varExist(GVAR_BACKUPDIR) == false)
   {
      ERROR(ERR_NOBACKUPDIR,"Backup directory not set.(See show cluster).\n");
      return;
   }
   
   memBeginModule();
   int opt_noremove=optionIsSET("opt_noremove");
   int opt_verbose=optionIsSET("opt_verbose");
   int saved_verbose=globalArgs.verbosity;
   globalArgs.verbosity=opt_verbose;

   int min_opt=0;
   char *bckid_list=memAlloc(1024);
   char *backup_location=memAlloc(1024);
   char *query=memAlloc(1024);
   char *subqry=memAlloc(1024);
   char *qry_before=memAlloc(128); qry_before[0]=0x00; 
   char *qry_after=memAlloc(128);  qry_after[0]=0x00; 
   char *qry_id=memAlloc(1024);    qry_id[0]=0x00;
   if (qualifierIsUNSET("qal_before") == false)
   { sprintf(qry_before," and bdate < to_timestamp('%s','%s')",varGet("qal_before"),varGet(GVAR_CLUDATE_FORMAT)); min_opt++;};

   if (qualifierIsUNSET("qal_after") == false)
   { sprintf(qry_after," and bdate > to_timestamp('%s','%s')",varGet("qal_after"),varGet(GVAR_CLUDATE_FORMAT));  min_opt++;};

   if (qualifierIsUNSET("qal_uid") == false)
   { 
      strcpy(qry_id,varGet("qal_uid"));
      bckid_list[0]=0x00;
      if (strchr(qry_id,',') != NULL)
      {
         char *comma=qry_id;
         while (comma != NULL)
         {
            char *next_comma=strchr(comma,',');
            if (next_comma == NULL) { if (strlen(bckid_list) > 0) strcat(bckid_list,",");
                                      strcat(bckid_list,"'");strcat(bckid_list,comma);strcat(bckid_list,"'");
                                    }
                               else { next_comma[0]=0x00; 
                                      if (strlen(bckid_list) > 0) strcat(bckid_list,",");
                                      strcat(bckid_list,"'");strcat(bckid_list,comma);strcat(bckid_list,"'");
                                      next_comma++;
                               };
            comma=next_comma;                   
         }
         sprintf(qry_id," and bck_id in (%s)",bckid_list);  
      }
      else
      {
          sprintf(qry_id," and bck_id='%s'",varGet("qal_uid"));  
      }
      min_opt++;
   }; 

   // Ignore KEEP_FOR_EVER backups. NYou need to remove the /lock flag to delete the backup.
   sprintf(query,"select bcktyp,bck_id,bcksts,bdate,bckdir from %s.backups where cid=%s and ((options & 8) = 0) ",
                  varGet(GVAR_DEPUSER),
                  varGet(GVAR_CLUCID));
   if (optionIsSET("opt_incomplete") == true) { strcat(query," and bcksts=2"); min_opt++;};
   if (optionIsSET("opt_obsolete")   == true) { strcat(query," and bcksts=1"); min_opt++;};
   if (optionIsSET("opt_failed")     == true) { strcat(query," and bcksts=4"); min_opt++;};
   strcat(query,qry_before);
   strcat(query,qry_after);
   strcat(query,qry_id);
   strcat(query," order by bdate");

   if (min_opt == 0)
   {
      ERROR(ERR_MISSMINOPT,"Incomplete command. Please add more options to be more precise.\n");
      memEndModule();
      return;
   };
   char *f_bcktyp=memAlloc(20);
   char *f_bck_id=memAlloc(20);
   char *f_bcksts=memAlloc(10);
   char *f_bdate=memAlloc(40);
   char *sf_pcnam=memAlloc(1024);
   char *sf_pc_id=memAlloc(128);
   int rows=DEPOquery(query,0);
   if (rows == 0)
   {
      INFO("No backup to delete\n");
   };
   int bcksts_n;
   int rc;
   for (int n=0;n < rows;n++)
   {
      strcpy(f_bcktyp,       DEPOgetString(n,0));
      strcpy(f_bck_id,       DEPOgetString(n,1));
      bcksts_n=              DEPOgetInt(n,2);
      strcpy(f_bdate,        DEPOgetString(n, 3));
      strcpy(backup_location,DEPOgetString(n, 4));
      sprintf(subqry,"select pcid,pname from %s.backup_pieces where cid=%s and bck_id ='%s' order by pcid",
                         varGet(GVAR_DEPUSER),
                         varGet(GVAR_CLUCID),
                         f_bck_id);
      switch(bcksts_n)
      {
      case RECM_BACKUP_STATE_AVAILABLE : VERBOSE("Scanning Backup %s '%s' (%s)\n",f_bcktyp,f_bck_id,RECM_BACKUP_STATE_AVAILABLE_S );break;
      case RECM_BACKUP_STATE_OBSOLETE  : VERBOSE("Scanning Backup %s '%s' (%s)\n",f_bcktyp,f_bck_id,RECM_BACKUP_STATE_OBSOLETE_S  );break;
      case RECM_BACKUP_STATE_INCOMPLETE: VERBOSE("Scanning Backup %s '%s' (%s)\n",f_bcktyp,f_bck_id,RECM_BACKUP_STATE_INCOMPLETE_S);break;
      case RECM_BACKUP_STATE_RUNNING   : VERBOSE("Scanning Backup %s '%s' (%s)\n",f_bcktyp,f_bck_id,RECM_BACKUP_STATE_RUNNING_S   );break;
      case RECM_BACKUP_STATE_FAILED    : VERBOSE("Scanning Backup %s '%s' (%s)\n",f_bcktyp,f_bck_id,RECM_BACKUP_STATE_FAILED_S    );break;
      }
     
      TRACE("sub-query=%s\n",subqry);
      int rowsq=DEPOquery(subqry,0);
      int j=0;
      while ( j < rowsq)
      {
         sprintf(sf_pcnam,"%s/%s",backup_location,DEPOgetString(j,1));
         if (file_exists(sf_pcnam) == true) 
         { 
            if (opt_noremove == false)
            {
               VERBOSE(" Removing physical file '%s'\n",sf_pcnam); 
               int rc=unlink(sf_pcnam); 
               if (rc != 0) 
               { 
                  ERROR(ERR_DELFAILED,"Could not delete file '%s': %s\n",sf_pcnam,strerror(errno)); 
               }
            }
            else
            {
               VERBOSE("File '%s' not deleted (Add option '/deletefiles')\n",sf_pcnam);
            }
          }
          else
          {
             VERBOSE("File '%s' already deleted.\n",sf_pcnam);
          }
          j++;
      }
      DEPOqueryEnd();
      sprintf(query,"delete from %s.backup_dbs where cid=%s and bck_id='%s'",varGet(GVAR_DEPUSER),varGet(GVAR_CLUCID),f_bck_id);
      rc=DEPOquery(query,0);
      DEPOqueryEnd();
      sprintf(query,"delete from %s.backup_ndx where cid=%s and bck_id='%s'",varGet(GVAR_DEPUSER),varGet(GVAR_CLUCID),f_bck_id);
      rc=DEPOquery(query,0);
      DEPOqueryEnd();
      sprintf(query,"delete from %s.backup_pieces where cid=%s and bck_id='%s'",varGet(GVAR_DEPUSER),varGet(GVAR_CLUCID),f_bck_id);
      rc=DEPOquery(query,0);
      DEPOqueryEnd();
      sprintf(query,"delete from %s.backup_tbl where cid=%s and bck_id='%s'",varGet(GVAR_DEPUSER),varGet(GVAR_CLUCID),f_bck_id);
      rc=DEPOquery(query,0);
      DEPOqueryEnd();
      sprintf(query,"delete from %s.backup_tbs where cid=%s and bck_id='%s'",varGet(GVAR_DEPUSER),varGet(GVAR_CLUCID),f_bck_id);
      rc=DEPOquery(query,0);
      DEPOqueryEnd();
      sprintf(query,"delete from %s.backup_wals where cid=%s and bck_id='%s'",  varGet(GVAR_DEPUSER),varGet(GVAR_CLUCID),f_bck_id);
      rc=DEPOquery(query,0);
      DEPOqueryEnd();
      sprintf(query,"delete from %s.backups where cid=%s and bck_id='%s'",      varGet(GVAR_DEPUSER),varGet(GVAR_CLUCID),f_bck_id);
      rc=DEPOquery(query,0);
      DEPOqueryEnd();
   };
   DEPOqueryEnd();
   
   // May be we have to cleanup some old restore point
   if (opt_noremove == false)
   {   
      sprintf(query,"with lfull as (select min(bdate) as fulldate from %s.backups where cid=%s and bcksts=0 and bcktyp='FULL')"
                    "delete from %s.backup_rp where crdate < (select fulldate from lfull)",
                    varGet(GVAR_DEPUSER),
                    varGet(GVAR_CLUCID),
                    varGet(GVAR_DEPUSER));
      rc=DEPOquery(query,0);
      DEPOqueryEnd();
   }
   globalArgs.verbosity=saved_verbose;
   memEndModule();
}
