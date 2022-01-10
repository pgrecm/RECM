/**********************************************************************************/
/* Backup CONFIGURATION command                                                   */
/* Usage:                                                                         */
/*    backup config                                                               */
/*      Available Option(s):                                                      */
/*         /verbose                                                               */
/*      Available Qualifier(s):                                                   */
/*         /tag=<String>             Define custom keyword to backup.             */
/*                                                                                */
/**********************************************************************************/
void COMMAND_BACKUPCFG(int idcmd,char *command_line)
{
   long BACKUP_OPTIONS=0;
   
   if (isConnectedAndRegistered() == false) return;

   if (IsClusterEnabled(varGetLong(GVAR_CLUCID)) == false) 
   {
      WARN(ERR_SUSPENDED,"Cluster '%s' is 'DISABLED'. Operation ignored.\n",varGet(GVAR_CLUNAME));
      return;
   }

   if (varGetInt(GVAR_COMPLEVEL) > -1){ BACKUP_OPTIONS=backupSetOption(BACKUP_OPTIONS,BACKUP_OPTION_COMPRESSED); };   /* Change : 0xA000A */
   if (optionIsSET("opt_lock"))       { BACKUP_OPTIONS=backupSetOption(BACKUP_OPTIONS,BACKUP_OPTION_KEEPFOREVER);  }; /* Change : 0xA000A */
   
   if (varIsSET(GVAR_BACKUPDIR) == false)
   {
      ERROR(ERR_NOBACKUPDIR,"Backup directory not set.(See show cluster);\n");
      return;
   }
   memBeginModule();

   char *destination_folder=memAlloc(1024);
   varTranslate(destination_folder,varGet(GVAR_BACKUPDIR),1);
   
   if (directory_exist(destination_folder) == false)
   { 
      mkdirs(destination_folder,maskTranslate(varGet(GVAR_CLUMASK_BACKUP) )); 
   }
   if (directory_exist(destination_folder) == false)
   {
      ERROR(ERR_INVDIR,"Invalid Backup directory '%s'\n",destination_folder);
      memEndModule();
      return;
   }
   int opt_verbose=optionIsSET("opt_verbose");
   int saved_verbose=globalArgs.verbosity;
   globalArgs.verbosity=opt_verbose;

   varAdd(RECM_BCKTYPE,RECM_BCKTYP_CFG);
   
    
   long TL=pgGetCurrentTimeline();
   char *tag=memAlloc(128);
   if (qualifierIsUNSET("qal_tag") == false)
   {
      char *qtag=asLiteral(varGet("qal_tag"));
      strcpy(tag,qtag);
   }
   else
   { varTranslate(tag,"{YYYY}{MM}{DD}_{CluName}_CFG",0); }

    if (isValidName(tag) == false)
   {
      ERROR(ERR_INVCHARINNAME,"Invalid character in tag '%s'\n",tag);
      memEndModule();
      return;
   };
                                
   char *CfgFile=memAlloc(1024);
   sprintf(CfgFile,"%s/postgresql.conf",varGet(GVAR_CLUPGDATA));
   long total_backup_size=0;
   long total_backup_files=0;
   struct stat st;
   FILE *fh=fopen(CfgFile, "r");
   if (fh == NULL) 
   { 
      int rc=errno;
      switch(rc)
      {
         case EACCES : ERROR(ERR_FILNOACCESS,"Cannot acces configuration file(rc=%d)\n",rc); break;
         default     : ERROR(ERR_FILNOTFND,  "Configuration '%s' file not found(rc=%d)\n",CfgFile,rc);    break;
      };
      memEndModule();
      return;
   };
   RECMClearPieceList();
   createSequence(1);                                                           // Reset Piece sequence to 1
   createUID();                                                                 // Create UID      
  
   char *prm=memAlloc(128);
   char *val=memAlloc(1024);
   char *wrkfile = memAlloc(1024);
   char *query=memAlloc(1024);

   sprintf(wrkfile,"%s/postgresql.auto.conf",varGet(GVAR_CLUPGDATA));
   if (file_exists(wrkfile)==true) 
   {
      total_backup_files++;
      stat(wrkfile, &st);
      total_backup_size+=st.st_size;
      RECMaddFile(getFileNameOnly(wrkfile),wrkfile,varGet(GVAR_CLUPGDATA));
      VERBOSE("Add file '%s'\n",wrkfile);
   };

   char* buffer = memAlloc(1024);
   while (fgets(buffer, 1023, fh) != NULL)
   {
      char *line=buffer;
      if (strcmp(line,"\n") == 0) continue;
      if (strchr(line,'#') == line) continue;
      while (line[0] == ' ' || line[0] == '\t') { line++; };
      char *sign=strchr(line,'=');
      if (sign == NULL) continue;                                                  // Incomplete line.. Just ignore it 
      char *left=sign;
      while (strchr("\t =",left[0]) != NULL) { left--; };
      left++;
      left[0]=0x00;
      strcpy(prm,line);
      sign++;
      while (strchr("\t '",sign[0]) != NULL) { sign++; };
      char *right=strchr(sign,'#');
      if (right == NULL) right=strchr(sign,'\n');
      if (right != NULL) { right--; 
                           while (strchr(" \t",right[0]) != NULL) { right--; };
                           while (right[0] == '\'') { right--; };
                           right++;
                           right[0]=0x00;
                         }
      strcpy(val,sign);
      if (strstr("ident_file|hba_file|ssl_ca_file|ssl_cert_file|ssl_dh_params_file|",prm) != NULL)
      {
         if (strchr(val,'/') != NULL) { strcpy(wrkfile,val); }
                                 else { sprintf(wrkfile,"%s/%s",varGet(GVAR_CLUPGDATA),val); };
         if (file_exists(wrkfile)==true) 
         { 
            total_backup_files++;
            stat(wrkfile, &st);
            total_backup_size+=st.st_size;
            RECMaddFile(getFileNameOnly(wrkfile),wrkfile,varGet(GVAR_CLUPGDATA));
            VERBOSE("Add file '%s'\n",wrkfile);
         }
      };
   }
   fclose(fh);
   sprintf(wrkfile,"%s/postgresql.conf",varGet(GVAR_CLUPGDATA));
   total_backup_files++;
   stat(wrkfile, &st);
   total_backup_size+=st.st_size;
   VERBOSE("Add file '%s'\n",wrkfile);
   RECMaddFile(getFileNameOnly(wrkfile),wrkfile,varGet(GVAR_CLUPGDATA));
   
   RECMclose(total_backup_size,total_backup_files);

   // Load special information for restore
   int rcx=CLUquery("select setting from pg_settings where name='log_timezone'",0);
   varAdd(GVAR_CLUTZ,CLUgetString(0,0));
   CLUqueryEnd();
   rcx=CLUquery("select setting from pg_settings where name='DateStyle'",0);
   varAdd(GVAR_CLUDS,CLUgetString(0,0));
   CLUqueryEnd();
   TRACE("set BACKUPDIR='%s'\n",destination_folder);
   sprintf(query,"insert into %s.backups (cid,bcktyp,bcksts,bck_id,bcktag,hstname,pgdata,pgversion,"
                                         "timeline,options,bcksize,pcount,ztime,sdate,bckdir)"
                        " values (%s,'%s',0,'%s','%s','%s','%s','%s',%ld,%ld,%ld,%ld,'%s','%s','%s')",
                          varGet(GVAR_DEPUSER),
                          varGet(GVAR_CLUCID),
                          varGet(RECM_BCKTYPE),
                          varGet(RECM_BCKID),
                          tag,
                          varGet(GVAR_CLUHOST),
                          varGet(GVAR_CLUPGDATA),
                          varGet(GVAR_CLUVERSION),
                          pgGetCurrentTimeline(),
                          BACKUP_OPTIONS,
                          total_backup_size,
                          1L,
                          varGet(GVAR_CLUTZ),
                          varGet(GVAR_CLUDS),
                          destination_folder);
   int rows=DEPOquery(query,1);
   DEPOqueryEnd();
   // Update cluster record for last backup config date.
   sprintf(query,"update %s.clusters set lstcfg=(select max(bdate) from %s.backups b "
                 " where bcktyp='%s' and cid=%s) where cid=%s",
                   varGet(GVAR_DEPUSER),
                   varGet(GVAR_DEPUSER),
                   varGet(RECM_BCKTYPE),
                   varGet(GVAR_CLUCID),
                   varGet(GVAR_CLUCID));
   rows=DEPOquery(query,1);
   DEPOqueryEnd();
   
   INFO("Backup UID : %s\n",varGet(RECM_BCKID));
   INFO("Total pieces ......... : %-8ld\n",1L);
   INFO("Total files backuped.. : %-8ld\n",total_backup_files);
   INFO("Total Backup size .... : %-8ld (%s)\n",total_backup_size, DisplayprettySize(total_backup_size) );
      
   RECMsetProperties(varGet(RECM_BCKID));
   globalArgs.verbosity=saved_verbose;
   memEndModule();
}
