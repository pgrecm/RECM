/**********************************************************************************/
/* modify cluster command                                                         */
/* Usage:                                                                         */
/*      list wal                                                                  */
/*      options:                                                                  */
/*            /enable         Change cluster state                                */
/*            /disable        Change status to 'incomplete'                       */
/*            /obsolete       Change status to 'oboslete'                         */
/*            /lock           disable retention policy                            */
/*            /unlock         enable retention policy                             */
/*      Qualifiers:                                                               */
/*            /backupdir=<PATH>      Modify default backup location               */
/*            /blksize=<SIZE>        Modify allocation memory                     */
/*            /cfg=<RETENTION>       Modify configuration backup retention        */
/*            /compression=<LEVEL>   Change compression level ('-1'=disable)      */
/*            /cid=<CID>             Cluster ID to modify                         */
/*            /concurrently=<YES|NO> Recreate index concurrently at restore time  */
/*            /db=<STRING>           Change default database                      */
/*            /date_format=<STRING>  Change default date format                   */
/*            /delwal=<MINUTES>      Delay delete WAL after backup                */
/*            /description=<STRING>  Change Cluster description                   */
/*            /full=<RETENTION>      Cluster ID to modify                         */
/*            /ip=<STRING>           Change Cluster IP                            */
/*            /maxfiles=<CID>        Change backup piece file limit count         */
/*            /maxsize=<CID>         Change backup piece size limit               */
/*            /meta=<RETENTION>      Change METADATA backup retention             */
/*            /pwd=<CID>             Change Password of user to connect to cluster*/
/*            /port=<CID>            Change Cluster's port to connect             */
/*            /repopts=<CID>         Change repliation options                    */
/*            /reppwd=<CID>          Change repliation user's password            */
/*            /repusr=<CID>          Change repliation user                       */
/*            /usr=<CID>             Change user to connect to cluster            */
/*            /waldir=<CID>          Change WAL backup directory                  */
/**********************************************************************************/
void COMMAND_MODIFYCLUSTER(int idcmd,char *command_line)
{
   int cluster_is_local=true;
   if (DEPOisConnected() == false)
   {
      ERROR(ERR_NOTCONNECTED,"Not connected to any deposit.\n");
      return;
   }
   
   memBeginModule();
   
   // Change verbosity
   int opt_verbose=optionIsSET("opt_verbose");
   int saved_verbose=globalArgs.verbosity;
   globalArgs.verbosity=opt_verbose;

   
   int cluster_id;
   if (varExist(GVAR_CLUCID) == true && strcmp(varGet(GVAR_CLUCID),VAR_UNSET_VALUE) != 0) cluster_id=varGetInt(GVAR_CLUCID);

   char *query=memAlloc(1024);
   char *cluster_name=memAlloc(128); 

   if (qualifierIsUNSET("qal_name") == false)
   {
      sprintf(query,"select count(*) from %s.clusters where clu_nam='%s'",
                    varGet(GVAR_DEPUSER),
                    varGet("qal_name"));
      int rows=DEPOquery(query,0);
      if (DEPOgetInt(0,0) == 0)
      {
         ERROR(ERR_BADCLUNAME,"Unknown cluster source name '%s'\n",varGet("qal_name"));
         DEPOqueryEnd();
         memEndModule();
         return;
      }
      DEPOqueryEnd();
      sprintf(query,"select CID from %s.clusters where clu_nam='%s'",
                       varGet(GVAR_DEPUSER),
                       varGet("qal_name"));
      rows=DEPOquery(query,0);
      cluster_id=DEPOgetInt(0,0);
      DEPOqueryEnd();
      strcpy(cluster_name,varGet("qal_name"));
      cluster_is_local=false;
   }
   else
   {
      strcpy(cluster_name,varGet(GVAR_CLUNAME));
   };
   if (qualifierIsUNSET("qal_cid") == false)
   {
      sprintf(query,"select count(*) from %s.clusters where cid=%s",
                    varGet(GVAR_DEPUSER),
                    varGet("qal_cid"));
      int rows=DEPOquery(query,0);
      if (DEPOgetInt(0,0) == 0)
      {
         ERROR(ERR_BADCLUNAME,"Unknown cluster ID %s\n",varGet("qal_cid"));
         DEPOqueryEnd();
         memEndModule();
         return;
      }
      DEPOqueryEnd();      
      sprintf(query,"select clu_nam from %s.clusters where cid=%s",
                       varGet(GVAR_DEPUSER),
                       varGet("qal_cid"));
      rows=DEPOquery(query,0);
      strcpy(cluster_name,DEPOgetString(0,0));
      cluster_id=varGetInt("qal_cid");
      DEPOqueryEnd();
      cluster_is_local=false;
   };

   char virgule=' ';

   char *qry_where=memAlloc(512); 
   sprintf(qry_where," where cid=%d ",cluster_id);

   char *qry_state=memAlloc(1024); qry_state[0]=0x00;
   sprintf(query,"update %s.clusters set ",varGet(GVAR_DEPUSER));
   if (qualifierIsUNSET("qal_description") == false)
   {  sprintf(qry_state," clu_info='%s'",asLiteral(varGet("qal_description")));
      strcat(query,qry_state);
      virgule=',';
   };
   
   // option 'user'
   if (qualifierIsUNSET("qal_usr") == false)                                   // set cluster/user="xxxxxx"
   { 
      varAdd(GVAR_CLUUSER,varGet("qal_usr"));          
      globalArgs.configChanged=1; 
   };
   
   // option 'password'
   if (qualifierIsUNSET("qal_pwd") == false)                               // set cluster/password="xxxxxx"
   {
      varAdd(GVAR_CLUPWD,varGet("qal_pwd"));       
      globalArgs.configChanged=1; 
   };

   // option 'database'
   if (qualifierIsUNSET("qal_db") == false)                               // set cluster/database="xxx"
   { 
      varAdd(GVAR_CLUDB,varGet("qal_db"));        
      globalArgs.configChanged=1; 
   };

   // option 'compression'
   if (qualifierIsUNSET("qal_compression") == false)                           // set cluster/compression=[0-9]
   {
      if (varGetInt("qal_compression") < -1 || varGetInt("qal_compression") > 9) 
      {
         WARN(WRN_INVCOMPLVL,"Invalid compression level %s (Accepted values are '-1'(disable) and '0-9')\n",
                             varGet("qal_compression"));
      }
      varAdd(GVAR_COMPLEVEL,varGet("qal_compression"));
      sprintf(qry_state,"%c opt_compress=%d",virgule,varGetInt("qal_compression"));
      strcat(query,qry_state);
      virgule=',';
   }
   

   // option 'delwal'
   if (qualifierIsUNSET("qal_delwal") == false)                                  // modify cluster/delwal=<value>
   {  
      int opt_delwal=varGetLong("qal_delwal");
      if (opt_delwal < -1) opt_delwal=-1;                                        // -1 is for disable deltion
      if (opt_delwal > (60*24*7)) opt_delwal=(60*24*7);                          // Max Value is '1 WEEK'
      sprintf(qry_state,"%c opt_delwal=%d",virgule,opt_delwal);
      strcat(query,qry_state);
      sprintf(qry_state,"%d",opt_delwal);
      varAdd(GVAR_CLUAUTODELWAL,qry_state);
      virgule=',';
   };
   
   // option 'date_format'
   if (qualifierIsUNSET("qal_date_format") == false)                            // set cluster/date_format="yyyy-mm-dd hh:mi:ss"
   { 
      //
      sprintf(qry_state,"%c opt_datfmt='%s'",virgule,varGet("qal_date_format"));
      printf("datfmt='%s'\n",varGet("qal_date_format"));
      strcat(query,qry_state);
      virgule=',';
      varAdd(GVAR_CLUDATE_FORMAT,varGet("qal_date_format"));
      globalArgs.configChanged=1;
   };
   // option 'full'
   if (qualifierIsUNSET("qal_full") == false)
   {
      if (validRetention(varGet("qal_full")) == false)                          // set cluster/meta="[count|days]:<value>"
      { 
         ERROR(ERR_INVALIDFORMAT,"Invalid format '%s'. Should contain '<count|days>:<value>'\n",
                                 varGet("qal_full"));
      }
      else
      {
         sprintf(qry_state,"%c opt_retfull='%s'",virgule,varGet("qal_full"));
         strcat(query,qry_state);
         varAdd(GVAR_CLURETENTION_FULL,varGet("qal_full"));
         virgule=',';
      };
   };
   // option 'cfg'
   if (qualifierIsUNSET("qal_cfg") == false)
   {
      if (validRetention(varGet("qal_cfg")) == false)                          // set cluster/meta="[count|day]:<value>"
      { 
         ERROR(ERR_INVALIDFORMAT,"Invalid format '%s'. Should contain '<count|days>:<value>'\n",
                                 varGet("qal_cfg"));
      }
      else
      {
         sprintf(qry_state,"%c opt_retcfg='%s'",virgule,varGet("qal_cfg"));
         strcat(query,qry_state);
         varAdd(GVAR_CLURETENTION_CONFIG,varGet("qal_full"));
         virgule=',';
      };
   };
   
   // option 'meta'
   if (qualifierIsUNSET("qal_meta") == false)
   {
      if (validRetention(varGet("qal_meta")) == false)                          // set cluster/meta="[count|day]:<value>"
      { 
         ERROR(ERR_INVALIDFORMAT,"Invalid format '%s'. Should contain '<count|days>:<value>'\n",
                                 varGet("qal_meta"));
      }
      else
      {
         sprintf(qry_state,"%c opt_retmeta='%s'",virgule,varGet("qal_meta"));
         strcat(query,qry_state);
         varAdd(GVAR_CLURETENTION_META,varGet("qal_full"));
         virgule=',';
      };
   };
   // option 'maxsize'
   if (qualifierIsUNSET("qal_maxsize") == false)                                // modify cluster/maxsize=nM|G
   {
      TRACE("set MAXSIZE='%s'\n",varGet("qal_maxsize"));
      varAdd(GVAR_CLUBACKUP_MAXSIZE,varGet("qal_maxsize"));  
      sprintf(qry_state,"%c opt_maxsize='%s'",virgule,varGet("qal_maxsize"));
      strcat(query,qry_state);
      virgule=',';
   };
   // option 'maxfiles'
   if (qualifierIsUNSET("qal_maxfiles") == false)                               // modify cluster/maxfiles=<count>
   {
      TRACE("set MAXFILES='%s'\n",varGet("qal_maxfiles"));
      varAdd(GVAR_CLUBACKUP_MAXFILES,varGet("qal_maxfiles"));  
      sprintf(qry_state,"%c opt_maxfiles=%ld",virgule,varGetLong("qal_maxfiles"));
      strcat(query,qry_state);
      virgule=',';
   }

   // option 'concurrently' from version 12
   if (qualifierIsUNSET("qal_concurrently") == false)                           // modify cluster/concurrently=yes|no
   {
      TRACE("set CONCURRENTLY='%s'\n",varGet("qal_concurrently"));
      int cr=true;

      cr=isYESNOvalid(varGet("qal_concurrently"));
      if (cr == YESNO_BAD)
      {
         ERROR (ERR_BADVALUE," Invalid value '%s'.  Please choose between 'yes|no','on|off' or '1|0'.\n",varGet("qal_concurrently"));
      }
      else
      {
         varAddInt(GVAR_REINDEXCONCURRENTLY,cr);
         sprintf(qry_state,"%c opt_concurindex=%d",virgule,cr);
         strcat(query,qry_state);
         virgule=',';
      }
   }

   // option 'blksize'
   if (qualifierIsUNSET("qal_blksize") == false)                                // modify cluster/blksize=[yes/no]
   {
      char *siz=strdup(varGet("qal_blksize"));                                  // Accept '10M' / '10K' / 10G'
      char *unit;
      long bytes=atol(siz);
      unit=strchr(siz,'K');
      if (unit == NULL) unit=strchr(siz,'k');
      if (unit != NULL) { unit[0]=0x00; bytes=(bytes * 1024); };
      
      unit=strchr(siz,'M');
      if (unit == NULL) unit=strchr(siz,'m');
      if (unit != NULL) { unit[0]=0x00; bytes=(bytes * 1024* 1024); };

      unit=strchr(siz,'G');
      if (unit == NULL) unit=strchr(siz,'g');
      if (unit != NULL) { unit[0]=0x00; bytes=(bytes * 1024* 1024* 1024); };

      sprintf(qry_state,"%c opt_blksize=%ld",virgule,bytes);
      strcat(query,qry_state);
      sprintf(qry_state,"%ld",bytes);
      varAdd(GVAR_CLUREADBLOCKSIZE,qry_state);
      virgule=',';
   }

   if (qualifierIsUNSET("qal_backupdir") == false)
   { 
      int valid=true;
      if (cluster_is_local == true)
      {
         if (directory_exist(varGet("qal_backupdir")) == false) 
         { 
            mkdirs(varGet("qal_backupdir"),maskTranslate(varGet(GVAR_CLUMASK_BACKUP) )); 
         }
         if (directory_exist(varGet("qal_backupdir")) == false)
         {
            ERROR(ERR_DIRNOTFND,"Directory '%s' does not exist.\n",varGet("qal_backupdir"));
            valid=false;
         }
         if (directoryIsWriteable(varGet("qal_backupdir")) == false)
         {
            ERROR(ERR_BADDIRPERM,"Need write access into directory '%s'.\n",varGet("qal_backupdir"));
            valid=false;
         }
      }
      if (valid == true) 
      { 
         varAdd(GVAR_BACKUPDIR,varGet("qal_backupdir"));
         sprintf(qry_state,"%c opt_bkpdir='%s'",virgule,asLiteral(varGet("qal_backupdir")));
         strcat(query,qry_state);
         virgule=',';
      }
   };   

   if (qualifierIsUNSET("qal_waldir") == false)
   {
      if (cluster_is_local == true)
      {
         if (directory_exist(varGet("qal_waldir")) == false) 
         { 
            mkdirs(varGet("qal_waldir"),maskTranslate(varGet(GVAR_CLUMASK_BACKUP) )); 
         }
         if (directory_exist(varGet("qal_waldir")) == false)
         {
            ERROR(ERR_INVDIR,"Invalid WAL directory '%s'\n",varGet("qal_waldir"));
            memEndModule();
            return;
         }
      }
      varAdd(GVAR_WALDIR,varGet("qal_waldir"));     
      sprintf(qry_state,"%c opt_waldir='%s'",virgule,asLiteral(varGet(GVAR_WALDIR)));
      strcat(query,qry_state);
      virgule=',';
      INFO("WAL directory must correspond to the 'archive_command' definition.\n");
   };
   

   if (qualifierIsUNSET("qal_ip") == false)
   {
      sprintf(qry_state,"%c clu_addr='%s'",virgule,asLiteral(varGet("qal_ip")));
      strcat(query,qry_state);
      virgule=',';
   };

   if (qualifierIsUNSET("qal_port") == false)
   {  sprintf(qry_state,"%c clu_port='%s'",virgule,asLiteral(varGet("qal_port")));
      strcat(query,qry_state);
      virgule=',';
   };

   if (qualifierIsUNSET("qal_repopts") == false)
   {  sprintf(qry_state,"%c rep_opts='%s'",virgule,asLiteral(varGet("qal_repopts")));
      strcat(query,qry_state);
      virgule=',';
   };
   
   if (qualifierIsUNSET("qal_repusr") == false)
   {  sprintf(qry_state,"%c rep_usr='%s'",virgule,asLiteral(varGet("qal_repusr")));
      strcat(query,qry_state);
      INFO("Think this user have to be defined in 'pg_hba.conf' file.\n");
      virgule=',';
   };

   if (qualifierIsUNSET("qal_reppwd") == false)
   {  sprintf(qry_state,"%c rep_pwd='%s'",virgule,scramble(varGet("qal_reppwd")));
      strcat(query,qry_state);
      virgule=',';
   };   
   int state_changed=0;
   
   if (optionIsSET("opt_enable") == true && state_changed == 0)
   {  sprintf(qry_state,"%c clu_sts='%s'",virgule,CLUSTER_STATE_ENABLED);
      strcat(query,qry_state);
      virgule=',';
      state_changed++;
   };
   
   if (optionIsSET("opt_disable") == true && state_changed == 0)
   {  sprintf(qry_state,"%c clu_sts='%s'",virgule,CLUSTER_STATE_DISABLED);
      strcat(query,qry_state);
      virgule=',';
      state_changed++;
   };

   if (virgule == ' ')
   {
      WARN(WRN_NOCHANGE,"No change on cluster '%s'\n",cluster_name);
   }
   else
   {
      strcat(query,qry_where);
      int row=DEPOquery(query,0);
      if (DEPOrowCount() == 0)
      {
         ERROR(ERR_BACKUPNOTFND,"Cluster '%s' not found in DEPOSIT.\n",cluster_name);
         DEPOqueryEnd();
         memEndModule();
         return;
      }
      DEPOqueryEnd();
      if (qualifierIsUNSET("qal_description") == false  && varGetLong(GVAR_CLUCID) == cluster_id)
      {
         varAdd(GVAR_CLUDESCR,varGet("qal_description"));
         globalArgs.configChanged=1;
      }
   }
   if (globalArgs.configChanged == 1)
   {
      SaveConfigFile("CLU",varGet(GVAR_CLUNAME),SECTION_CLUSTER);
      INFO("Cluster '%s' changed.\n",cluster_name);
   }
   memEndModule();
   globalArgs.verbosity=saved_verbose;
   return;
};

   