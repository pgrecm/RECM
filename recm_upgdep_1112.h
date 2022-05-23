// Upgrade deposit from 1.1 to 1.2
// Variable 'query','rc' is allocated in main module 'recm_upgdeposit.h'

#define CHANGE_CLUSTERS_COL_MAXWSIZE  1
#define CHANGE_CLUSTERS_COL_MAXWFILES 2
#define CHANGE_CLUSTERS_COL_LSTMETA 4
   changes=0;
   
   printf("Checking upgrade from '%s' to '%s'\n",VERSION_11,VERSION_12);
   
   
   /* -1- Column 'opt_maxwsize' in clusters table */
   
   sprintf(query,"select count(*) from information_schema.columns where table_schema='%s'" 
                 " and table_name='clusters' and column_name='opt_maxwsize'",
                 varGet(GVAR_DEPUSER));
   rc=DEPOquery(query,0);
   if (rc < 0) 
   {
      ERROR(ERR_UPGRADEFAIL,"Upgrade to '%s' failed at (0xC001).\n",VERSION_12);
      DEPOqueryEnd();   
      memEndModule();
      return;
   }
   rc=DEPOgetInt(0,0);
   DEPOqueryEnd();   
   if (rc == 0)                                                                              // Column does not exist, change can be made
   { 
      changes=changes |  CHANGE_CLUSTERS_COL_MAXWSIZE;
      VERBOSE("0xC001 : Done\n");
   }
   else { VERBOSE("0xC001 : skipped\n"); };

   /* -2- Column 'opt_maxwfiles' in clusters table */
   sprintf(query,"select count(*) from information_schema.columns where table_schema='%s'" 
                 " and table_name='clusters' and column_name='opt_maxwfiles'",
                 varGet(GVAR_DEPUSER));
   rc=DEPOquery(query,0);
   if (rc < 0) 
   {
      ERROR(ERR_UPGRADEFAIL,"Upgrade to '%s' failed at (0xC002).\n",VERSION_12);
      DEPOqueryEnd();   
      memEndModule();
      return;
   }
   rc=DEPOgetInt(0,0);
   DEPOqueryEnd();   
   if (rc == 0)                                                                              // Column does not exist, change can be made
   { 
      changes=changes |  CHANGE_CLUSTERS_COL_MAXWFILES;
      VERBOSE("0xC002 : Done\n");
   }
   else { VERBOSE("0xC001 : skipped\n"); };

   /* - 3 - alter table clusters add lstmeta timestamp; */ 
   sprintf(query,"select count(*) from information_schema.columns where table_schema='%s'" 
                 " and table_name='clusters' and column_name='lstmeta'",
                 varGet(GVAR_DEPUSER));
   rc=DEPOquery(query,0);
   if (rc < 0) 
   {
      ERROR(ERR_UPGRADEFAIL,"Upgrade to '%s' failed at (0xC003).\n",VERSION_12);
      DEPOqueryEnd();   
      memEndModule();
      return;
   }
   rc=DEPOgetInt(0,0);
   DEPOqueryEnd();   
   if (rc == 0)                                                                              // Column does not exist, change can be made
   { 
      changes=changes |  CHANGE_CLUSTERS_COL_LSTMETA;
      VERBOSE("0xC003 : Done\n");
   }
   else { VERBOSE("0xC003 : skipped\n"); };

   if (optionIsSET("opt_apply") == true)
   {
      printf("Upgrading DEPOSIT from '%s' to '%s'\n",VERSION_11,VERSION_12);
      if ((changes & CHANGE_CLUSTERS_COL_MAXWSIZE) == CHANGE_CLUSTERS_COL_MAXWSIZE)
      {
         TRACE("Adding column 'opt_maxwsize' to clusters table\n");
         sprintf(query,"alter table %s.clusters add opt_maxwsize  varchar(20) default('4G');",varGet(GVAR_DEPUSER));
         rc=DEPOquery(query,0);
         DEPOqueryEnd();   
         if (rc < 0) 
         { 
            ERROR(ERR_UPGRADEFAIL,"Upgrade to '%s' failed at (0xA001).\n",VERSION_12);
            memEndModule();
            return;
         }
      };
      if ((changes & CHANGE_CLUSTERS_COL_MAXWFILES) == CHANGE_CLUSTERS_COL_MAXWFILES)
      {
         TRACE("Adding column 'opt_maxwfiles' to clusters table\n");
         sprintf(query,"alter table %s.clusters add opt_maxwfiles bigint  default(16);",varGet(GVAR_DEPUSER));
         rc=DEPOquery(query,0);
         DEPOqueryEnd();   
         if (rc < 0) 
         { 
            ERROR(ERR_UPGRADEFAIL,"Upgrade to '%s' failed at (0xA002).\n",VERSION_12);
            memEndModule();
            return;
         }
      }
      if ((changes & CHANGE_CLUSTERS_COL_LSTMETA) == CHANGE_CLUSTERS_COL_LSTMETA)
      {
         TRACE("Adding column 'lstmeta' to clusters table\n");
         sprintf(query,"alter table %s.clusters lstmeta timestamp",varGet(GVAR_DEPUSER));
         rc=DEPOquery(query,0);
         DEPOqueryEnd();   
         if (rc < 0) 
         { 
            ERROR(ERR_UPGRADEFAIL,"Upgrade to '%s' failed at (0xA003a).\n",VERSION_12);
            memEndModule();
            return;
         }
         sprintf(query,"update %s.clusters set lstmeta=NULL",varGet(GVAR_DEPUSER));
         rc=DEPOquery(query,0);
         DEPOqueryEnd();   
         if (rc < 0) 
         { 
            ERROR(ERR_UPGRADEFAIL,"Upgrade to '%s' failed at (0xA003b).\n",VERSION_12);
            memEndModule();
            return;
         }
      }
      // Update Version to config table.
      sprintf(query,"update %s.config set version='%s',updated=now(),description='RECM Deposit (PostgreSQL) Release %s'",varGet(GVAR_DEPUSER),VERSION_12,VERSION_12);
      rc=DEPOquery(query,0);
      DEPOqueryEnd();   
      if (rc < 0) 
      { 
         ERROR(ERR_UPGRADEFAIL,"Upgrade to '%s' failed at (0x0003).\n",VERSION_12);
         memEndModule();
         return;
      }
   }
   strcpy(curVersion,VERSION_12); // Upgraded to 1.2

