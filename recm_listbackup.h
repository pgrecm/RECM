/**********************************************************************************/
/* LIST BACKUP command                                                            */
/* Usage:                                                                         */
/*    list backup                                                                 */                 
/*      Available Option(s):                                                      */
/*         /[no]meta                 List or exclude METADATA backups             */
/*         /[no]wal                  List or exclude WAL backups                  */
/*         /[no]full                 List or exclude FULL backups                 */
/*         /[no]cfg                  List or exclude CONFIG backups               */
/*         /locked                   List locked backups only                     */
/*         /noindex                  List FULL backups with '/noindex' option     */
/*         /available                List AVAILABLE backups                       */
/*         /incomplete               List INCOMPLETE backups                      */
/*         /obsolete                 List OBSOLETE backups                        */
/*         /verbose                                                               */
/*      Available Qualifier(s):                                                   */
/*         /cid=<STRING>              use a backup from a different cluster       */
/*         /directory=<PATH>          New PGDATA to create                        */
/*         /uid=<STRING>              FULL Backup UID to use for the restore      */
/*         /lsn=<STRING>              Recover until a specified LSN               */
/*         /newname=<STRING>          Give a name of the restored engine          */
/*         /rp=<STRING>               Recover to a given restore point            */
/*         /port=<STRING>             Restore engine port used                    */
/*         /until=<DATE>              Restre until a given date                   */
/*         /lsn=<STRING>              name of the restore point to delete         */
/**********************************************************************************/
void COMMAND_LISTBACKUP(int idcmd,char *command_line)
{
   if (DEPOisConnected() == false) 
   {
      ERROR(ERR_NOTCONNECTED,"Not connected to any deposit.\n");
      return;
   }
   if ((CLUisConnected() == false) && 
        qualifierIsUNSET("qal_source") == true && 
        qualifierIsUNSET("qal_cid") == true)
   {
      ERROR(ERR_NOTCONNECTED,"Not connected to any cluster.\n");
      return;
   };

   memBeginModule();
   // Change verbosity
   int opt_verbose=optionIsSET("opt_verbose");
   int saved_verbose=globalArgs.verbosity;
   globalArgs.verbosity=opt_verbose;

   long cluster_id;
   char *cluster_name=memAlloc(128); 

   if (CLUisConnected() == true)
   {
      if (strcmp(varGet(GVAR_CLUCID),VAR_UNSET_VALUE) != 0)  cluster_id=varGetLong(GVAR_CLUCID);
      if (strcmp(varGet(GVAR_CLUNAME),VAR_UNSET_VALUE) != 0) strcpy(cluster_name,varGet(GVAR_CLUNAME));
   }
   else
   {
      cluster_id=-1;
      strcpy(cluster_name,"(null)");
   };
   char *query=memAlloc(1024);
   char *qry_before=memAlloc(128); qry_before[0]=0x00; 
   char *qry_after=memAlloc(128);  qry_after[0]=0x00; 
   char *qry_tag=memAlloc(128);    qry_tag[0]=0x00; 
   char *qry_id=memAlloc(128);     qry_id[0]=0x00; 

   if (qualifierIsUNSET("qal_source") == false)
   {
      sprintf(query,"select count(*) from %s.clusters where clu_nam='%s'",
                    varGet(GVAR_DEPUSER),
                    varGet("qal_source"));
      int rows=DEPOquery(query,0);
      if (DEPOgetInt(0,0) == 0)
      {
         ERROR(ERR_BADCLUNAME,"Unknown cluster source name '%s'•\n",varGet("qal_source"));
         DEPOqueryEnd();
         memEndModule();
         return;
      }
      DEPOqueryEnd();
      sprintf(query,"select CID from %s.clusters where clu_nam='%s'",
                       varGet(GVAR_DEPUSER),
                       varGet("qal_source"));
      rows=DEPOquery(query,0);
      cluster_id=DEPOgetLong(0,0);
      DEPOqueryEnd();
      strcpy(cluster_name,varGet("qal_source"));
      varAdd("qal_cid",QAL_UNSET);                                                                                                 // Take care to not try to use /CID after /SOURCE
   }
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
      cluster_id=varGetLong("qal_cid");
      DEPOqueryEnd();
   };
   
   if (IsClusterEnabled(cluster_id) == false)
   {
      WARN(WRN_CLUDISABLED,"Cluster '%s' is 'DISABLED'.\n",cluster_name);
   }
   
   if (qualifierIsUNSET("qal_before") == false)
   { sprintf(qry_before," and bdate < to_timestamp('%s','%s')",varGet("qal_before"),varGet(GVAR_CLUDATE_FORMAT)); };

   if (qualifierIsUNSET("qal_after") == false)
   { sprintf(qry_after," and bdate > to_timestamp('%s','%s')",varGet("qal_after"),varGet(GVAR_CLUDATE_FORMAT)); };

   if (qualifierIsUNSET("qal_tag") == false)
   { sprintf(qry_tag," and lower(bcktag) like lower('%%%s%%')",varGet("qal_tag")); }; 

   sprintf(query,"select bcktyp,bck_id,bcksts,case bcksts when 0 then 'AVAILABLE' when 1 then 'OBSOLETE' when 2 then 'INCOMPLETE' when 3 then 'RUNNING' else 'FAILED' end,bcktag,hstname,"
                 "       pgdata,pgversion,timeline,options,bcksize,pcount,"
                 "       coalesce(to_char(bdate,'%s'),'0000-00-00 00:00:00'),coalesce(bwall,''),coalesce(bwalf,''),"
                 "       coalesce(to_char(edate,'%s'),'0000-00-00 00:00:00'),coalesce(ewall,''),coalesce(ewalf,''),"
                 "       trunc(coalesce(EXTRACT(EPOCH FROM (edate - bdate)),0)) from %s.backups  where cid=%ld",
                  varGet(GVAR_CLUDATE_FORMAT),
                  varGet(GVAR_CLUDATE_FORMAT),
                  varGet(GVAR_DEPUSER),
                  cluster_id);
   char *accept_bck=memAlloc(30);accept_bck[0]=0x00;
   char *reject_bck=memAlloc(30);reject_bck[0]=0x00;
   int accept_used=0;
   int reject_used=0;
   if (optionIsSET("opt_full")   == true) { if (accept_used == 0) {strcpy(accept_bck," and bcktyp in ('FULL'"); } else { strcat(accept_bck,",'FULL'"); }; accept_used++;}; 
   if (optionIsSET("opt_cfg")    == true) { if (accept_used == 0) {strcpy(accept_bck," and bcktyp in ('CFG'");  } else { strcat(accept_bck,",'CFG'");  }; accept_used++;}; 
   if (optionIsSET("opt_wal")    == true) { if (accept_used == 0) {strcpy(accept_bck," and bcktyp in ('WAL'");  } else { strcat(accept_bck,",'WAL'");  }; accept_used++;}; 
   if (optionIsSET("opt_meta")   == true) { if (accept_used == 0) {strcpy(accept_bck," and bcktyp in ('META'"); } else { strcat(accept_bck,",'META'"); }; accept_used++;}; 
   if (accept_used > 0) { strcat(accept_bck,")"); };

   if (optionIsSET("opt_nofull") == true) { if (reject_used == 0) {strcpy(reject_bck," and bcktyp not in ('FULL'"); } else { strcat(reject_bck,",'FULL'"); }; reject_used++;};
   if (optionIsSET("opt_nocfg")  == true) { if (reject_used == 0) {strcpy(reject_bck," and bcktyp not in ('CFG'");  } else { strcat(reject_bck,",'CFG'");  }; reject_used++;};
   if (optionIsSET("opt_nowal")  == true) { if (reject_used == 0) {strcpy(reject_bck," and bcktyp not in ('WAL'");  } else { strcat(reject_bck,",'WAL'");  }; reject_used++;}; 
   if (optionIsSET("opt_nometa") == true) { if (reject_used == 0) {strcpy(reject_bck," and bcktyp not in ('META'"); } else { strcat(reject_bck,",'META'"); }; reject_used++;}; 
   if (reject_used > 0) { strcat(reject_bck,")"); };

   strcat(query,accept_bck);
   strcat(query,reject_bck);
   strcat(query,qry_tag);
   strcat(query,qry_before);
   strcat(query,qry_after);
   strcat(query,qry_id);
   char *status_bck=memAlloc(30);
   status_bck[0]=0x00;
   int status_used=0;
   if (optionIsSET("opt_available")  == true) { if (status_used == 0) {strcpy(status_bck," and bcksts in (0"); } else { strcat(status_bck,",0"); }; status_used++;};
   if (optionIsSET("opt_obsolete")   == true) { if (status_used == 0) {strcpy(status_bck," and bcksts in (1"); } else { strcat(status_bck,",1"); }; status_used++;};
   if (optionIsSET("opt_incomplete") == true) { if (status_used == 0) {strcpy(status_bck," and bcksts in (2"); } else { strcat(status_bck,",2"); }; status_used++;};
   if (status_used > 0) { strcat(status_bck,")"); };
   strcat(query,status_bck);
   status_bck[0]=0x00;
   if (optionIsSET("opt_locked")     == true) { sprintf(status_bck," and ((options & %d) = %d)",BACKUP_OPTION_KEEPFOREVER,BACKUP_OPTION_KEEPFOREVER); };
   if (optionIsSET("opt_noindex")    == true) { sprintf(status_bck," and ((options & %d) = %d)",BACKUP_OPTION_NOINDEX,BACKUP_OPTION_NOINDEX); };
   strcat(query,status_bck);
   
   strcat(query," order by bdate");
    
   char *f_bcktyp=memAlloc(20);
   char *f_bck_id=memAlloc(20);
   char *f_bcksts=memAlloc(20);
   char *f_bcktag=memAlloc(256);
   char *f_hstname=memAlloc(256);
   char *f_pgdata=memAlloc(1024);
   char *f_pgversion=memAlloc(24);
   char *f_pgtimeline=memAlloc(24);
   long bck_options;             /* Change : 0xA000A */
   char *f_bcksize=memAlloc(60);
   char *f_pcount=memAlloc(60);
   char *f_bdate=memAlloc(60);
   char *f_bwall=memAlloc(60);
   char *f_bwalf=memAlloc(60);
   char *f_edate=memAlloc(60);
   char *f_ewall=memAlloc(60);
   char *f_ewalf=memAlloc(60);
   long f_elaps=0;
   
   long bck_full=0;
   long bck_wal=0;
   long bck_cfg=0;
   long bck_meta=0;
   
   long bck_stat_available=0;
   long bck_stat_obsolete=0;
   long bck_stat_incomplete=0;
   long bck_stat_running=0;
   long bck_stat_failed=0;
   int bcksts_n;
   printf("List backups of cluster '%s' (CID=%ld)\n",cluster_name,cluster_id);
   int rows=DEPOquery(query,0);
   for (int n=0;n < rows;n++)
   {
      strcpy(f_bcktyp,      DEPOgetString(n,0));
      strcpy(f_bck_id,      DEPOgetString(n,1));
      bcksts_n=             DEPOgetInt(n,2);
      strcpy(f_bcksts,      DEPOgetString(n, 3));
      strcpy(f_bcktag,      DEPOgetString(n, 4));
      strcpy(f_hstname,     DEPOgetString(n, 5));
      strcpy(f_pgdata,      DEPOgetString(n, 6));
      strcpy(f_pgversion,   DEPOgetString(n, 7));
      strcpy(f_pgtimeline,  DEPOgetString(n, 8));
      bck_options=DEPOgetLong(n, 9); /* Change : 0xA000A */
      strcpy(f_bcksize,     DEPOgetString(n,10));
      strcpy(f_pcount,      DEPOgetString(n,11));
      strcpy(f_bdate,       DEPOgetString(n,12));
      strcpy(f_bwall,       DEPOgetString(n,13));
      strcpy(f_bwalf,       DEPOgetString(n,14));
      strcpy(f_edate,       DEPOgetString(n,15));
      strcpy(f_ewall,       DEPOgetString(n,16));
      strcpy(f_ewalf,       DEPOgetString(n,17));
      f_elaps=DEPOgetLong(n,18);
      if (strcmp(f_bcktyp,RECM_BCKTYP_FULL) == 0) { bck_full++; };
      if (strcmp(f_bcktyp,RECM_BCKTYP_WAL)  == 0) { bck_wal++; };
      if (strcmp(f_bcktyp,RECM_BCKTYP_CFG)  == 0) { bck_cfg++; };
      if (strcmp(f_bcktyp,RECM_BCKTYP_META) == 0) { bck_meta++; };
      switch( bcksts_n)
      {
         case RECM_BACKUP_STATE_AVAILABLE  : bck_stat_available++;break;
         case RECM_BACKUP_STATE_OBSOLETE   : bck_stat_obsolete++ ;break;
         case RECM_BACKUP_STATE_INCOMPLETE : bck_stat_incomplete++;break;
         case RECM_BACKUP_STATE_RUNNING    : bck_stat_running++;break;
         case RECM_BACKUP_STATE_FAILED     : bck_stat_failed++;break;
      }

      if (globalArgs.verbosity == true)
      {
         // Verbose dsplay mode
         char opts[10];                                                                             // Build Options to display
         if ((bck_options & BACKUP_OPTION_COMPRESSED)  == BACKUP_OPTION_COMPRESSED)  { opts[0]='C'; } else { opts[0]='_';};
         if ((bck_options & BACKUP_OPTION_NOINDEX)     == BACKUP_OPTION_NOINDEX)     { opts[1]='I'; } else { opts[1]='_';};
         if ((bck_options & BACKUP_OPTION_EXCLUSIVE)   == BACKUP_OPTION_EXCLUSIVE)   { opts[2]='E'; } else { opts[2]='_';};
         if ((bck_options & BACKUP_OPTION_KEEPFOREVER) == BACKUP_OPTION_KEEPFOREVER) { opts[3]='L'; } else { opts[3]='_';};

         opts[4]=0x00;                                                           /* free */
         opts[5]=0x00;                                                           /* free */
         if (n == 0)
         {
            printf("%-22s %-20s %-6s %-12s %-4s %-4s %5s %12s %10s %s\n","UID","Date","Type","Status","TL","Opts","Cnt","Size ","Elaps(Sec)","Tag");
            printf("%-22s %-20s %-6s %-12s %-4s %-4s %5s %12s %10s %s\n","----------------------","--------------------","------","------------","----","----","-----","----------","----------","-------------------------");
         };
         printf("%-22s %-20s %-6s %-12s %-4s %4s %5ld %12s %10s %s\n",f_bck_id,f_bdate,f_bcktyp,f_bcksts,f_pgtimeline,opts,atol(f_pcount),DisplayprettySize(atol(f_bcksize)),DisplayElapsed(f_elaps),f_bcktag);
      }
      else
      {
         // Short dsplay mode
         if (n == 0)
         {
            printf("%-22s %-20s %-6s %-12s %-4s %12s %s\n","UID","Date","Type","Status","TL","Size ","Tag");
            printf("%-22s %-20s %-6s %-12s %-4s %12s %s\n","----------------------","--------------------","------","------------","----","----------","-------------------------");
         };
         printf("%-22s %-20s %-6s %-12s %-4s %12s %s\n",f_bck_id,f_bdate,f_bcktyp,f_bcksts,f_pgtimeline,DisplayprettySize(atol(f_bcksize)),f_bcktag);
      };
   }
   DEPOqueryEnd();
   
/*
if (globalArgs.verbosity == true)
   {
      printf("Statistics\n");
      printf(" Backup:      FULL=%-6ld        WAL=%-6ld   CONFIG=%ld  META=%ld\n",bck_full,bck_wal,bck_cfg,bck_meta);
      printf(" State:  AVAILABLE=%-6ld INCOMPLETE=%-6ld OBSOLETE=%ld\n",bck_stat_available, bck_stat_incomplete,bck_stat_obsolete);
      printf("           RUNNING=%-6ld     FAILED=%-6ld\n",bck_stat_running,bck_stat_failed);
   }
*/

   varAdd("qal_source",VAR_UNSET_VALUE);

   memEndModule();
   // Restore verbosity
   globalArgs.verbosity=saved_verbose;
}
