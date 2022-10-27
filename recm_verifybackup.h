/**********************************************************************************/
/* verify backup command                                                          */
/* Usage:                                                                         */
/*    verify backup                                                               */
/*         /verbose                                                               */
/*         /before=<date>     Verify all backups before the specified date        */
/*         /after=<date>      Verify all backups after the specified date         */
/**********************************************************************************/
/*

@command verify backup
@definition
Validate/Verify backups folder content with DEPOSIT information.
This operation verify if all backup pieces are complete, and check if there is no GAP with the WAL files.

@option "/verbose" "Display more details"
@option "/uid=STRING"           "Verify a specific backup"
@option "/before=DATE"          "Verify all backups before the specified date"
@option "/after=DATE"           "Verify all backups after the specified date"

@example
@inrecm verify backup /verbose 
@out recm-inf: Verifying backups of cluster 'CLU12'
@out recm-inf: Backup '0001634875fe2b66ded0'. piece '/Volumes/pg_backups//0001634875fe2b66ded0_1_FULL.recm' present [204 object(s)]
@out ...
@out recm-inf: Backup '00016349a64029e1e4b0'. piece '/Volumes/pg_backups//00016349a64029e1e4b0_16_FULL.recm' present [225 object(s)]
@out recm-inf: Backup '00016349a65c0f23fa00'. piece '/Volumes/pg_backups//00016349a65c0f23fa00_1_WAL.recm' present [3 object(s)]
@out recm-inf: Verifying WAL sequence
@out recm-inf: Backup piece(s) state changed : 10 AVAILABLE, 0 OBSOLETE, 0 INCOMPLETE.
@inrecm
@end
 
*/
void RECMfileCheck_WALL(const char *bck_id,const char *recm_file)
{
   TRACE("Checking WAL backup piece '%s'\n",recm_file);
   RecmFile *rf=RECMGetProperties(recm_file);
   if (rf == NULL) return;
   
   //zip_t *zipHandle;
   RECMDataFile *hDF=RECMopenRead(recm_file);
   if (hDF == NULL) return;

   char *query=malloc(1024);

   struct zip_stat sb;
   //struct zip_file *zf;
   int i=0;
   long entries=RECMGetEntriesRead(hDF);
   TRACE("entries in ZIP : %ld\n",entries);
   while( i < entries)
   {  
      if (zip_stat_index(hDF->zipHandle, i, 0, &sb) == 0) 
      {
         //RecmFileItemProperties *rip=RECMGetFileProperties(&zipHandle,i);
         char *x=strstr(sb.name,"./");
         if (x != NULL) { x+=2; } else { x=(char *)&sb.name; }
         char *nn=x; 
         nn+=strlen(x);
         nn--;

         //VERBOSE("Checking WAL file '%s'\n",x);
         sprintf(query,"select count(*) from %s.backup_wals where cid=%s and bck_id='%s' and walfile='%s'",
                       varGet(GVAR_DEPUSER),
                       varGet(GVAR_CLUCID),
                       bck_id,
                       x);
         
         int rc=DEPOquery(query,0);
         int rec_is_present=DEPOgetInt(0,0);
         TRACE("Bakup UID '%s' : backup wals count=%d [rc=%d]\n",bck_id,rec_is_present,rc);
         //printf("Checking WAL:%s\nrec_is_present=%d\n\n",query,rec_is_present);
         DEPOqueryEnd();
         if (rec_is_present == 0)
         {
            sprintf(query,"insert into %s.backup_wals (cid,bck_id,walfile,ondisk,bckcount,edate)"
                          " values (%s,'%s','%s',1,1,to_timestamp(%lu))\n",
                          varGet(GVAR_DEPUSER),
                          varGet(GVAR_CLUCID),
                          bck_id,
                          x,
                          sb.mtime);
            //printf("Checking WAL:%s\n",query);
            int rc=DEPOquery(query,0);
            VERBOSE("Remembering WAL file '%s'\n",x);
            TRACE("Remembering WAL file '%s' [rc=%d]\n",x,rc);
            DEPOqueryEnd();
         }
      }
      i++;
   };
   free(query);
   RECMcloseRead(hDF);
};



void COMMAND_VERIFYBACKUP(int idcmd,char *command_line)
{
   if (isConnectedAndRegistered() == false) return;
   
   memBeginModule();

   INFO("Verifying backups of cluster '%s'\n",varGet(GVAR_CLUNAME));

   if (optionIsSET("opt_verbose") == true) globalArgs.verbosity=true;                                                              // Set Verbosity

   char *qry_before=memAlloc(128); qry_before[0]=0x00; 
   char *qry_after=memAlloc(128);  qry_after[0]=0x00; 
   char *qry_id=memAlloc(128);     qry_id[0]=0x00; 
   
   if (qualifierIsUNSET("qal_before") == false)
   { sprintf(qry_before," and edate < to_timestamp('%s','%s')",varGet("qal_before"),varGet(GVAR_CLUDATE_FORMAT)); };

   if (qualifierIsUNSET("qal_after") == false)
   { sprintf(qry_after," and edate > to_timestamp('%s','%s')",varGet("qal_after"),varGet(GVAR_CLUDATE_FORMAT)); };

   if (qualifierIsUNSET("qal_uid") == false)
   { sprintf(qry_id," and bck_id='%s'",varGet("qal_uid")); }; 
   char *query=memAlloc(1024);
   char *subqry=memAlloc(1024);

   char *backup_id=memAlloc(40);   
   int   backup_status;
   char *backup_date=memAlloc(30);   
   char *backup_type=memAlloc(20);   

   char *backup_piece_id=memAlloc(40);   
   char *backup_piece_name=memAlloc(1024);
   char *backup_location=memAlloc(1024);
   long backup_options;
   
   sprintf(query,"select bck_id,bcksts,bdate,bcktyp,bckdir,options,pcount from %s.backups where CID=%s",
                 varGet(GVAR_DEPUSER),
                 varGet(GVAR_CLUCID));
   strcat(query,qry_before);
   strcat(query,qry_after);
   strcat(query,qry_id);
   strcat(query," order by bdate");
   TRACE("query=%s\n",query);
   int changed_obsolete=0;
   int changed_available=0;
   int changed_incomplete=0;
   int rows=DEPOquery(query,0);
   TRACE("rows=%d\n",rows);
   for(int i=0; i < rows; i++)
   {
      int backup_is_complete=true;
      strcpy(backup_id,  DEPOgetString(i,0));
      backup_status=DEPOgetInt(i,1);
      TRACE("backup '%s' (%d) complete=%d\n",backup_id,backup_status,backup_is_complete);
      strcpy(backup_date,      DEPOgetString(i,2));
      strcpy(backup_type,      DEPOgetString(i,3));
      strcpy(backup_location,  DEPOgetString(i,4));
      backup_options=DEPOgetLong(i,5);
      int number_of_pieces=DEPOgetInt(i,6);
      TRACE("backup_location=%s\n",backup_location);
      if (directory_exist(backup_location) == false) 
      {
         WARN(WRN_DIRNOTFND,"Directory '%s' does not exist.\n",backup_location);
      }
      if (directoryIsWriteable(backup_location) == false) 
      {
         WARN(WRN_BADDIRPERM,"Do not have write access into directory '%s'.\n",backup_location);
      }
      sprintf(subqry,"select pcid,pname from %s.backup_pieces where cid=%s and bck_id='%s' order by pcid",
                     varGet(GVAR_DEPUSER),
                     varGet(GVAR_CLUCID),
                     backup_id);
      int rowsq = DEPOquery(subqry,1);
      if (rowsq ==0) backup_is_complete=false;                                  // Will be marked as incomplet if there is no piece !
      int j=0;
      while ( j < rowsq && backup_is_complete==true)
      {
         strcpy(backup_piece_id,                           DEPOgetString( j,0));
         sprintf(backup_piece_name,"%s/%s",backup_location,DEPOgetString( j,1));
         if (file_exists(backup_piece_name) == false)
         {
            backup_is_complete=false;
            VERBOSE("Backup '%s'. piece '%s' NOT available\n",backup_id,backup_piece_name); 
         }
         else
         {
            // Check if file is corrupted or not 
            long entries=0;
            RECMDataFile *hDF=RECMopenRead(backup_piece_name);
            if (hDF == NULL)
            {
               backup_is_complete=false; 
            }
            else
            {
               entries=RECMGetEntriesRead(hDF);
               int rcc=RECMcloseRead(hDF);
               if (entries == -1 || rcc == false) backup_is_complete=false;
            }
            if (backup_is_complete == true) VERBOSE("Backup '%s'. piece '%s' present [%ld object(s)]\n",backup_id,backup_piece_name,entries);
         };
         TRACE("backup_piece '%s' (%s) complete=%d\n",backup_piece_id,backup_piece_name,backup_is_complete);
         j++;
      }
      DEPOqueryEnd();
      TRACE("rows=%d number_of_pieces=%d\n",rowsq,number_of_pieces);
      if (rowsq != number_of_pieces) { backup_is_complete=false; };
      if (backup_is_complete == true)
      {
         sprintf(query,"update %s.backups set bcksts=%d where cid=%s and bck_id='%s'",
                       varGet(GVAR_DEPUSER),
                       RECM_BACKUP_STATE_AVAILABLE,
                       varGet(GVAR_CLUCID),
                       backup_id);
         int rc=DEPOquery(query,0);
         TRACE("updated %ld rows (available backups)[rc=%d]\n",DEPOrowCount(),rc);
         changed_available+=DEPOrowCount();
         DEPOqueryEnd();
         
         // For WAL backup, we can extract information to be sure that the 'backup_wal' table is up-to-date.
         if (strcmp(backup_type,RECM_BCKTYP_WAL) == 0)
         {
            RECMfileCheck_WALL(backup_id,backup_piece_name);
         }

      }
      else
      {
         sprintf(query,"update %s.backups set bcksts=%d where cid=%s and bck_id='%s'",
                       varGet(GVAR_DEPUSER),
                       RECM_BACKUP_STATE_INCOMPLETE,
                       varGet(GVAR_CLUCID),
                       backup_id);
         int rc=DEPOquery(query,0);
         changed_incomplete+=DEPOrowCount();
         TRACE("Update backup UID '%s' (INCOMPLETE) [rc=%d]\n",backup_id,rc);
         DEPOqueryEnd();
         if ((backup_options & BACKUP_OPTION_KEEPFOREVER) == BACKUP_OPTION_KEEPFOREVER)
         {
            WARN(WRN_LOCKEDNCOMPLETE,"Backup UID '%s' is incomplete (This backup was Locked)\n",backup_id);
         }
      };
   };
   DEPOqueryEnd();
   // Now, update all available backups to become obsolete
   //  Verify with '/DAYS=n' option
   // FULL and WAL backups
   //Retention Mode 'DAY'
   if (retentionMode(varGet(GVAR_CLURETENTION_FULL)) == RETENTION_MODE_DAYS)
   {
      sprintf(query,"update %s.backups set bcksts=%d where cid=%s and bcktyp in ('FULL','WAL') and bcksts<2 and ((options & 8)=0) and bck_id in "
                     "(select bck_id from %s.backups where (bdate < (now() - interval '%d days'))) "
                     "and "
                     " bdate < "
                     "(select coalesce(max(bd.bdate),now()) from %s.backups bd where cid=%s and bcktyp='FULL' and bcksts < 2"
                     " and bdate < (select min(crdate) from %s.backup_rp where rp_typ='P' and cid=%s))",
                     varGet(GVAR_DEPUSER),
                     RECM_BACKUP_STATE_OBSOLETE,
                     varGet(GVAR_CLUCID),
                     varGet(GVAR_DEPUSER),
                     retentionCount(varGet(GVAR_CLURETENTION_FULL)),
                     varGet(GVAR_DEPUSER),
                     varGet(GVAR_CLUCID),
                     varGet(GVAR_DEPUSER),
                     varGet(GVAR_CLUCID));
      int rc=DEPOquery(query,1);
      TRACE("Update backups (OBSOLETE) [rc=%d]\n",rc);
      changed_obsolete+=DEPOrowCount();
      TRACE("updated %ld rows (DAY mode:OBSOLETE backups)[rc=%d]\n",DEPOrowCount(),rc);
      DEPOqueryEnd();             
   }
   else
   {
      sprintf(query,"update %s.backups set bcksts=%d where cid=%s and bcktyp in ('FULL','WAL') and bcksts<2 and ((options & 8)=0) and "
                    "bdate < (select min(bdate) from"
                    "         (select min(res.bdate) as bdate FROM (select bdate from %s.backups "
                    "           where cid=%s and bcktyp='FULL' and bcksts=0 ORDER BY bdate desc limit %d) as res"
                    "          union"
                    "          select max(bd.bdate) from %s.backups bd where cid=%s and bcktyp='FULL' and bcksts < 2"
                    " and bdate < (select min(crdate) from %s.backup_rp where rp_typ='P' and cid=%s)) as r2)",
                    //select min(crdate) as bdate from %s.backup_rp  where cid=%s and rp_typ='P') as r2)",
                    varGet(GVAR_DEPUSER),
                    RECM_BACKUP_STATE_OBSOLETE,
                     varGet(GVAR_CLUCID), // 1
                    varGet(GVAR_DEPUSER),
                    varGet(GVAR_CLUCID),
                    retentionCount(varGet(GVAR_CLURETENTION_FULL)), // 4
                    varGet(GVAR_DEPUSER),
                    varGet(GVAR_CLUCID),
                    varGet(GVAR_DEPUSER),
                    varGet(GVAR_CLUCID));
      int rc=DEPOquery(query,1);
      TRACE("FULL/WAL updated %ld rows (COUNT mode:OBSOLETE backups)[rc=%d]\n",DEPOrowCount(),rc);
      changed_obsolete=DEPOrowCount();
      DEPOqueryEnd();
   }

   // CONFIG backups
   //Retention Mode 'DAY'
   if (retentionMode(varGet(GVAR_CLURETENTION_CONFIG)) == RETENTION_MODE_DAYS)
   {
      sprintf(query,"update %s.backups set bcksts=%d where cid=%s and bcktyp ='CFG' and bcksts=0 and ((options & 8)=0) and bck_id in "
                     "(select bck_id from %s.backups where cid=%s and bcktyp ='CFG' and (bdate < (now() - interval '%d days')))",
                     varGet(GVAR_DEPUSER),
                     RECM_BACKUP_STATE_OBSOLETE,
                     varGet(GVAR_CLUCID),
                     varGet(GVAR_DEPUSER),
                     varGet(GVAR_CLUCID),
                     retentionCount(varGet(GVAR_CLURETENTION_CONFIG))
                     );
      int rc=DEPOquery(query,1);
      changed_obsolete+=DEPOrowCount();
      TRACE("CFG updated %ld rows (DAY mode:OBSOLETE backups)[rc=%d]\n",DEPOrowCount(),rc);
      DEPOqueryEnd();
   }
   else
   {
     sprintf(query,"update %s.backups set bcksts=%d where cid=%s and bcktyp in ('CFG') and ((options & 8)=0) and bcksts=0 and "
                    "bdate < (select min(bdate) from"
                    "         (select min(res.bdate) as bdate FROM (select bdate from %s.backups "
                    "           where cid=%s and bcktyp='FULL' and bcksts=0 and ((options & 8)=0) ORDER BY bdate desc limit %d) as res"
                    "          union"
                    "          select min(crdate) as bdate from %s.backup_rp  where cid=%s and rp_typ='P') as r2)",
                    varGet(GVAR_DEPUSER),
                    RECM_BACKUP_STATE_OBSOLETE,
                     varGet(GVAR_CLUCID), // 1
                    varGet(GVAR_DEPUSER),
                    varGet(GVAR_CLUCID),
                    retentionCount(varGet(GVAR_CLURETENTION_CONFIG)), // 4
                    varGet(GVAR_DEPUSER),
                    varGet(GVAR_CLUCID));      
      int rc=DEPOquery(query,1);
      TRACE("CFG updated %ld rows (COUT mode:OBSOLETE backups)[rc=%d]\n",DEPOrowCount(),rc);
      changed_obsolete=DEPOrowCount();
      DEPOqueryEnd();
   }
   
   // METADATA backups
   //Retention Mode 'DAY'
   if (retentionMode(varGet(GVAR_CLURETENTION_META)) == RETENTION_MODE_DAYS)
   {
      sprintf(query,"update %s.backups set bcksts=%d where cid=%s and bcktyp ='META' and ((options & 8)=0) and bcksts=0 and bck_id in "
                     "(select bck_id from %s.backups where cid=%s and bcktyp ='META' and (bdate < (now() - interval '%d days')))",
                     varGet(GVAR_DEPUSER),
                     RECM_BACKUP_STATE_OBSOLETE,
                     varGet(GVAR_CLUCID),
                     varGet(GVAR_DEPUSER),
                     varGet(GVAR_CLUCID),
                     retentionCount(varGet(GVAR_CLURETENTION_META))
                     );
      int rc=DEPOquery(query,1);
      changed_obsolete+=DEPOrowCount();
      TRACE("META updated %ld rows (DAY mode:OBSOLETE backups)[rc=%d]\n",DEPOrowCount(),rc);
      DEPOqueryEnd();
   }
   else
   {
     sprintf(query,"update %s.backups set bcksts=%d where cid=%s and bcktyp in ('META') and ((options & 8)=0) and bcksts=0 and "
                    "bdate < (select min(bdate) from"
                    "         (select min(res.bdate) as bdate FROM (select bdate from %s.backups "
                    "           where cid=%s and bcktyp='FULL' and bcksts=0  ORDER BY bdate desc limit %d) as res"
                    "          union"
                    "          select min(crdate) as bdate from %s.backup_rp  where cid=%s and rp_typ='P') as r2)",
                    varGet(GVAR_DEPUSER),
                    RECM_BACKUP_STATE_OBSOLETE,
                     varGet(GVAR_CLUCID), // 1
                    varGet(GVAR_DEPUSER),
                    varGet(GVAR_CLUCID),
                    retentionCount(varGet(GVAR_CLURETENTION_META)), // 4
                    varGet(GVAR_DEPUSER),
                    varGet(GVAR_CLUCID));       
      int rc=DEPOquery(query,1);
      TRACE("META updated %ld rows (COUNT mode:OBSOLETE backups)[rc=%d]\n",DEPOrowCount(),rc);
      changed_obsolete=DEPOrowCount();
      DEPOqueryEnd();
   }
   // Looking for WAL GAP now
   sprintf(query,"select w.bck_id,w.walfile,b.bcksts"
              " from %s.backup_wals w,%s.backups b where w.cid=b.cid and w.cid=%s and w.bck_id=b.bck_id and b.bcksts<2"
              " and w.walfile not like '%s' and w.walfile not like '%s' and w.walfile not like '%s' order by w.walfile",
                 varGet(GVAR_DEPUSER),
                 varGet(GVAR_DEPUSER),
                 varGet(GVAR_CLUCID),
                 "%backup","%history","%partial");

   // If 'verify /uid=xxx is used, we don't check WAL sequence
   if (qualifierIsUNSET("qal_uid") == true)
   {
      INFO("Verifying WAL sequence\n");
      int wal_rows=DEPOquery(query,1);
      char *prev_bckid=memAlloc(30);
      char *bckid=memAlloc(30);
      prev_bckid[0]=0x00;
      bckid[0]=0x00;
      
      char *curWAL=NULL;
      char *prvWAL=memAlloc(64);prvWAL[0]=0x00;
      char *cw    =memAlloc(64);    cw[0]=0x00;
      for (int i=0;i < wal_rows;i++)
      {
         strcpy(prev_bckid,bckid);
         strcpy(bckid,DEPOgetString(i,0));
         if (curWAL != NULL && strstr(curWAL,"backup") == NULL) { strcpy(prvWAL,curWAL); };
         strcpy(cw,DEPOgetString(i,1));
         curWAL=cw;
         if (curWAL[0] == '.') cw++;
         if (curWAL[0] == '/') cw++;
         char *remove_ext=strstr(curWAL,".");
         if (remove_ext != NULL) remove_ext[0]=0x00;
         int bcksts=DEPOgetInt(i,2);
         int prvWAL_valid=1;
         if (strstr(prvWAL,"backup")  != NULL) prvWAL_valid=0;
         if (strstr(prvWAL,"history") != NULL) prvWAL_valid=0;
         
         int curWAL_valid=1;
         if (strstr(curWAL,"backup")  != NULL) curWAL_valid=0;
         if (strstr(curWAL,"history") != NULL) curWAL_valid=0;
         TRACE("Current WAL='%s'\n",curWAL);
         long cTL=getWALtimeline(prvWAL);
         long nTL=getWALtimeline(curWAL);
         if (i > 0 && curWAL_valid == 1 && prvWAL_valid == 1 && strcmp(curWAL,ComputeNextWAL(prvWAL)) != 0 && strcmp(prvWAL,curWAL) != 0)
         {
            if (nTL != cTL)
            {
               INFO("TIMELINE(TL) change between '%s' and '%s'.\n",prvWAL,curWAL);
            }
            else
            {
               switch(bcksts)
               {
               case RECM_BACKUP_STATE_OBSOLETE : 
                  INFO("GAP between '%s' and '%s'. (OBSOLETE backup).\n",prvWAL,curWAL);break;
               default:
                  WARN(WRN_GAPFOUND,"GAP between '%s' and '%s'. (AVAILABLE backup). Verify and Launch a FULL backup if necessary.\n",prvWAL,curWAL);
               }
            }
         };
      }
      DEPOqueryEnd();
   };
   INFO("Backup piece(s) state changed : %d AVAILABLE, %d OBSOLETE, %d INCOMPLETE.\n",changed_available,changed_obsolete,changed_incomplete);
   
   memEndModule();   
}
