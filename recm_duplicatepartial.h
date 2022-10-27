/**********************************************************************************/
/* DUPLICATE FULL|PARTIAL command                                                 */
/* RESTORE FULL|PARTIAL command                                                   */
/* Usage:                                                                         */
/*    duplicate full                                                              */                 
/*      Available Option(s):                                                      */
/*         /norecover                 Do not start recovery after restore         */
/*         /standby                   Perform a restore/duplicate for a standby   */
/*         /inclusive                 Recover until last transaction included     */
/*         /noninclusive              Recover until last transaction excluded     */
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
/*         /vacuum="none"             Process vacuum after restore/duplicate      */
/*                                    none :                                      */
/*                                       skip vacuum(Default)                     */
/*                                    FULL :                                      */
/*                                       vacuum FULL                              */
/*                                    FREEZE :                                    */
/*                                       Aggressive “freezing” of tuples          */
/*                                    VERBOSE :                                   */
/*                                                        */
/*                                    ANALYZE :                                   */
/*                                                               */
/*                                    DISABLE_PAGE_SKIPPING:                      */
/*                                       Normally, VACUUM will skip pages based   */
/*                                       on the visibility map                    */
/*                                    SKIP_LOCKED:                                */
/*                                       Specifies that VACUUM should not wait    */
/*                                       for any conflicting locks                */
/*                                    INDEX_CLEANUP: AUTO|ON|OFF                  */
/*                                       Normally, VACUUM will skip index         */
/*                                       vacuuming when there are very few dead   */
/*                                       tuples in the table                      */
/*                                    PROCESS_TOAST:                              */
/*                                       process the corresponding TOAST table    */
/*                                       for each relation, if one exists         */
/*                                    TRUNCATE:                                   */
/*                                       attempt to truncate off any empty pages  */
/*                                       at the end of the table                  */
/**********************************************************************************/
/*
@command duplicate|restore full|partial
@definition
Perform a duplicate or a restore of a backup.
A partial restore can be limited to a database,a schema or a table.

@option "/norecover"                 "Do not start recovery after restore"
@option "/standby"                   "Perform a restore/duplicate for a standby"
@option "/inclusive"                 "Recover until last transaction included"
@option "/noninclusive"              "Recover until last transaction excluded"
@option "/verbose"                   "Display more details"
@option "/cid=STRING"                "use a backup from a different cluster"
@option "/directory=PATH"            "New PGDATA to create"
@option "/uid=STRING"                "FULL Backup UID to use for the restore"
@option "/lsn=STRING"                "Recover until a specified LSN"
@option "/newname=STRING"            "Give a name of the restored engine"
@option "/rp=STRING"                 "Recover to a given restore point"
@option "/port=STRING"               "Restore engine port used"
@option "/until=DATE"                "Restre until a given date"
@option "/lsn=STRING"                "name of the restore point to delete"
@option "/vacuum=NONE"               "Process vacuum after operation.<br>Value can be NONE|FULL|FREEZE|ANALYSE|DISABLE_PAGE_SKIPPING|SKIP_LOCKEDPROCESS_TOAST|TRUNCATE"
@example
@inrecmp duplicate partial /port=6666 /dir="/tmp/r6666" /verbose /db=demo /table="test*,cities*" 
@out recm-inf: Restore started at 2022-10-14 20:22:20
@out recm-inf: Restore until 'latest' possible transaction.
@out recm-inf: Restore will be done in folder '/tmp/r6666'
@out recm-inf: File System have 28567526 MB free. Backup will take 796 MB.
@out recm-inf: Restoring backup from UID '00016349a64029e1e4b0' (TL 36, Version 12, WAL '73/51000028')
@out recm-inf: WAL files are compressed (extension '.gz')
@out recm-inf: Recovery will stop just after the specified recovery target (recovery_target_inclusive is true)
@out recm-inf: Restore will read 17 pieces
@out recm-inf: [1/17] '/Volumes/pg_backups//00016349a64029e1e4b0_1_FULL.recm'
@out recm-inf: Selected table 'public'.'test' (database 'demo').
@out recm-inf: Selected table 'public'.'test_copy' (database 'demo').
@out recm-inf: Selected table 'public'.'cities' (database 'demo').
@out recm-inf: Selected table 'public'.'cities_2' (database 'demo').
@out recm-inf: Restoring piece '/Volumes/pg_backups//00016349a64029e1e4b0_1_FULL.recm' (204 files)
@out 100.00% ################################################################################
@out recm-inf: [2/17] '/Volumes/pg_backups//00016349a64029e1e4b0_2_FULL.recm'
@out recm-inf: Restoring piece '/Volumes/pg_backups//00016349a64029e1e4b0_2_FULL.recm' (203 files)
@out 100.00% ################################################################################
@out recm-inf: [3/17] '/Volumes/pg_backups//00016349a64029e1e4b0_3_FULL.recm'
@out recm-inf: Restoring piece '/Volumes/pg_backups//00016349a64029e1e4b0_3_FULL.recm' (202 files)
@out ...
@out recm-inf: [17/17] '/Volumes/pg_backups//00016349a65c0f23fa00_1_WAL.recm'
@out recm-inf: Restoring piece '/Volumes/pg_backups//00016349a65c0f23fa00_1_WAL.recm' (3 files)
@out 100.00% ################################################################################
@out recm-inf: Restore ended.
@out recm-inf:  - 81 directories created.
@out recm-inf:  - 2971 file(s) restored.
@out recm-inf:  - 55 file(s) skipped.
@out recm-inf: Starting recovery...
@out recm-inf: Recovery in progress (LSN '73/51000110')
@out recm-inf: Recovery in progress (LSN '73/52000000')
@out recm-inf: Recovery ended at WAL '000000250000007300000051', LSN '73/52000000'
@out recm-inf: Opening cluster instance...
@out recm-inf: Database successfully restored.
@out recm-inf: Connect using Host:port: localhost:6666
@out recm-inf: Clean up excluded definition...
@out NOTICE:  table "test_copy" has no indexes to reindex
@out NOTICE:  table "cities_2" has no indexes to reindex
@out recm-inf: Restore start time 2022-10-14 20:22:20
@out recm-inf:          finish at 2022-10-14 20:22:34
@out recm-inf: Elapsed time : 0 Min. 15 Sec.
@inrecm exit;
@inshell bin/psql -p 6666 demo    
@out psql (12.3)
@out Type "help" for help.
@out 
@out demo=# \d
@out            List of relations
@out  Schema |   Name    | Type  |  Owner   
@out --------+-----------+-------+----------
@out  public | cities    | table | postgres
@out  public | cities_2  | table | postgres
@out  public | test      | table | postgres
@out  public | test_copy | table | postgres
@out (4 rows)

@end
*/

#define FLAG_DATABASE   1
#define FLAG_SCHEMA     2
#define FLAG_TABLE      4
#define FLAG_PROCESSED  8
#define FLAG_SELECTED  16
#define FLAG_VACUUMED  32
#define FLAG_RESET      0
#define FLAG_ALL_SELECTED (FLAG_DATABASE + FLAG_SCHEMA + FLAG_TABLE)

long setFlag(long flag,long bit)
{
   if ((flag & bit) != bit)  return (flag^bit);
   return(flag);
}

long unsetFlag(long flag,long bit)
{
   if ((flag & bit) == bit) return(flag-bit);
   return(flag);
}



#define ITEM_EXCLUDED 'E'
#define ITEM_INCLUDED 'I'
#define ITEM_PROCESSED 'P'

#define ITEM_ALLOWDROPDB 'Y'
#define ITEM_KEEPDB 'N'
#define ITEM_DBDROPED 'D'

#define ITEM_DATABASE 'D'
#define ITEM_SCHEMA   'S'
#define ITEM_TABLE    'T'
#define ITEM_INDEX    'I'

#define ITR_STATE_RESET      0
#define ITR_STATE_SELECTED   2
#define ITR_STATE_PROCESSED  4

#define ITR_FLAG_UNCHANGED  0
#define ITR_FLAG_CHANGED    1

typedef struct ItemToRestore ItemToRestore;
struct ItemToRestore
{
   char structType;               // 'D'atabase
   long flag;                     //
   char candropdb;                // 'N' no / 'Y' yes
   unsigned long dbId;            // database ID
   char *        dbNam;           // database Name
   char *        sch;             // Schema Name
   char *        tbl;             // Table Name
   char *        tblfid;          // relfilenode (name of the file in the folder)
   unsigned long tbsoid;          // tablespace OID  
} ;

int validFilter(const char * filter)
{
   char *star_position=strchr(filter,'*');
   int rcx=0;
   while (star_position != NULL)
   {
      rcx++;
      star_position++;
      star_position=strchr(star_position,'*');
   };
   return(rcx);
}

#define CMPITEM_EQUAL 1
#define CMPITEM_DIFFERENT 0
#define CMPITEM_EQUAL_NEGATED 2
#define CMPITEM_DIFFERENT_NEGATED 4

int compareItem(const char *pattern,const char *nam)
{
   char *buffer=strdup(pattern);
   char *left=buffer;
   int INVERTED=0;
   if (left[0] == '-') { left++; INVERTED=1; };
   if (left[0] == '+') { left++; INVERTED=0; };
   char *right=strchr(left,'*');
   if (right != NULL)              // pattern contain '*'
   {
      right[0]=0x00;right++;
      //printf("'%s' with LEFT='%s' (right='%s')\n",nam,left,right);
      int match=strncmp(left,nam,strlen(left));
      if (match == 0)                          // Left part is the same
      {
          char *pnam=(char *)nam;
          if (strlen(right) > 0)               // Something behind the '*' ?
          {
             pnam+=strlen(nam) - strlen(right);
             //printf("RIGHT='%s' and pnam='%s'\n",right,pnam);
             match=strcmp(right,pnam);
          }
      }
     free(buffer);
      if (match == 0) { if (INVERTED == 0) { return(CMPITEM_EQUAL);} 
                                      else { return(CMPITEM_EQUAL_NEGATED); } 
                      };  
      if (INVERTED == 0) { return(CMPITEM_DIFFERENT); } 
                    else { return(CMPITEM_DIFFERENT_NEGATED); };
   }
   else
   {
      int match=strcmp(left,nam);
      free(buffer);
      if (match == 0) { if (INVERTED == 0) { return(CMPITEM_EQUAL);} 
                                      else { return(CMPITEM_EQUAL_NEGATED); } 
                      };
      if (INVERTED == 0) { return(CMPITEM_DIFFERENT); } 
                    else { return(CMPITEM_DIFFERENT_NEGATED); };
   }
}

int compareString(const char *objNam,const char *filter)
{
   int rcleft=1;
   int rcright=1;
   int accept=1; // Not equal
   char *star_position=strchr(filter,'*');
   if (star_position != NULL)
   {
      char *leftPart=strdup(filter);
      char *rightPart=strchr(leftPart,'*');
      rightPart[0]=0x00;rightPart++;

      if (strlen(leftPart) == 0) rcleft=0;   // Accept left part by default
      if (strlen(rightPart) == 0) rcright=0; // Accept right part by default
      if (strlen(leftPart) > 0 && strncmp(leftPart,objNam,strlen(leftPart)) == 0) { rcleft=0; };
      if (strlen(rightPart) > 0)
      {
         char *rightObj=(char *)objNam;
         rightObj+=(strlen(objNam) - strlen(rightPart));
         if (strcmp(rightObj,rightPart) == 0) { rcright=0; }; 
      }
      accept=rcleft+rcright;

      free(leftPart);
   }
   else
   {
      accept=strcmp(objNam,filter);
   }
   return(accept);
}
   
/* Clear array of ItemToRestore */
void clearArrayItemToRestore(Array *arrayList)
{
   if (arrayList == NULL) return;
   ArrayItem *itm=arrayFirst(arrayList);
   while (itm != NULL)
   {
       ItemToRestore *data=itm->data;
       if (data != NULL)
       {
          if (data->dbNam != NULL) { free(data->dbNam); };
          if (data->sch   != NULL) { free(data->sch); };
          if (data->tbl   != NULL) { free(data->tbl); };
          free(data);
       }
       itm->data=NULL;
       arrayDelete(arrayList,itm);
       itm=arrayFirst(arrayList);
   };
   free(arrayList);
}


Array *convertListToArray(const char *itemlist)
{
   Array *list=arrayCreate(ARRAY_UP_ORDER);
   if (strcmp(itemlist,QAL_UNSET) == 0) return(list);
   char *wrkstr_list=malloc(1024);
   strcpy(wrkstr_list,itemlist);
   strcat(wrkstr_list,",");
   char *itm=wrkstr_list;
   char *virgule=strchr(itm,',');
   while (virgule != NULL)
   {
      char *spaces=virgule;
      virgule[0]=0x00;
      spaces--;
      while (spaces[0] == ' ') { spaces--; };
      spaces++;
      spaces[0]=0x00;
      //printf("convertListToArray:itm='%s'\n",itm);
      if (strlen(itm) > 0)
      {
         if (validFilter(itm) > 1)
         {
            WARN(WRN_INVALIDFILTER,"Invalid filter '%s'.\n",itm);
         }
         arrayAddKey(list,itm,NULL);
      };
      virgule++;
      if (virgule[0] != 0x00)
      {
         itm=virgule;
         while (itm[0] == ' ' && itm[0] != 0x00) { itm++; };
         if (itm != NULL) { virgule=strchr(itm,','); } else {virgule=NULL;};
      }
      else
      { virgule=NULL; };
   }
   free(wrkstr_list); 
   return(list);
}


/*
restore partial /database                /source=SRC /cid=CID /port=NNNN /directory=PGDATA
restore partial /database /schema        /source=SRC /cid=CID /port=NNNN /directory=PGDATA
restore partial /database /schema /table /source=SRC /cid=CID /port=NNNN /directory=PGDATA

struct timeval time_start,time_stop;
       gettimeofday(&time_start, NULL);

*/

#define RECOVERY_LATEST 0           // No option
#define RECOVERY_UNTIL  1           // /until="date-time"          (recovery_target_time)
#define RECOVERY_RP     2           // /rp="restore_point_name"    (recovery_target_name)
#define RECOVERY_LSN    3           // /lsn='8/ED000060'           (recovery_target_lsn)

// Parameters :
//    action: restore   = 1 : cluster must be stopped
//            duplicate = 2

#define ACTION_RESTORE   1
#define ACTION_DUPLICATE 2
#define ACTION_DUPLICATE_FULL 3

void COMMAND_DUPLICATEPARTIAL(int action,int idcmd,char *command_line)
{
   if (DEPOisConnected(true) == false) return;

//   zip_t *zipHandle=NULL;

   memBeginModule();
   
   if (action == ACTION_RESTORE && CLUisConnected(false) == true )
   {
      printf("To perform a restore on the current connected cluster, you have to perform\n");
      printf("the following actions , to prevent unintentional restoration :\n");
      printf("\t1) Required informations :\n");
      printf("\t   - CID=%ld\n",varGetLong(GVAR_CLUCID));
      printf("\t   - PGDATA='%s'\n",varGet(GVAR_CLUPGDATA));
      printf("\t2) Perform a WAL backup, if you need to restore close to the current timestamp (Optional).\n");
      printf("\t3) Exit from recm tool.\n");
      printf("\t4) Stop your cluster\n");
      printf("\t   # pg_ctl stop -D %s\n",varGet(GVAR_CLUPGDATA));  
      printf("\t5) Empty the PGDATA folder (Use option '/keepdata' if you want to keep content)\n");
      printf("\t   Warning : If you want to restore the db close to the current timestamp, perform a WAL backup before to proceed.\n");
      printf("\t6) Launch recm, and connect to repository only.\n");
      printf("\t   # recm -d %s\n",varGet(GVAR_DEPNAME));
      printf("\t7) Launch the restore.\n");
      printf("\t   Example:\n\t\tRECM> restore full /cid=%ld /port=%s /directory=\"%s\"/until=\"2021-01-01 00:00:01\"\n",
             varGetLong(GVAR_CLUCID),
             varGet(GVAR_CLUPORT),
             varGet(GVAR_CLUPGDATA));
      memEndModule();
      return;
   }
   if (qualifierIsUNSET("qal_newname") == true)
   {
      char *buildName=malloc(128);
      sprintf(buildName,"RESTORE_%ld",varGetLong("qal_port"));
      qualifierSET("qal_newname",buildName);
      free(buildName);
   };
   if (action == ACTION_RESTORE)
   {
      qualifierUNSET("qal_db");
      qualifierUNSET("qal_schema");
      qualifierUNSET("qal_table");
   }
   if ( optionIsSET("opt_standby") ) // Ignore any restore limitations when '/standby' is used.
   {
      qualifierUNSET("qal_lsn");
      qualifierUNSET("qal_until");
      qualifierUNSET("qal_rp");
      qualifierUNSET("qal_db");
      qualifierUNSET("qal_schema");
      qualifierUNSET("qal_table");
   };
   if (action == ACTION_DUPLICATE_FULL)
   {
      qualifierUNSET("qal_db");
      qualifierUNSET("qal_schema");
      qualifierUNSET("qal_table");
   }

   if (optionIsSET("opt_verbose") == true) globalArgs.verbosity=true;                                                              // Set Verbosity

   struct timeval time_start,time_stop;
   
   long cluster_port=0; 
   int cluster_id=0;

   gettimeofday(&time_start, NULL);
   
   VERBOSE("Restore started at %s\n",displayTime(&time_start));
   if (strcmp(varGet("qal_port"),QAL_UNSET) != 0)
   {
      cluster_port=varGetLong("qal_port"); 
   }
   else
   { 
      ERROR(ERR_MISQUALVAL,"'/port' qualifier is mandatory for restore.\n");
      memEndModule();
      return;
   };

   char *restore_rp=NULL;
   char *destination_folder=memAlloc(1024);
    
   if (qualifierIsUNSET("qal_directory") == false)
   {
      strcpy(destination_folder,varGet("qal_directory")); 
   }
   else
   { 
      ERROR(ERR_MISQUALVAL,"'/directory' qualifier is mandatory for partial restore.\n");
      memEndModule();
      return;
   };
   if (varGet(GVAR_CLUCID) != NULL && strcmp(varGet(GVAR_CLUCID),VAR_UNSET_VALUE) != 0) cluster_id=varGetInt(GVAR_CLUCID);

   char *query=memAlloc(1024);
   char *cluster_name=memAlloc(128); 
     
   if (qualifierIsUNSET("qal_source")==false)
   {
      sprintf(query,"select count(*) from %s.clusters where clu_nam='%s'",
                    varGet(GVAR_DEPUSER),
                    varGet("qal_source"));
      int rc_rs=DEPOquery(query,0); // -12- OK
      if (DEPOgetInt(0,0) == 0)
      {
         ERROR(ERR_BADCLUNAME,"Unknown cluster source name '%s'•\n",varGet("qal_source"));
         DEPOqueryEnd(); // -12- OK
         memEndModule();
         return;
      }
      DEPOqueryEnd(); // -12- OK
      sprintf(query,"select CID from %s.clusters where clu_nam='%s'",
                       varGet(GVAR_DEPUSER),
                       varGet("qal_source"));
      rc_rs=DEPOquery(query,0); // -13- OK
      cluster_id=DEPOgetInt(0,0);
      DEPOqueryEnd(); // -13- OK
      strcpy(cluster_name,varGet("qal_source"));
      varAdd("qal_cid",QAL_UNSET);                                              // Take care to not try to use /CID after /SOURCE
   }

   if (qualifierIsUNSET("qal_cid") == false)
   {
      sprintf(query,"select count(*) from %s.clusters where cid=%s",
                    varGet(GVAR_DEPUSER),
                    varGet("qal_cid"));
      int rc_rs=DEPOquery(query,0);  // -14- OK
      if (DEPOgetInt(0,0) == 0)
      {
         ERROR(ERR_BADCLUNAME,"Unknown cluster ID %s\n",varGet("qal_cid"));
         DEPOqueryEnd(); // -14- OK
         memEndModule();
         return;
      }
      DEPOqueryEnd();  // -14- OK    
      sprintf(query,"select clu_nam from %s.clusters where cid=%s",
                       varGet(GVAR_DEPUSER),
                       varGet("qal_cid"));
      rc_rs=DEPOquery(query,0); // -15- OK
      strcpy(cluster_name,DEPOgetString(0,0));
      cluster_id=varGetInt("qal_cid");
      DEPOqueryEnd(); // -15- OK
   };

// Now, identify the FULL backup we need to use for
   char *qry_until=memAlloc(128);  qry_until[0]=0x00; 
   char *qry_id=memAlloc(128);     qry_id[0]=0x00; 
   char *psg_mode=memAlloc(128);   psg_mode[0]=0x00;
   char *psg_date=memAlloc(128);   psg_date[0]=0x00;
   
   if (qualifierIsUNSET("qal_until") == false)
   { strcpy(qry_until,varGet("qal_until")); };

   if (qualifierIsUNSET("qal_uid") == false)
   { sprintf(qry_id," and bp.bck_id='%s'",varGet("qal_uid")); }; 


   int recovery_end_mode=RECOVERY_LATEST;

   //###########################################################################
   // qualifier : /until="date"  ==> recovery_target_time
   //###########################################################################
   if (qualifierIsUNSET("qal_until") == false)
   {
      sprintf(query,"select to_timestamp('%s','%s')",qry_until,varGet(GVAR_CLUDATE_FORMAT));
      int rcd=DEPOquery(query,0); // -5- OK
      if (rcd != 1)
      {
         DEPOqueryEnd();// -5- OK
         ERROR(ERR_INVDATFMT,"Invalid date format '%s'. Must be in format '%s'\n",
                             qry_until,
                             varGet(GVAR_CLUDATE_FORMAT));
         memEndModule();
         return;
      };
      recovery_end_mode=RECOVERY_UNTIL;
      strcpy(psg_mode,"recovery_target_time");
      strcpy(psg_date,DEPOgetString(0,0));
      DEPOqueryEnd();// -5- OK
#define QRYR_BCKID   0
#define QRYR_BCKTYP  1
#define QRYR_PATH    2
#define QRYR_FILE    3 
#define QRYR_PCID    4
#define QRYR_TL      5
#define QRYR_BCKSIZ  6
#define QRYR_PGVER   7
#define QRYR_BWALL   8
#define QRYR_OPTIONS 9

      sprintf(query,"with "
                    "  rdate as(select to_timestamp('%s','%s') as restore_date), "
                    "  fullback as(select max(bdate) fulldate from %s.backups,rdate r where cid=%d and bcktyp='FULL' and bcksts=0 and bdate <= r.restore_date) "
                    "select b.bck_id,b.bcktyp,b.bckdir,p.pname,p.pcid,b.timeline,b.bcksize,b.pgversion,b.bwall,b.options from %s.backups b, %s.backup_pieces p ,fullback f "
                    "where b.cid=%d and p.cid=b.cid and b.bdate = f.fulldate and bcktyp='FULL' and bcksts=0 and p.bck_id=b.bck_id "
                    "union "
                    "select distinct w.bck_id,'WAL',b.bckdir,p.pname,0 as pcid,0 as timeline,0 as bcksize,'0' as pgversion,'' as bwall,0"
                    "  from %s.backups b,%s.backup_wals w, %s.backup_pieces p,fullback f ,rdate r "
                    "   where w.cid=%d and p.bck_id=w.bck_id and p.cid=w.cid and b.bck_id=p.bck_id and b.cid=p.cid"
                    "   and w.edate >= f.fulldate and w.edate <= r.restore_date order by bck_id,pcid",
                    qry_until,
                      varGet(GVAR_CLUDATE_FORMAT),
                      varGet(GVAR_DEPUSER),
                      cluster_id,
                      varGet(GVAR_DEPUSER),
                      varGet(GVAR_DEPUSER),
                      cluster_id,
                      varGet(GVAR_DEPUSER),
                      varGet(GVAR_DEPUSER),
                      varGet(GVAR_DEPUSER),
                      cluster_id);
      VERBOSE("Restore until Date '%s'\n",qry_until);   
   };
   //###########################################################################
   // qualifier : /lsn="0/NNNNN"  ==> (recovery_target_lsn)
   //###########################################################################
   if (qualifierIsUNSET("qal_lsn") == false)
   {
      sprintf(query,"select bck_id,bwall,ewall,timeline from %s.backups where cid=%d and bcktyp='FULL' and bcksts=0 order by bck_id desc",
                    varGet(GVAR_DEPUSER),
                    cluster_id);
      int rc_rs=DEPOquery(query,1); // -6- OK
      int row=0;
      char *selected_bck_id=NULL;
      char *f_bck_id=memAlloc(124);
      char *f_bwall =memAlloc(124);
      char *f_ewall =memAlloc(124);
      long f_timeline;
      int stop_on_error=0;
      while (stop_on_error == 0 && row < rc_rs && selected_bck_id == NULL)
      {
         strcpy(f_bck_id,DEPOgetString(row,0));
         strcpy(f_bwall, DEPOgetString(row,1));
         strcpy(f_ewall, DEPOgetString(row,2));
         f_timeline=DEPOgetLong(row,3);
         int rcB=LSN_COMPARE(f_bwall,varGet("qal_lsn"));
         switch(rcB)
         {
         case LSN_ISLT :                                                        // If LSN is llower that the specified one, we ca take this backup
            INFO("Selecting backup '%s' (%s <- %s -> %s)\n",f_bck_id,f_bwall,varGet("qal_lsn"),f_ewall);
            selected_bck_id=strdup(f_bck_id);
            break;
         case LSN_2ISBAD :
            WARN(WRN_BADLSN,"Invalid LSN '%s' (Should be in the format '<hex>/<hex>')\n",varGet("qal_lsn"));
            stop_on_error++;
            break;
         default:
            INFO("Checking backup '%s' (%s <-> %s)\n",f_bck_id,f_bwall,f_ewall);
         };
         row++;
      }
      DEPOqueryEnd(); // -6- OK
      if (stop_on_error != 0)
      {
         memEndModule();
         return;
      };
      if (selected_bck_id == NULL)
      {
         ERROR(ERR_NOLSNFND,"No backup found for LSN '%s'.\n",varGet("qal_lsn"));
         memEndModule();
         return;
      };
      char *last_wal_file=WALFILE_FROM_LSN(varGet("qal_lsn"),f_timeline);
      if (last_wal_file == NULL) 
      {
         WARN(WRN_BADLSN,"Invalid LSN '%s' (Should be in the format '<hex>/<hex>')\n",varGet("qal_lsn"));
         memEndModule();
         return;
      }
      recovery_end_mode=RECOVERY_LSN;
      strcpy(psg_mode,"recovery_target_lsn");
      strcpy(psg_date,varGet("qal_lsn"));
      VERBOSE("Restore until logical sequence number (LSN) '%s'\n",varGet("qal_lsn"));
           sprintf(query,"select b.bck_id,b.bcktyp,b.bckdir,p.pname,p.pcid,b.timeline,b.bcksize,b.pgversion,b.bwall,b.options from %s.backups b, %s.backup_pieces p"
                         " where b.cid=%d and p.cid=b.cid and b.bck_id='%s' and bcktyp='FULL' and bcksts=0 and p.bck_id=b.bck_id "
                         "union "
                         "select distinct w.bck_id,'WAL',b.bckdir,p.pname,0 as pcid,0 as timeline,0 as bcksize,'0' as pgversion,'' as bwall,0 "
                         "  from %s.backups b,%s.backup_wals w, %s.backup_pieces p,%s.backup_wals bwl"
                         "   where w.cid=%d and b.bck_id > '%s' and p.bck_id=w.bck_id and p.cid=w.cid and b.bck_id=p.bck_id and b.cid=p.cid"
                         "   and bwl.cid=b.cid and bwl.walfile='%s'"
                         "   and w.edate <= bwl.edate"
                         "   order by bck_id,pcid",
                         varGet(GVAR_DEPUSER),varGet(GVAR_DEPUSER),
                         cluster_id,
                         selected_bck_id,
                         varGet(GVAR_DEPUSER),varGet(GVAR_DEPUSER),varGet(GVAR_DEPUSER),varGet(GVAR_DEPUSER),
                         cluster_id,
                         selected_bck_id,
                         last_wal_file
                         );
   }

   //###########################################################################
   // qualifier : /rp="string"  ==> restore_point_name (Restore point name)
   //###########################################################################
   if (qualifierIsUNSET("qal_rp") == false)
   {
      sprintf(query,"select lsn from %s.backup_rp "
                    "where cid=%d "
                    "  and rp_name=lower('%s')",
                    varGet(GVAR_DEPUSER),
                    cluster_id,
                    varGet("qal_rp"));
      int rcd=DEPOquery(query,0); // -7- OK
      if (rcd != 1 || DEPOrowCount() == 0)
      {
         DEPOqueryEnd();// -7- OK
         ERROR(ERR_INVRPNAME,"restore point '%s' not found.\n",varGet("qal_rp"));
         memEndModule();
         return;
      };
      restore_rp=memAlloc(1024);
      strcpy(restore_rp,DEPOgetString(0,0));
      recovery_end_mode=RECOVERY_RP;
      strcpy(psg_mode,"recovery_target_name");
      strcpy(psg_date,varGet("qal_rp"));
      DEPOqueryEnd();// -7- OK
         
      // Now, with the date, we need to identify the FULL backup we need to take
      sprintf(query,"with "
                    "  rdate as(select crdate as restore_date from %s.backup_rp where cid=%d and rp_name=lower('%s') ), "
                    "  fullback as(select max(bdate) fulldate from %s.backups,rdate r where cid=%d and bcktyp='FULL' and bcksts=0 and bdate <= r.restore_date) "
                    "select b.bck_id,b.bcktyp,b.bckdir,p.pname,p.pcid,b.timeline,b.bcksize,b.pgversion,b.bwall,b.options from %s.backups b, %s.backup_pieces p ,fullback f "
                    "where b.cid=%d and p.cid=b.cid and b.bdate = f.fulldate and bcktyp='FULL' and bcksts=0 and p.bck_id=b.bck_id "
                    "union "
                    "select distinct w.bck_id,'WAL',b.bckdir,p.pname,0 as pcid,0 as timeline,0 as bcksize,'0' as pgversion,'' as bwall,0 "
                    "  from %s.backup_wals w, %s.backup_pieces p,fullback f ,rdate r, %s.backups b "
                    "   where w.cid=%d and b.bck_id=w.bck_id and b.cid=w.cid" 
                    "     and w.cid=b.cid and b.bck_id=p.bck_id "
                    "   and w.edate >= f.fulldate and w.edate <= (r.restore_date + interval '30 minutes') order by bck_id,pcid",
                    varGet(GVAR_DEPUSER),
                    cluster_id,
                    varGet("qal_rp"),
                    varGet(GVAR_DEPUSER),
                    cluster_id,
                    varGet(GVAR_DEPUSER),
                    varGet(GVAR_DEPUSER),
                    cluster_id,
                    varGet(GVAR_DEPUSER),
                    varGet(GVAR_DEPUSER),
                    varGet(GVAR_DEPUSER),
                    cluster_id);
      VERBOSE("Restore until restore point '%s'\n",varGet("qal_rp"));
   };

   //###########################################################################
   // Missing qualifier : '/rp','/until'
   // Restore to thte latest backupf as possible
   //###########################################################################   
   if (recovery_end_mode == RECOVERY_LATEST)
   {
      strcpy(psg_mode,"recovery_target_timeline");
      strcpy(psg_date,"latest");
      /*sprintf(query,"select b.bck_id,b.bcktyp,b.bckdir,p.pname,p.pcid,b.timeline,b.bcksize,b.pgversion,b.bwall,b.options from %s.backups b, %s.backup_pieces p "
                    "where b.cid=%d and b.bdate >= (select max(bdate) from %s.backups where"
                    " cid=%d and bcktyp='FULL' and bcksts=0)"
                    " and b.bcktyp in ('WAL','FULL')"
                    " and p.bck_id=b.bck_id and p.cid=b.cid order by b.bck_id,P.pcid",
       */             
       sprintf(query,"with fullbck as "
                    "( select max(bck_id) bck_id from %s.backups where cid=%d and bcktyp='FULL' and bcksts=0 ) "
                    "select b.bck_id,b.bcktyp,b.bckdir,p.pname,p.pcid,b.timeline,b.bcksize,b.pgversion,b.bwall,b.options  "
                    "  from %s.backups b, %s.backup_pieces p, fullbck fb "
                    " where b.cid=%d and b.bck_id=p.bck_id and b.bck_id=fb.bck_id "
                    "union  "
                    "select b.bck_id,b.bcktyp,b.bckdir,p.pname,p.pcid,b.timeline,b.bcksize,b.pgversion,b.bwall,b.options  "
                    "  from %s.backups b, %s.backup_pieces p,  fullbck fb "
                    " where b.cid=%d and b.bck_id>=fb.bck_id and b.bck_id=p.bck_id and b.bcktyp='WAL' and b.bcksts=0 "
                    " order by bck_id,pcid ",
                    varGet(GVAR_DEPUSER),
                    cluster_id,
                    varGet(GVAR_DEPUSER),
                    varGet(GVAR_DEPUSER),
                    cluster_id,
                    varGet(GVAR_DEPUSER),
                    varGet(GVAR_DEPUSER),
                    cluster_id);
      VERBOSE("Restore until 'latest' possible transaction.\n");
   }
   Array *dbnlist=convertListToArray(varGet("qal_db"));
   Array *schlist=convertListToArray(varGet("qal_schema"));
   Array *tbllist=convertListToArray(varGet("qal_table"));

   //Array *itemToRestore=arrayCreate(ARRAY_UP_ORDER);
   //int ok=0;

   if (directory_exist(destination_folder) == false) 
   { 
      //TRACE("GVAR_CLUMASK_RESTORE='%s'\n",varGet(GVAR_CLUMASK_RESTORE));
      mkdirs(destination_folder,maskTranslate(varGet(GVAR_CLUMASK_RESTORE))); 
   }
   else
   {
      chmod(destination_folder,maskTranslate(varGet(GVAR_CLUMASK_RESTORE)));
   }
   VERBOSE("Restore will be done in folder '%s'\n",destination_folder);
   long restored=0;
   long skipped=0;
   long directories=0;
   long datablock_size=varGetLong(GVAR_CLUREADBLOCKSIZE);                       // Allocate a buffer. Size is coming from variable 'GVAR_CLUREADBLOCKSIZE'
   if (datablock_size < 1024) datablock_size=16384;
   char *datablock=memAlloc(datablock_size+1);

   char *zipfile=memAlloc(1024);   zipfile[0]=0x00; 
   char *datafile=memAlloc(128);   datafile[0]=0x00; 
   
   if (strcmp(varGet("qal_until"),QAL_UNSET) != 0)
   { strcpy(qry_until,varGet("qal_until")); };

   if (strcmp(varGet("qal_uid"),QAL_UNSET) != 0)
   { sprintf(qry_id," and bp.bck_id='%s'",varGet("qal_uid")); }; 


   int rows=DEPOquery(query,1);                                                 // Loop on all pieces
   if (rows == 0)
   {
      INFO("No Backup found.\n");
      DEPOqueryEnd(); // -9- OK
      memEndModule();
      return;
   }
   

   char *full_bck_id=memAlloc(124); 
   char *f_bck_id=memAlloc(124); 
   char *f_bcktyp=memAlloc(124); 
   char *f_bckfil=memAlloc(1024);
   char *f_bckbwl=memAlloc(124);
   long f_bckpci;
   int  f_bcktl ;
   long f_bcksiz;
   long freeDiskSpace;
   int  f_bckver;
   long f_options;
   
   freeDiskSpace=freeSpaceMB(destination_folder);
   TRACE("freeDiskSpace=%ld\n",freeDiskSpace);
   // First row contain the FULL backup.
   int row=0;
   // Check if 'SET SOURCE/DIRECTORY' has been defined
   char *SRCDIR=memAlloc(2048);
   strcpy(f_bck_id,DEPOgetString(row,QRYR_BCKID));
   strcpy(full_bck_id,f_bck_id);
   strcpy(f_bcktyp,DEPOgetString(row,QRYR_BCKTYP));
   if (varExist(SOURCE_DIRECTORY) == true) { strcpy(SRCDIR,varGet(SOURCE_DIRECTORY)); }
                                      else { strcpy(SRCDIR,DEPOgetString(row,QRYR_PATH)); }
   sprintf(f_bckfil,"%s/%s",SRCDIR,DEPOgetString(row,QRYR_FILE));
   f_bckpci=DEPOgetLong (row,QRYR_PCID);
   f_bcktl =DEPOgetInt  (row,QRYR_TL);
   f_bcksiz=(DEPOgetLong(row,QRYR_BCKSIZ) / 1024/1024);
   f_bckver=DEPOgetInt  (row,QRYR_PGVER);
   strcpy(f_bckbwl,DEPOgetString(row,QRYR_BWALL));
   f_options=DEPOgetLong(row,QRYR_OPTIONS);

   // Change : 0xA000B
   if ((f_options & BACKUP_OPTION_NOINDEX) == BACKUP_OPTION_NOINDEX && optionIsSET("opt_standby") == true && optionIsSET("opt_force") == false)
   {
      ERROR(ERR_INVBCKTYPE,"Duplicate for STANDBY cannot be initiated by a backup with '/noindex' option (Backup ID:%s)\n", f_bck_id);
      DEPOqueryEnd();// -9- OK
      clearArrayItemToRestore(dbnlist);                                                 // Clear all arrays
      clearArrayItemToRestore(schlist);
      clearArrayItemToRestore(tbllist);
      memEndModule();
      return;
   }
  
   
   if (f_bcksiz >= freeDiskSpace)                                               // Verify disk space before to start restore
   {
      ERROR(ERR_NOFREESPACE,"File system have only %ld MB free. Need %ld MB.\n",freeDiskSpace,f_bcksiz);
      DEPOqueryEnd();// -9- OK
      clearArrayItemToRestore(dbnlist);                                                 // Clear all arrays
      clearArrayItemToRestore(schlist);
      clearArrayItemToRestore(tbllist);
      memEndModule();
      return;
   }

   VERBOSE("File System have %ld MB free. Backup will take %ld MB.\n",freeDiskSpace,f_bcksiz);

   INFO("Restoring backup from UID '%s' (TL %d, Version %d, WAL '%s')\n",
        full_bck_id,
        f_bcktl,
        f_bckver,
        f_bckbwl);
   
   // Check if we have used tablespace translation (set target /tablespace=xxx /directory="yyyy" )
/*
typedef struct TargetPiece TargetPiece;
struct TargetPiece
{
    int    tgtype;   // Target type 'TARGET_TABLESPACE'
    char   *name;    // Target 'name'
    char   *value;   // Target 'value'
    void   *t1;      // used by 'duplicate'
    void   *t2;      // used by 'duplicate'
    void   *t3;
};
recm_db=# select * from backup_tbs;
 cid |        bck_id        | tbs_oid | tbs_nam |              tbs_loc              
-----+----------------------+---------+---------+-----------------------------------
   3 | 000361df4bb22733cff8 |  131219 | tbs2    | /tmp/data/TBS0_6666
   3 | 000361df4bb22733cff8 |  255325 | tbs3    | /tmp/data/TBS1_6666
   1 | 00016205299d02278f38 |  255459 | tbs1    | /Library/PostgreSQL/12/CLU12_TBS1
   1 | 0001620529131d5746b8 |  255459 | tbs1    | /Library/PostgreSQL/12/CLU12_TBS1
   1 | 0001620529d003da9e38 |  255459 | tbs1    | /Library/PostgreSQL/12/CLU12_TBS1

   
*/
   if (target_list == NULL) target_list=arrayCreate(ARRAY_UP_ORDER); 
   if (arrayCount(target_list) > 0)
   {
      char *qry2=memAlloc(1024);
      sprintf(qry2,"select tbs_nam,tbs_oid,tbs_loc from %s.backup_tbs where bck_id='%s' order by tbs_nam",
                   varGet(GVAR_DEPUSER),
                   full_bck_id);
      int rctbs=DEPOquery(qry2,0);
      int error_count=0;
      if (rctbs > 0)
      {
         char *tbsnam=memAlloc(512);
         char *tbsoid=memAlloc(128);
         char *tbspth=memAlloc(1024);
         int trow=0;
         while (trow < rctbs)
         {
            strcpy(tbsnam,DEPOgetString(trow,0));
            strcpy(tbsoid,DEPOgetString(trow,1));
            strcpy(tbspth,DEPOgetString(trow,2));
            ArrayItem *fnd=arrayFind(target_list,tbsnam);
            if (fnd != NULL && strcmp(fnd->key,tbsnam) == 0)
            { 
               TargetPiece *tp=(TargetPiece *)fnd->data;
               tp->t1=strdup(tbsoid);
               tp->t2=strdup(tbspth);
               VERBOSE("Target tablespace '%s' [%s] will be remapped to '%s'\n",tp->name,tp->t1,tp->value);
               TRACE("TargetPiece : name='%s' value='%s' t1='%s' t2='%s' t3='%s'\n",tp->name,tp->value,tp->t1,tp->t2,tp->t3);

               if (directory_exist(tp->value) == false)
               {
                  ERROR(ERR_DIRNOTFND,"Directory '%s' (Remap of tablespace '%s') must exist.\n",tp->value,tp->name);
                  error_count++;
               }
               if (directoryIsWriteable(tp->value) == false)
               {
                  ERROR(ERR_BADDIRPERM,"Missing write access to directory '%s' (Remap of tablespace '%s').\n",tp->value,tp->name);
                  error_count++;        
               }
               if (directoryIsEmpty(tp->value) == false)
               {
                  ERROR(ERR_DIRNOTEMPTY,"Directory '%s' (Remap of tablespace '%s') must be empty.\n",tp->value,tp->name);
                  error_count++;
               }             
            }
            else
            {
               VERBOSE("Target tablespace '%s' will not be remapped.\n",tbsnam);
            }
            trow++;
         }
      };
      DEPOqueryEnd();
      if (error_count > 0)
      {
         memEndModule();
         return;
      }
   };
   // Stop PostgreSQL engine : PGDATA is '/directory=' and PORT is '/port='
   //(const char *AUXPGDATA,long AUXPORT)
   //if (pgStopAuxiliaryEngine(destination_folder,cluster_port) == false) VERBOSE("Engine is not running (%s)",last_pg_ctl_message);
   
   if (optionIsSET("opt_keepdata") == false && directoryIsEmpty(destination_folder) == false)
   {
      ERROR(ERR_DIRNOTEMPTY,"Directory '%s' must be empty to launch a restore.\n",destination_folder);
      DEPOqueryEnd();// -9- OK
      clearArrayItemToRestore(dbnlist);                                                 // Clear all arrays
      clearArrayItemToRestore(schlist);
      clearArrayItemToRestore(tbllist);
      memEndModule();
      return;
   }

   // rename 'data' directory
   if (optionIsSET("opt_keepdata") == true)  
   {
      char *old_data=memAlloc(1024);
      int index_dir=0;
      int rc=-1;
      while (rc == -1)
      {
         index_dir++;
         sprintf(old_data,"%s_%02d",destination_folder,index_dir);
         rc=rename(destination_folder,old_data);
      };
      VERBOSE("PGDATA '%s' renamed to %s\n",destination_folder,old_data);
   }
   TRACE("directory:'%s'\n",destination_folder);
   if (directory_exist(destination_folder) == false) 
   { 
      mkdirs(destination_folder,maskTranslate(varGet(GVAR_CLUMASK_RESTORE)));
   };
   
   // Need to know if we have compressed WAL files or not 

   char* RECM_WALCOMP_EXT=getenv (ENVIRONMENT_RECM_WALCOMP_EXT);
   char* walcomp_ext=NULL;
   if (RECM_WALCOMP_EXT == NULL) { walcomp_ext=strdup(".gz"); }
                            else { walcomp_ext=strdup(RECM_WALCOMP_EXT); };
   
   int WALisCompressed=false;
   sprintf(query,"select case position('%s' in walfile) when -1 then 'F' else 'T' end from %s.backup_wals where cid=%d and bck_id = (select max(bck_id) from %s.backups where cid=%d) limit 1",
                 walcomp_ext,
                 varGet(GVAR_DEPUSER),
                 cluster_id,
                 varGet(GVAR_DEPUSER),
                 cluster_id);
   int rcwal=DEPOquery(query,1); // -10- OK
   if (rcwal == 1)
   {
      WALisCompressed=(strcmp(DEPOgetString(0,0),"T") == 0);
   }
   DEPOqueryEnd(); // -10- OK
   if (WALisCompressed) VERBOSE("WAL files are compressed (extension '%s')\n",walcomp_ext);
   free(walcomp_ext);
      
   
   // We will use a temporary folder for WAL files
   char *tmpWALDIR=malloc(1024);
   sprintf(tmpWALDIR,"%s/recm_wals",destination_folder);
   long mskLg=maskTranslate(varGet(GVAR_CLUMASK_RESTORE));
   TRACE ("MASK RESTORE : mask='%s' smask='%s' hex='%3lx'\n",GVAR_CLUMASK_RESTORE, varGet(GVAR_CLUMASK_RESTORE),mskLg); 
   mkdirs(tmpWALDIR,mskLg);
   
   
   int option_inclusive=true; //recovery_target_inclusive = true (this is the default for postgreSQL)
   if (optionIsSET("opt_inclusive"))    { option_inclusive=true; };
   if (optionIsSET("opt_noninclusive")) { option_inclusive=false; };
   if (option_inclusive == false) { VERBOSE("Recovery will stop just before the specified recovery target (recovery_target_inclusive is false)\n"); }
                             else { VERBOSE("Recovery will stop just after the specified recovery target (recovery_target_inclusive is true)\n"); };
   
   if (optionIsSET("opt_norecover") == true)
   {
      WARN(WRN_RECONOTSTARTED,"Recovery will not start by request.\n");
   }
   if (optionIsSET("opt_standby") == true)
   {
      // Option '/paused' is ignored for '/standby'
      pgAddRecoveryParametersForStandby(cluster_id,destination_folder,tmpWALDIR,psg_mode,psg_date,WALisCompressed);
   }
   else
   {
      pgAddRecoveryParameters(destination_folder,tmpWALDIR,psg_mode,psg_date,WALisCompressed,option_inclusive,optionIsSET("opt_paused"));
   }
   pgCreateSignalRecoveryFile(optionIsSET("opt_standby"),destination_folder);
   free(tmpWALDIR);

   Array *objlist=arrayCreate(ARRAY_NO_ORDER);

   char *original_cfg =memAlloc(1024);sprintf(original_cfg,"%s/postgresql.conf",destination_folder);
   char *original_auto=memAlloc(1024);sprintf(original_auto,"%s/postgresql.auto.conf",destination_folder);
   char *original_hba =memAlloc(1024);sprintf(original_hba,"%s/pg_hba.conf",destination_folder);

   char *saved_cfg =memAlloc(1024);saved_cfg[0]=0x00;
   char *saved_auto=memAlloc(1024);saved_auto[0]=0x00;
   char *saved_hba =memAlloc(1024);saved_hba[0]=0x00;

   int  tbs_seq=0;       // Tablespace sequence ID (Restore replace tablespace link to a local folder named 'TBS<seq>_<port>'
   int failed=0;
   row=0;
   RECMDataFile *hDF;
   VERBOSE("Restore will read %d pieces\n",rows);
   while (row < rows && failed == 0)
   {
      strcpy(f_bck_id,DEPOgetString(row,QRYR_BCKID));
      strcpy(f_bcktyp,DEPOgetString(row,QRYR_BCKTYP));
      sprintf(f_bckfil,"%s/%s",SRCDIR,DEPOgetString(row,QRYR_FILE));
      f_bckpci=DEPOgetLong(row,QRYR_PCID);
      f_bcktl =DEPOgetLong(row,QRYR_TL);
      f_bcksiz=DEPOgetLong(row,QRYR_BCKSIZ);
      VERBOSE("[%d/%d] '%s'\n",(row+1),rows,f_bckfil);
      if (row == 0)                                                             // We have the BCK_ID. We can load objects we want to restore
      {
         // Now loading all user objects that will be 'included' by default
         sprintf(query,"select d.db_oid,d.db_nam,t.tab_sch,t.tab_nam,t.tab_fid,t.tbs_oid "
                      "  from %s.backup_tbl t,%s.backup_dbs d "
                      "where t.cid=%d and t.bck_id='%s' "
                      "  and t.cid=d.cid  and t.bck_id=d.bck_id"
                      "  and t.tab_dbid=d.db_oid "
                      "order by 1,2,3",
                      varGet(GVAR_DEPUSER),
                      varGet(GVAR_DEPUSER),
                      cluster_id,
                      f_bck_id);
         int rc=DEPOquery(query,0); // -11- ok
         TRACE("[rc=%d]\n",rc);
         for(int n=0; n < DEPOrowCount();n++)
         {
            TRACE("%d/%ld)Loading table (%s) '%s'.'%s'\n",n,DEPOrowCount(),DEPOgetString(n,1),DEPOgetString(n,2),DEPOgetString(n,3));
            ItemToRestore *obj=memAlloc(sizeof(ItemToRestore));
            obj->structType=ITEM_TABLE;
            obj->flag=FLAG_ALL_SELECTED;                  // All objects selected by default
            obj->candropdb=ITEM_ALLOWDROPDB;              // Assume we can drop database by default
            obj->dbId=DEPOgetLong(n,0);
            obj->dbNam=strdup(DEPOgetString(n,1));        // DB name
            obj->sch=strdup(DEPOgetString(n,2));          // Schema Name
            obj->tbl=strdup(DEPOgetString(n,3));          // Table Name
            obj->tblfid=strdup(DEPOgetString(n,4));       // Physical file name of the table
            obj->tbsoid=DEPOgetLong(n,5);                 // OID tablespace
            arrayAddKey(objlist,DEPOgetString(n,2),obj);
         }
         DEPOqueryEnd(); // -11- ok
         // Now Verify with passed arguments what we want to restore
         /* CHANGE : 0xA0009
             Accept filters like : 'tab*, '-tab*','dba*state'
             on /databae,/schema and /table
         */
         ArrayItem *element=arrayFirst(objlist);
         while (element != NULL)
         {
//            TRACE("Element '%s'\n",element->key);
            ItemToRestore *obj=(ItemToRestore *)element->data;
//            TRACE(" -> Object '%s' '%s.%s'\n",obj->dbNam,obj->sch,obj->tbl);
            // All databases
            ArrayItem *dbitm=arrayFirst(dbnlist);
            int changed=0;
            while (dbitm != NULL && changed==0)
            {
               switch( compareItem(dbitm->key,obj->dbNam))
               {
               case CMPITEM_EQUAL             : obj->flag=setFlag(obj->flag,FLAG_DATABASE); changed++;break;
               case CMPITEM_DIFFERENT         : obj->flag=unsetFlag(obj->flag,FLAG_DATABASE);break;
               case CMPITEM_EQUAL_NEGATED     : obj->flag=unsetFlag(obj->flag,FLAG_DATABASE);changed++;break;
               case CMPITEM_DIFFERENT_NEGATED : obj->flag=setFlag(obj->flag,FLAG_DATABASE);break;
               }
               dbitm=arrayNext(dbnlist);
            };
            // All schemas
            ArrayItem *schitm=arrayFirst(schlist);
            changed=0;
            while (schitm != NULL && changed==0)
            {
               switch( compareItem(schitm->key,obj->sch))
               {
               case CMPITEM_EQUAL             : obj->flag=setFlag(obj->flag,FLAG_SCHEMA); changed++;break;
               case CMPITEM_DIFFERENT         : obj->flag=unsetFlag(obj->flag,FLAG_SCHEMA);break;
               case CMPITEM_EQUAL_NEGATED     : obj->flag=unsetFlag(obj->flag,FLAG_SCHEMA);changed++;break;
               case CMPITEM_DIFFERENT_NEGATED : obj->flag=setFlag(obj->flag,FLAG_SCHEMA);break;
               }
               schitm=arrayNext(schlist);
            }
            // All tables
            ArrayItem *tblitm=arrayFirst(tbllist);
            changed=0;
            while (tblitm != NULL && changed==0)
            {
               switch( compareItem(tblitm->key,obj->tbl))
               {
               case CMPITEM_EQUAL             : obj->flag=setFlag(obj->flag,FLAG_TABLE); changed++;break;
               case CMPITEM_DIFFERENT         : obj->flag=unsetFlag(obj->flag,FLAG_TABLE);break;
               case CMPITEM_EQUAL_NEGATED     : obj->flag=unsetFlag(obj->flag,FLAG_TABLE);changed++;break;
               case CMPITEM_DIFFERENT_NEGATED : obj->flag=setFlag(obj->flag,FLAG_TABLE);break;
               }
               tblitm=arrayNext(tbllist);
            }
            char *info=malloc(20);info[0]=0x00;
            if ((obj->flag & FLAG_DATABASE) == FLAG_DATABASE) strcat(info,"DBN ");
            if ((obj->flag & FLAG_SCHEMA) == FLAG_SCHEMA)     strcat(info,"SCH ");
            if ((obj->flag & FLAG_TABLE) == FLAG_TABLE)       strcat(info,"TBL ");
            TRACE("Object '%s' (db='%s'): %s\n",obj->tbl,obj->dbNam,info);

            element=arrayNext(objlist);
         };
         // Now display only selected items:
         element=arrayFirst(objlist);
         while (element != NULL)
         {
            ItemToRestore *obj=(ItemToRestore *)element->data;
            if ((obj->flag & FLAG_ALL_SELECTED) == FLAG_ALL_SELECTED)
            {
               VERBOSE("Selected table '%s'.'%s' (database '%s').\n",obj->sch,obj->tbl,obj->dbNam);
            };
            element=arrayNext(objlist);
         }
      }
      // Restore files
      long entries=RECMGetEntries(f_bckfil);
      VERBOSE("Restoring piece '%s' (%ld files)\n",f_bckfil,entries);
      int ok=true;
      if (entries == -1)
      {
         ERROR(ERR_BADPIECE, "Cannot process piece '%s'\n",f_bckfil);
         ok=false;
         failed++;
      };
      if (ok == true && row > 0 && f_bckpci == 1 && strcmp(f_bcktyp,"FULL") == 0) { ok=false; };  // Just ignore any other FULL backup
      if (ok == true)
      {
         hDF=RECMopenRead(f_bckfil);
         if (hDF == NULL) { failed++;ok=false; };
      };
      if (ok == true)
      {
         struct zip_stat sb;
         struct zip_file *zf;
         char *OutFileName=memAlloc(1024);
         int i=0;
         long entries=RECMGetEntriesRead(hDF);
         TRACE("Entries in container : %ld\n",entries);
         while( i < entries)
         {  
            if (zip_stat_index(hDF->zipHandle, i, 0, &sb) == 0) 
            {
               TRACE("Entry %d is '%s'\n",i,sb.name); 
               RecmFileItemProperties *fileprop=RECMGetFileProperties(hDF,i);
               if (fileprop == NULL) { TRACE("fileprop is NULL\n"); }
                                else { TRACE("%x(%s) - %u:%u\n",fileprop->mode,maskCreate(fileprop->mode),fileprop->uid,fileprop->gid); };
               char *lnk_delimiter=strstr(sb.name,"@?@");
               if (lnk_delimiter != NULL)                                       // This is a LINK to a tablespace. Restore change link to a local folder named ''TBS<seq>_<port>''
               {
                  TRACE ("Found LINK:'%s'\n",sb.name);
                  char *left_part=malloc(1024);
                  char *right_part=malloc(1024);
                  lnk_delimiter[0]=0x00;
                  strcpy(left_part,sb.name);
                  lnk_delimiter+=3;
                                                                                // Below 'pg_tblspc',we have a tablespace OID as LINK name.
                                                                                // If we used 'set target/tablespace='nnnn', we may change the directory target
                  char *tbsoid=left_part;
                  tbsoid+=strlen(left_part)-1;
                  while (tbsoid[0] != '/') tbsoid--;
                  if (tbsoid[0] == '/') { tbsoid++;
                                          ArrayItem *itm=arrayFirst(target_list);
                                          ArrayItem *fnd=NULL;
                                          while (itm != NULL && fnd == NULL)
                                          {
                                             TargetPiece *tp=(TargetPiece *)itm->data;
                                             if (strcmp(tp->t1,tbsoid) == 0) { fnd=itm; };
                                             itm=arrayNext(target_list);
                                          };
                                          //char *src=lnk_delimiter;                            // We keep the default directory for the tablespace                    
                                          if (fnd != NULL)
                                          {
                                             TargetPiece *tp=(TargetPiece *)fnd->data;
                                             strcpy(right_part,tp->value);                    // We take the new TARGET directory for the tablespace
                                          }
                                          else
                                          {
                                             sprintf(right_part,"%s/TBS%d_%ld",destination_folder,tbs_seq,cluster_port); // Force folder name to become 'TBSxx_pppp'
                                             tbs_seq++;
                                          };
                                        }
                                   else {
                                          sprintf(right_part,"%s/TBS%d_%ld",destination_folder,tbs_seq,cluster_port); // Force folder name to become 'TBSxx_pppp'
                                          tbs_seq++;
                                        };
                  char *x=strstr(sb.name,"./");
                  if (x != NULL) { x+=2; } else { x=(char *)&sb.name; }
                  sprintf(OutFileName,"%s/%s",destination_folder,x);
                  int rc=mkdir(right_part,S_IRWXU | S_IRWXG | S_IRWXO);
                  TRACE("Create directory '%s' (rc=%d : errno=%d)\n",right_part,rc,errno);
                  rc=symlink(right_part,OutFileName);
                  VERBOSE("symlink ('%s','%s')\n",right_part,OutFileName);

                  char *mapfile=malloc(1024);
                  char *linknameonly=malloc(1024);
                  char *wx=strchr(x,'/');
                  char *sx=wx;
                  while(wx != NULL)
                  {
                     wx++;
                     sx=wx;
                     wx=strchr(wx,'/');
                  }
                  if (sx == NULL) { strcpy(linknameonly,x); }
                             else { strcpy(linknameonly,sx); };
                  sprintf(mapfile,"%s/tablespace_map",destination_folder);
                  FILE *mf=fopen(mapfile, "a");
                  fprintf(mf,"%s %s\n",linknameonly,right_part);
                  fclose(mf);
                  free(linknameonly);
                  free(mapfile);
                  printf("Create link '%s' with '%s'(rc=%d : %d)\n",OutFileName,right_part,rc,errno);
                  free(left_part);
                  free(right_part);
               }
               else
               {
                  char *x=strstr(sb.name,"./");
                  if (x != NULL) { x+=2; } else { x=(char *)&sb.name; }
                  char *nn=x; 
                  nn+=strlen(x);
                  nn--;
                  TRACE("nn='%s'\n",nn);
                  if (nn != NULL && strcmp(nn,"/") == 0)
                  {
                     sprintf(OutFileName,"%s/%s",destination_folder,x);
                     if (directory_exist(OutFileName) == false) 
                     {
                        int rc=mkdirs (OutFileName,S_IRUSR|S_IWUSR|S_IXUSR); //fileprop->mode);
                        TRACE("mkdir '%s' [rc=%d] [mode=%2x |uid=%d |gid=%d]\n",OutFileName,rc,fileprop->mode,fileprop->uid,fileprop->gid);
                     }
                     else
                     {
                        TRACE("dir '%s' already exist [mode=%2x |uid=%d |gid=%d]\n",OutFileName,fileprop->mode,fileprop->uid,fileprop->gid);
                     }
                     directories++;
                  }
                  else
                  {
                     // All WAL files are saved into 'pg_wal' directory directly'
                     if (strcmp(f_bcktyp,"WAL") == 0) { sprintf(OutFileName,"%s/recm_wals/%s",destination_folder,x); }
                                                 else { sprintf(OutFileName,"%s/%s",destination_folder,x); }

                     // Due to // restore, we need to check if target directory is already restored.
                     char *wrkstr=malloc(1024);
                     strcpy(wrkstr,extractFilePath(OutFileName));
                     if (directory_exist(wrkstr) == false) 
                     {
                        int rc=mkdirs (wrkstr,S_IRUSR|S_IWUSR|S_IXUSR); //fileprop->mode);
                        TRACE("mkdir '%s' [rc=%d] [mode=%2x |uid=%d |gid=%d]\n",OutFileName,rc,fileprop->mode,fileprop->uid,fileprop->gid);
                     };
                     
                     displayProgressBar(i,entries,80,NULL);
                     // Verify if we can restore this file
                     int accept=1;
                     if (strcmp(x,"postgresql.conf")       == 0) { sprintf(saved_cfg ,"%s.rtmp",OutFileName);strcpy(OutFileName,saved_cfg);  };
                     if (strcmp(x,"postgresql.auto.conf")  == 0) { sprintf(saved_auto,"%s.rtmp",OutFileName);strcpy(OutFileName,saved_auto); };
                     if (strcmp(x,"pg_hba.conf")           == 0) { sprintf(saved_hba ,"%s.rtmp",OutFileName);strcpy(OutFileName,saved_hba);  };
                     if (strcmp(x,"tablespace_map")        == 0) { accept=0; };
                     char *file_id=malloc(128);
                     ArrayItem *element=arrayFirst(objlist);
                     while (element != NULL && accept==1)
                     {
                        ItemToRestore *obj=(ItemToRestore *)element->data;
                        sprintf(file_id,"/%s",obj->tblfid);
                        int xl=strlen(x);
                        char *px=x;px+=xl;px--;
                        while (px[0] != '/' && xl > 0) { xl--; px--; };
                        if ((obj->flag & FLAG_ALL_SELECTED) != FLAG_ALL_SELECTED && strcmp(px,file_id) == 0)
                        {
                           accept=0;
                        };
                        element=arrayNext(objlist);
                     }
                     free(file_id);
                     if (accept == 1)
                     {
                        if (file_exists(OutFileName)==true)
                        {
                           skipped++;
                           accept=0;
                        }
                     }
                     if (accept == 1)
                     {
                        zf = zip_fopen_index(hDF->zipHandle, i, 0);
                        if (!zf) 
                        {
                           ERROR(ERR_BADPIECE, "Cannot process piece '%s': %s\n",f_bckfil,zip_strerror(hDF->zipHandle));
                           memEndModule();
                           return;
                        }
                        mkdirs( extractFilePath(OutFileName),fileprop->mode);
                        int fd = open(OutFileName, O_RDWR | O_TRUNC | O_CREAT, fileprop->mode);
                        if (fd < 0) 
                        {
                           ERROR(ERR_CREATEFILE, "Cannot create file '%s': %s\n",OutFileName,strerror(errno));
                           memEndModule();
                           return;
                        }
                        zip_uint64_t sum = 0;
                        int len=1;
                        TRACE("sb.size=%llu\n",sb.size);
                        while (sum != sb.size && len > 0 ) 
                        {
                            len = zip_fread(zf, datablock, datablock_size);
                            if (len < 0) 
                            {
                                ERROR(ERR_BADPIECE, "Cannot process piece '%s'\n",OutFileName);
                                memEndModule();
                                return;
                            }
                            int written=write(fd, datablock, len);
                            if (written < 0)
                            {
                               int rc=errno;
                               TRACE("write block size=%d error (errno=%d - %s)\n",len,rc,strerror(rc));
                            }
                            sum += len;
                        }
                        close(fd);
                        zip_fclose(zf);
                        TRACE("Restored file name '%s'\n",OutFileName);
                        restored++;
                     }
                     else
                     {
                        skipped++;
                     }
                  }
               }
               free(fileprop);
            }
            else
            {
               TRACE("Failed entry %i\n",i);
            }
            i++;
         }
         displayProgressBar(entries,entries,80,NULL);
         RECMcloseRead(hDF);
      };
      row++;
   }
   DEPOqueryEnd(); // -9-
   if (failed > 0)
   {
      VERBOSE("Restore aborted.\n");
      memEndModule();
      return;
   }
   VERBOSE("Restore ended.\n");
   VERBOSE(" - %ld directories created.\n",directories);
   VERBOSE(" - %ld file(s) restored.\n",restored);
   VERBOSE(" - %ld file(s) skipped.\n",skipped);

   create_hba_file(destination_folder);                                      // Create simple pg_hba.conf file, to allow local connection easily.

   // Need GVAR_SOCKPATH variable to be set
   if (varIsSET(GVAR_SOCKPATH) == false) { varAdd(GVAR_SOCKPATH,"/tmp"); };

   
   // Create custom postgresql.conf file
   if (qualifierIsUNSET("qal_newname") == false ) 
      { create_config_file(destination_folder,cluster_port,varGet("qal_newname")); }
   else
      { create_config_file(destination_folder,cluster_port,NULL); };

   chmod(destination_folder,maskTranslate(varGet(GVAR_CLUMASK_RESTORE)));

   if (optionIsSET("opt_norecover") == false)
   {
      if (pgStartAuxiliaryEngine(destination_folder,cluster_port) == false) 
      {
         memEndModule();
         return;
      }
   };
   if (optionIsSET("opt_norecover") == false && optionIsSET("opt_standby") == false)
   {
      // Check if recovery is still active
      if (AUXconnect (VAR_UNSET_VALUE,getUserName(),NULL,"localhost",varGet("qal_port")) == true)
      {
         TRACE("AUX Connected.\n");
         // Wait for RECOVERY to start
         VERBOSE("Starting recovery...\n");
         long IS_IN_RECOVERY=1;
         int MAX_WAIT=20;
         int max_wait=MAX_WAIT;
         char *plsn=memAlloc(20);
         char *lsn=memAlloc(20);lsn[0]=0x00;
         while (IS_IN_RECOVERY == 1 && max_wait > 0) 
         {
            int rc_tl=AUXquery("select case pg_is_in_recovery() when true then 1 else 0 end",0);
            if (rc_tl == 1) 
            { 
               IS_IN_RECOVERY=AUXgetLong(0,0);
               AUXqueryEnd();
               rc_tl=AUXquery("select coalesce(pg_last_wal_replay_lsn(),'0/00000000');",0);
               if (rc_tl == 1) 
               {
                  strcpy(plsn,lsn);
                  strcpy(lsn,AUXgetString(0,0));
                  if (strcmp(plsn,lsn) != 0)
                  {
                     switch (recovery_end_mode)
                     {
                     case RECOVERY_LSN   : INFO("Recovery in progress (LSN '%s' [until LSN '%s'] )\n",lsn,varGet("qal_lsn"));
                                           break;
                     case RECOVERY_RP    : INFO("Recovery in progress (LSN '%s' [until RP '%s'] )\n",lsn,restore_rp); 
                                           break;
                     case RECOVERY_UNTIL : INFO("Recovery in progress (LSN '%s' [until DATE/TIME '%s'] )\n",lsn,qry_until); 
                     
                     default: INFO("Recovery in progress (LSN '%s')\n",lsn);
                     }
                  };
               };
               AUXqueryEnd();
               max_wait=MAX_WAIT;
               sleep(5);
            } 
            else 
            { 
               AUXqueryEnd();
               max_wait=0;
            };
            TRACE("RECO_I:rc=%d IS_IN_RECOVERY=%ld\n",rc_tl,IS_IN_RECOVERY);
            max_wait--;
         }
         if (IS_IN_RECOVERY == 1)
         {
            WARN(WRN_STILINRECOVERY,"Database still in recovery mode. Please check.\n");
         }
         else
         {
            sprintf(query,"select coalesce(pg_walfile_name('%s'),'none')",lsn);
            int rc_tl=AUXquery(query,0);
            if (rc_tl == 1) { INFO("Recovery ended at WAL '%s', LSN '%s'\n",AUXgetString(0,0),lsn); }
            AUXqueryEnd();
            VERBOSE("Opening cluster instance...\n");
            rc_tl=AUXquery("SELECT timeline_id FROM pg_control_checkpoint()",0);
            long TIMELINE_AFTER=AUXgetLong(0,0);
            TRACE("Auxiliary Timeline is %ld [rc=%d]\n",TIMELINE_AFTER,rc_tl);
            AUXqueryEnd();
               
            INFO("Database successfully restored.\n");
            INFO("Connect using Host:port: localhost:%ld\n",cluster_port);
         }
         AUXdisconnect();
      };
      VERBOSE("Clean up excluded definition...\n");
      char *wrk_statement=memAlloc(1024);
      char *masterdb=memAlloc(512);masterdb[0]=0x00;
      
      PGconn     *dblist_cnx=NULL;
      PGresult   *dblist_res=NULL;
      char *connectString=memAlloc(512);
      ArrayItem *dbLoop=arrayFirst(objlist);                                    // Now, loop on ALL databases to 
      while (dbLoop != NULL)                                                    // drop excluded objects and reindex tables
      {
         ItemToRestore *db_obj=(ItemToRestore *)dbLoop->data;
         if ((db_obj->flag & FLAG_PROCESSED) != FLAG_PROCESSED)
         {
            if (dblist_cnx != NULL) PQfinish(dblist_cnx);
            dblist_cnx=NULL;
            sprintf(connectString,"dbname=%s host=localhost port=%s",
                               db_obj->dbNam,
                               varGet("qal_port"));
/*
            sprintf(connectString,"dbname=%s user=%s password=%s host=localhost port=%s",
                               db_obj->dbNam,
                               varGet(GVAR_CLUUSER),
                               varGet(GVAR_CLUPWD),
                               varGet("qal_port"));
*/
            TRACE("AUX connection:%s\n",connectString);
            dblist_cnx = PQconnectdb(connectString);
            if (PQstatus(dblist_cnx) == CONNECTION_OK)
            {
               TRACE("Connection OK\n");
               if (strlen(masterdb) == 0)
               {
                  // Get the master db name
                  dblist_res=PQexec(dblist_cnx,"select datname from pg_database where datistemplate=false and oid <= (select min(oid) from pg_database where datistemplate=false)");
                  strcpy(masterdb,PQgetvalue(dblist_res,0,0));
                  TRACE("MASTER DB is : %s\n",masterdb);
                  PQclear(dblist_res);
               };
               ArrayItem *obj=arrayFirst(objlist);
               while (obj != NULL)
               {
                  ItemToRestore *item=(ItemToRestore *)obj->data;
                  if (strcmp(item->dbNam,db_obj->dbNam) == 0)
                  {
                     int do_query=0;
                     TRACE("Database '%s', Object '%s.%s' (flag:%lx)\n",item->dbNam,item->sch,item->tbl,item->flag);
                     if ((item->flag & FLAG_ALL_SELECTED) != FLAG_ALL_SELECTED)
                     {
                        sprintf(wrk_statement,"DROP TABLE \"%s\".\"%s\" CASCADE",item->sch,item->tbl);
                        do_query++;
                     }
                     else
                     {
                        if ((f_options & BACKUP_OPTION_NOINDEX) == BACKUP_OPTION_NOINDEX) do_query++;
                        if (varGetInt(GVAR_REINDEXCONCURRENTLY) == true)
                        { 
                           sprintf(wrk_statement,"reindex table CONCURRENTLY \"%s\".\"%s\"",item->sch,item->tbl); 
                        }
                        else
                        {
                           sprintf(wrk_statement,"reindex table \"%s\".\"%s\"",item->sch,item->tbl);
                        };
                        item->candropdb=ITEM_KEEPDB;
                        // We need to flag all items of the same DB to avoid DB being droped
                        ArrayItem *dbchk=arrayFirst(objlist);
                        while (dbchk != NULL)
                        {
                           ItemToRestore *dbitm=(ItemToRestore *)dbchk->data;
                           if (strcmp(dbitm->dbNam,db_obj->dbNam) == 0) { dbitm->candropdb=ITEM_KEEPDB; };
                           dbchk=arrayNext(objlist);
                        }
                        arraySetCurrent(objlist,obj);
                     }
                     if (do_query > 0)                                          // We re-index only if FULL backup was made with '/noindex' option
                     {
                        TRACE("STATEMENT:'%s'\n",wrk_statement);
                        dblist_res=PQexec(dblist_cnx,wrk_statement);
                        ExecStatusType rc=PQresultStatus(dblist_res);
                        TRACE("PQresultStatus [rc=%d]\n",rc);
                        if (PQresultStatus(dblist_res) != PGRES_COMMAND_OK)
                        {
                           TRACE("RC=%s failed\n",PQerrorMessage(dblist_cnx));
                        }
                        else
                        {
                           TRACE("RC=%s OK\n",PQerrorMessage(dblist_cnx));
                        };
                        PQclear(dblist_res);
                     };
                     item->flag=setFlag(item->flag,FLAG_PROCESSED);
                  }
                  obj=arrayNext(objlist);
               }
               if (dblist_cnx != NULL) { PQfinish(dblist_cnx); dblist_cnx=NULL; };
            }
            else { TRACE("AUX connection failed\n"); };
         }
         arraySetCurrent(objlist,dbLoop);
         dbLoop=arrayNext(objlist);
      }
      if (dblist_cnx != NULL) PQfinish(dblist_cnx);

      if (optionIsSET("opt_vacuum") == true)
      {
         char *VACUUM_CMD=memAlloc(1024);
         char *vacuum_options=memAlloc(512);
         if (qualifierIsUNSET("qal_options") == false)
         {
            strToUpper(vacuum_options,varGet("qal_options"));
         }
         else
         {
            strcpy(vacuum_options,"FULL ");
            if (optionIsSET("opt_verbose") == true) strcat(vacuum_options,"VERBOSE ");
         };
         strcpy(VACUUM_CMD,"vacuum ");
         if (strstr(vacuum_options,"FULL") != NULL) strcat(VACUUM_CMD,"full ");
         if (strstr(vacuum_options,"FREEZE") != NULL) strcat(VACUUM_CMD,"freeze ");
         if (strstr(vacuum_options,"VERBOSE") != NULL) strcat(VACUUM_CMD,"verbose ");
         if (strstr(vacuum_options,"ANALYZE") != NULL) strcat(VACUUM_CMD,"analyze ");
         if (strstr(vacuum_options,"DISABLE_PAGE_SKIPPING") != NULL) strcat(VACUUM_CMD,"disable_page_skipping ");
         if (strstr(vacuum_options,"INDEX_CLEANUP:OFF") != NULL) strcat(VACUUM_CMD,"Index_cleanup:off ");
         if (strstr(vacuum_options,"INDEX_CLEANUP:ON")  != NULL) strcat(VACUUM_CMD,"Index_cleanup:on ");
         if (strstr(vacuum_options,"INDEX_CLEANUP:AUTO") != NULL) strcat(VACUUM_CMD,"Index_cleanup:auto ");
         if (strstr(vacuum_options,"PROCESS_TOAST") != NULL) strcat(VACUUM_CMD,"process_toast ");
         if (strstr(vacuum_options,"TRUNCATE") != NULL) strcat(VACUUM_CMD,"truncate ");
         VERBOSE("Starting '%s'...\n",VACUUM_CMD);           
         strcat(VACUUM_CMD,";");
         dbLoop=arrayFirst(objlist);                                    // Now, loop on ALL databases to perform VACUUM
         while (dbLoop != NULL)
         {
            ItemToRestore *db_obj=(ItemToRestore *)dbLoop->data;
            if ((db_obj->flag & FLAG_VACUUMED) != FLAG_VACUUMED)
            {
               if (dblist_cnx != NULL) PQfinish(dblist_cnx);
               dblist_cnx=NULL;
               sprintf(connectString,"dbname=%s host=localhost port=%s",
                                  db_obj->dbNam,
                                  varGet("qal_port"));
               TRACE("AUX connection:%s\n",connectString);
               dblist_cnx = PQconnectdb(connectString);
               if (PQstatus(dblist_cnx) == CONNECTION_OK)
               {
                  VERBOSE("Executing '%s' on database '%s'\n",VACUUM_CMD,db_obj->dbNam);
                  dblist_res=PQexec(dblist_cnx,VACUUM_CMD);
                  PQclear(dblist_res);
                  
                  /* for all objects that belongs to the same database, we set it as 'VACUUMED' */
                  ArrayItem *obj=arrayNext(objlist);
                  while (obj != NULL)
                  {
                     ItemToRestore *item=(ItemToRestore *)obj->data;
                     if (strcmp(item->dbNam,db_obj->dbNam) == 0) item->flag=setFlag(item->flag,FLAG_VACUUMED);
                     obj=arrayNext(objlist);
                  }
               }
               else
               {
                  TRACE("AUX connection failed\n");
               };
               db_obj->flag=setFlag(db_obj->flag,FLAG_PROCESSED);
            }
            arraySetCurrent(objlist,dbLoop);
            dbLoop=arrayNext(objlist);
         }
         if (dblist_cnx != NULL) PQfinish(dblist_cnx);
      } /* end of '/vacuum' command */

      // Now, check if we can drop an empty database
      if (strlen(masterdb) > 0)
      {
         sprintf(connectString,"dbname=%s host=localhost port=%s",
                               masterdb,
                               varGet("qal_port"));
          TRACE("AUX connection:%s\n",connectString);
          dblist_cnx = PQconnectdb(connectString);
          if (PQstatus(dblist_cnx) == CONNECTION_OK)
          {
             TRACE("AUX CONNECTED\n");
             ArrayItem *obj=arrayFirst(objlist);
             while (obj != NULL)
             {
                ItemToRestore *item=(ItemToRestore *)obj->data;
                if (item->candropdb == ITEM_ALLOWDROPDB)
                {
                   sprintf(wrk_statement,"drop database \"%s\"",item->dbNam);
                   dblist_res=PQexec(dblist_cnx,wrk_statement);
                   ExecStatusType rc=PQresultStatus(dblist_res);
                   TRACE("PQresultStatus [rc=%d]\n",rc);
                   if (PQresultStatus(dblist_res) != PGRES_COMMAND_OK)
                   {
                      TRACE("RC=%s failed\n",PQerrorMessage(dblist_cnx));
                   }
                   else
                   {
                      TRACE("RC=%s OK\n",PQerrorMessage(dblist_cnx));
                   };
                   PQclear(dblist_res);
                   item->candropdb=ITEM_DBDROPED;
                   // Now, flag all objects of the same DB
                   ArrayItem *Sobj=arrayFirst(objlist);
                   while (Sobj != NULL)
                   {
                      ItemToRestore *Sitem=(ItemToRestore *)Sobj->data;
                      if (strcmp(Sitem->dbNam,item->dbNam) == 0) { Sitem->candropdb=ITEM_DBDROPED; }
                      Sobj=arrayNext(objlist);
                   }
                   arraySetCurrent(objlist,obj);
                }
                obj=arrayNext(objlist);
             }
             PQfinish(dblist_cnx);
          }
      }
   }
   else
   {
      if (optionIsSET("opt_norecover") == true) VERBOSE("Recovery not done.\n");
      if (optionIsSET("opt_standby") == true) VERBOSE("STANDBY database started.\n");
   }

   gettimeofday(&time_stop, NULL);
   double elapsed= (time_stop.tv_sec  - time_start.tv_sec);
   VERBOSE("Restore start time %s\n",displayTime(&time_start));
   VERBOSE("         finish at %s\n",displayTime(&time_stop));
   VERBOSE("Elapsed time : %s.\n",DisplayElapsed(elapsed));
   
   if (action == ACTION_RESTORE) // If we do a RESTORE FULL, we will restart the cluster with the original configuration files
   {
      if (pgStopAuxiliaryEngine(destination_folder,cluster_port) == true)
      {
         TRACE ("Auxiliary Engine : STOPPED\n");
         if (strlen(saved_cfg) > 0)
         {
            int rc=unlink(original_cfg);
            if (rc != 0) { TRACE("UNLINK error %d  (%s)\n",errno,original_cfg); };
            rc=rename(saved_cfg,original_cfg);
            if (rc != 0) { TRACE("RENAME error %d  (%s -> %s)\n",errno,saved_cfg,original_cfg); };
            TRACE("Restoring original configuration file '%s'\n",original_cfg);
         };
         if (strlen(saved_hba) > 0)
         {
            int rc=unlink(original_hba);
            if (rc != 0) { TRACE("UNLINK error %d  (%s)\n",errno,original_hba); };
            rc=rename(saved_hba,original_hba);
            if (rc != 0) { TRACE("RENAME error %d  (%s -> %s)\n",errno,saved_hba,original_hba); };
            TRACE("Restoring original configuration file '%s'\n",original_hba);
         };
         if (strlen(saved_auto) > 0)
         {
            int rc=unlink(original_auto);
            if (rc != 0) { TRACE("UNLINK error %d  (%s)\n",errno,original_auto); };
            rc=rename(saved_auto,original_auto);
            if (rc != 0) { TRACE("RENAME error %d  (%s -> %s)\n",errno,saved_auto,original_auto); };
            TRACE("Restoring original configuration file '%s'\n",saved_auto);
         };
         // Restarting cluster with original configuration files
         pgStartEngine();
      }
      else
      {
         TRACE("Could not stop Auxiliary Engine\n");
      }
   }
   else
   {
      unlink(saved_cfg);                                                        // If we made a duplicate, we can drop the original configuration files
      unlink(saved_hba);
      unlink(saved_auto);
   };
   memEndModule();
}

