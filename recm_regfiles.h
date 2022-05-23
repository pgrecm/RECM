/**********************************************************************************/
/* REGISTER FILES command                                                         */
/* Usage:                                                                         */
/*    register files                                                              */                 
/*      Available Option(s):                                                      */
/*         /meta                      Register only METADATA backups              */
/*         /wal                       Register only WAL backups                   */
/*         /full                      Register only FULL backups                  */
/*         /cfg                       Register only CONFIG backups                */
/*         /verbose                                                               */
/*      Available Qualifier(s):                                                   */
/*         /cid=<STRING>              use a backup from a different cluster       */
/*         /directory=<PATH>          New PGDATA to create                        */
/**********************************************************************************/

#define RECMFILE_VERIFY_NEW   1 
#define RECMFILE_VERIFY_EXIST 2 
#define RECMFILE_VERIFY_PROCESSED 4 


// register files  /verbose,/full,/wal,/cfg,/meta,",   "/directory=S,/cid=N,"
void COMMAND_REGFILES(int idCmd,char *kw)
{
   if (DEPOisConnected(true) == false) return;
   if (CLUisConnected(true) == false) return;
//   zip_t *zipHandle;
   
   memBeginModule();
   
   if (optionIsSET("opt_verbose") == true) globalArgs.verbosity=true;                                                              // Set Verbosity
   
   char *directory_source=memAlloc(1024);
   
   if (qualifierIsUNSET("qal_directory") == false)                              // Specific directory ?
   { 
      strcpy(directory_source,varGet("qal_directory")); 
   }
   else
   { 
      if (varExist(GVAR_BACKUPDIR) == true)
      {
         strcpy(directory_source,varGet(GVAR_BACKUPDIR)); 
      }
      else
      {
         ERROR(ERR_INVDIR,"No directory specified. (See 'set cluster/backupdir')\n");
         memEndModule();
         return;
      }
   }                         // Use default Backup direcotry
   if (directory_exist(directory_source) == false)
   {  ERROR(ERR_INVDIR,"Invalid directory '%s'\n",directory_source);
      memEndModule();
      return;
   };
   char *query=memAlloc(2048);
   // We will take only the cluster's backups
   
   int filter_wal =optionIsSET("opt_wal");
   int filter_full=optionIsSET("opt_ful");
   int filter_cfg =optionIsSET("opt_cfg");
   int filter_meta=optionIsSET("opt_meta");
   int filter_all=false;
   if (filter_wal  == false && 
       filter_full == false && 
       filter_cfg  == false && 
       filter_meta==false) 
   {
      filter_all=true;
   };
   

   // Load special information for restore
   int rcx=CLUquery("select setting from pg_settings where name='log_timezone'",0);
   varAdd(GVAR_CLUTZ,CLUgetString(0,0));
   CLUqueryEnd();
   rcx=CLUquery("select setting from pg_settings where name='DateStyle'",0);
   varAdd(GVAR_CLUDS,CLUgetString(0,0));
   CLUqueryEnd();
       
   // Start scanning directory
   DIR *dp;
   struct dirent *ep;
   //struct stat st;

   VERBOSE("Scanning directory '%s'\n",directory_source);
   dp = opendir (directory_source);
   if (dp == NULL)
   {
      ERROR(ERR_INVDIR,"Invalid directory '%s' : %s\n",directory_source,strerror(errno));
      memEndModule();
      return;
   }
   
   Array *pieces=arrayCreate(ARRAY_NO_ORDER);    // Create array for all pieces
   ArrayItem *itm;

   char *cluster_id_string=memAlloc(10);
   sprintf(cluster_id_string,"%04x",varGetInt(GVAR_CLUCID));                    // We have to filter files to take only cluster's files

   char *fileNameOnly=memAlloc(1024);
   char *fullname=memAlloc(1024);
   while ( (ep = readdir (dp)) )                                                // Get  file from directory
   {
      char *sn=(char *)ep->d_name;
      if (strcmp(sn,".") == 0)  { continue; };
      if (strcmp(sn,"..") == 0) { continue; };
      if (ep->d_type == DT_DIR) 
      { 
         VERBOSE("Ignoring subfolder '%s'\n",sn);
         continue;
      }; 
      sprintf(fullname,"%s/%s",directory_source,sn);
      if (file_exists(fullname) == false) { continue; };
      if (strncmp(sn,cluster_id_string,strlen(cluster_id_string)) != 0)
      {
         VERBOSE("Skipping file '%s'. Does not belongs to cluster\n",sn);
         continue;
      };

      // Now, extract information from zip file
      RecmFile *rf=RECMGetProperties(fullname);
      if (rf == NULL)
      { 
         VERBOSE("Skipping file '%s'. Not a RECM file.\n",sn);
         continue; 
      };
      sprintf(query,"select count(*) from %s.backup_pieces where cid=%s and bck_id='%s' and pcid=%d",
                    varGet(GVAR_DEPUSER),
                    varGet(GVAR_CLUCID),
                    rf->bck_id,
                    rf->piece);
      int rc=DEPOquery(query,0);
      TRACE("q(%d):%s\n",rc,query);
      int rows=DEPOgetLong(0,0);
      DEPOqueryEnd();
      if (rows == 1) 
      { 
         VERBOSE("Backup UID '%s',piece %d already registered\n",rf->bck_id,rf->piece);
         rf->processed=RECMFILE_VERIFY_EXIST;
      }
      else
      {
         rf->processed=RECMFILE_VERIFY_NEW;
         INFO("Registering backup UID '%s', file '%s'.\n",rf->bck_id,fullname);
      };
      itm=malloc(sizeof(ArrayItem));
      itm->key=strdup(rf->bck_id);
      itm->data=rf;
      arrayAdd(pieces,itm);
   };
   closedir (dp);
   
         
   itm=arrayFirst(pieces);
   while (itm != NULL)
   {
      RecmFile *rf=(RecmFile *)itm->data;
      if ((rf->processed & RECMFILE_VERIFY_PROCESSED) == 0) // Struc not processed
      {
         long tot_size=rf->zipsize;
         long  tot_pieces=0;
         ArrayItem *context=arrayCurrent(pieces);
         ArrayItem *fndItem=arrayFirst(pieces);
         while (fndItem != NULL)
         {
            RecmFile *fnd_rf=(RecmFile *)fndItem->data;
            int accepted=true;
            if (strcmp(fnd_rf->bcktyp,"WAL")  == 0 && filter_wal  == false) { accepted=false; };
            if (strcmp(fnd_rf->bcktyp,"FULL") == 0 && filter_full == false) { accepted=false; };
            if (strcmp(fnd_rf->bcktyp,"CFG")  == 0 && filter_cfg  == false) { accepted=false; };
            if (strcmp(fnd_rf->bcktyp,"META") == 0 && filter_meta == false) { accepted=false; };
            if (filter_all == true) {accepted=true;}
            if (strcmp(fndItem->key,itm->key) == 0 && accepted == true)
            {
               char *lastSlash=&fnd_rf->filename[0];
               lastSlash+=strlen(fnd_rf->filename);
               lastSlash--;
               while (lastSlash[0] != 0x00 && lastSlash[0] != '/') { lastSlash--; };
               if (lastSlash[0] == '/') { lastSlash++; strcpy(fileNameOnly,lastSlash); } 
                                   else { strcpy(fileNameOnly,fnd_rf->filename); };
               RECMDataFile *hDF=RECMopenRead(fnd_rf->filename);
               if (hDF == NULL) { rf->processed=flagSetOption(rf->processed,RECMFILE_VERIFY_PROCESSED); continue; };
               long entries=RECMGetEntries2(hDF);

               // Check if record already exist ?
               sprintf(query,"select count(*) from %s.backup_pieces where cid=%s and bck_id='%s' and pcid=%d",
                             varGet(GVAR_DEPUSER),
                             varGet(GVAR_CLUCID),
                             itm->key,
                             fnd_rf->piece);
               int rcP=DEPOquery(query,0);
               int val=DEPOgetInt(0,0);    
               DEPOqueryEnd();
               if (rcP == 0 || val == 0) // Record not found...we can add it
               {   
                  sprintf(query,"insert into %s.backup_pieces (cid,bck_id,pcid,pname,psize,csize,pcount) values (%s,'%s',%d,'%s',%ld,%ld,%ld)",
                                varGet(GVAR_DEPUSER),
                                varGet(GVAR_CLUCID),
                                itm->key,
                                fnd_rf->piece,
                                fileNameOnly,
                                fnd_rf->zipsize,
                                0L,
                                entries);
                  DEPOquery(query,0);
                  DEPOqueryEnd();
                  rf->processed=flagSetOption(rf->processed,RECMFILE_VERIFY_PROCESSED);
               }
               // WAL files have to be registred into table backup_wals also
               if (strcmp(fnd_rf->bcktyp,"WAL") == 0)
               {
                  VERBOSE("Reading piece '%s'\n",fnd_rf->filename); 
                  struct zip_stat sb;
                  int i=0;
                  while( i < entries) 
                  {
                     if (zip_stat_index(hDF->zipHandle, i, 0, &sb) == 0) 
                     {
                        char *pwal=strchr(sb.name,'/');
                        if (pwal != NULL) { pwal++;} else { pwal=(char *)&sb.name; };
                        sprintf(query,"select count(*) from %s.backup_wals where cid=%s and bck_id='%s' and walfile='%s'",
                                       varGet(GVAR_DEPUSER),
                                       varGet(GVAR_CLUCID),
                                       itm->key,
                                       pwal);
                        int rcW=DEPOquery(query,0);
                        int valW=DEPOgetInt(0,0); 
                        DEPOqueryEnd();
                        if (rcW == 0 || valW == 0)
                        {
                           sprintf(query,"insert into %s.backup_wals (cid,walfile,ondisk,bckcount,edate,bck_id)"
                                         " values (%s,'%s',1,1,to_timestamp(%lu),'%s')",
                                         varGet(GVAR_DEPUSER),
                                         varGet(GVAR_CLUCID),
                                         pwal,
                                         sb.mtime,
                                         itm->key);
                           TRACE("WAL query=%s\n",query);
                           DEPOquery(query,0);
                           DEPOqueryEnd();
                           INFO("Remembering WAL file '%s'\n",pwal);
                        }
                        else
                        {
                           VERBOSE("WAL file '%s' already registered.\n",pwal);

                        };
                     }
                     
                     i++;
                  }
               };
               RECMcloseRead(hDF);
               tot_size+=fnd_rf->zipsize;
               tot_pieces++;
            }
            fndItem=arrayNext(pieces);
         }
         if (strcmp(rf->end_date,"none") == 0) strcpy(rf->end_date,rf->begin_date);
         sprintf(query,"select count(*) from %s.backups where cid=%s and bck_id='%s'",
                       varGet(GVAR_DEPUSER),
                       varGet(GVAR_CLUCID),
                       rf->bck_id);
         int rcQ=DEPOquery(query,0);
         int valQ=DEPOgetInt(0,0);
         DEPOqueryEnd();
         if (rcQ == 0 || valQ == 0)
         {
            sprintf(query,"insert into %s.backups (cid,bcktyp,bcksts,bck_id,bcktag,hstname,pgdata,pgversion,timeline,"
                          "options,bcksize,pcount,bwall,bwalf,bdate,ewall,ewalf,edate,ztime,sdate,bckdir)" 
                          "values (%s,'%s',0,'%s','%s','%s','%s',%d,%d,%ld,%ld,%ld,'%s','%s',to_timestamp('%s','%s'),'%s','%s',to_timestamp('%s','%s'),'%s','%s','%s')",
                          varGet(GVAR_DEPUSER),
                          varGet(GVAR_CLUCID),
                          rf->bcktyp,
                          rf->bck_id,
                          rf->bcktag,
                          varGet(GVAR_CLUHOST),
                          varGet(GVAR_CLUPGDATA),
                          rf->version,
                          rf->timeline,
                          rf->options,
                          rf->bcksize,
                          rf->pcount,
                          rf->begin_wall,
                          rf->begin_walf,
                           rf->begin_date,
                           varGet(GVAR_CLUDATE_FORMAT),
                          rf->end_wall,
                          rf->end_walf,
                           rf->end_date, 
                           varGet(GVAR_CLUDATE_FORMAT),
                          varGet(GVAR_CLUTZ),
                          varGet(GVAR_CLUDS),
                          directory_source);
            DEPOquery(query,0);
            DEPOqueryEnd();
         };
         rf->processed=flagSetOption(rf->processed,RECMFILE_VERIFY_PROCESSED);
         arraySetCurrent(pieces,context);
      };
      itm=arrayNext(pieces);
   };
   // free array
   itm=arrayFirst(pieces);
   while (itm != NULL)
   {
      RecmFile *rf=(RecmFile*)itm->data;
      freeRecmFile(rf);
      itm->data=NULL;
      arrayDelete(pieces,itm);
      itm=arrayFirst(pieces);
   };
   free(pieces);
   memEndModule();
}
