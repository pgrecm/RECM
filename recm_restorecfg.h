/**********************************************************************************/
/* RESTORE CONFIG command                                                         */
/* Usage:                                                                         */
/*    restore config                                                              */                 
/*      Available Option(s):                                                      */
/*         /overwrite                 Overwrite existing file                     */
/*         /verbose                                                               */
/*      Available Qualifier(s):                                                   */
/*         /cid=<STRING>              use a backup from a different cluster       */
/*         /uid=<STRING>              use a specified backup                      */
/*         /file=<FILE>               Select file to restore                      */
/*         /directory=<PATH>          Destination folder                          */
/*         /before=<DATE>             Find a backup with a given date before      */
/*         /after=<DATE>              Find a backup with a given date after       */
/*         /tag=<STRING>              Search for a backup by it's TAG             */
/**********************************************************************************/
void COMMAND_RESTORECFG(int idcmd,char *command_line)
{
   long cluster_id=0;
//   zip_t *zipHandle;
   RECMDataFile *hDF;

   if (DEPOisConnected(true) == false) return;
   
   memBeginModule();
   
   char *SRCDIR=memAlloc(1024);
     
/*
if (directory_exist(varGet(GVAR_BACKUPDIR)) == false)
   {
      ERROR(ERR_INVDIR,"Invalid Backup directory '%s'\n",varGet(GVAR_BACKUPDIR));
      memEndModule();
      return;
   };
*/
   char *query=memAlloc(1024);
   char *cluster_name=memAlloc(128);
   
   if (varGet(GVAR_CLUCID) != NULL && strcmp(varGet(GVAR_CLUCID),VAR_UNSET_VALUE) != 0) cluster_id=varGetLong(GVAR_CLUCID);

   if (qualifierIsUNSET("qal_cid") == false)
   {
      sprintf(query,"select count(*) from %s.clusters where cid=%s",
                    varGet(GVAR_DEPUSER),
                    varGet("qal_cid"));
      int rows=DEPOquery(query,0);
      if (DEPOgetLong(0,0) == 0)
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
   }
   else
   {
      strcpy(cluster_name,varGet(GVAR_CLUNAME));
   }
   
   char *destination_folder=memAlloc(1024);

   if (qualifierIsUNSET("qal_directory") == false)
   { strcpy(destination_folder,varGet("qal_directory")); }
   else
   { strcpy(destination_folder,varGet(GVAR_CLUPGDATA)); };

   if (directory_exist(destination_folder) == false) 
   { mkdirs(destination_folder,maskTranslate(varGet(GVAR_CLUMASK_BACKUP))); };

   TRACE("destination_folder=%s\n",destination_folder);

   long datablock_size=getHumanSize(GVAR_CLUREADBLOCKSIZE);                       // Allocate a buffer. Size is coming from variable 'GVAR_CLUREADBLOCKSIZE'
   if (datablock_size < 1024) datablock_size=163840;
   char *datablock=memAlloc(datablock_size);

   if (optionIsSET("opt_verbose") == true) globalArgs.verbosity=true;                                                              // Set Verbosity

   char *qry_before=memAlloc(128); qry_before[0]=0x00; 
   char *qry_after=memAlloc(128);  qry_after[0]=0x00; 
   char *qry_id=memAlloc(128);     qry_id[0]=0x00; 
   char *qry_file=memAlloc(128);   qry_file[0]=0x00;
   char *qry_tag=memAlloc(128);    qry_tag[0]=0x00;
   char *outfile=memAlloc(1024);   outfile[0]=0x00; 


   if (qualifierIsUNSET("qal_before") == false)
   { sprintf(qry_before," and b.bdate < to_timestamp('%s','%s')",varGet("qal_before"),varGet(GVAR_CLUDATE_FORMAT)); };

   if (qualifierIsUNSET("qal_after") == false)
   { sprintf(qry_after," and b.bdate > to_timestamp('%s','%s')",varGet("qal_after"),varGet(GVAR_CLUDATE_FORMAT)); };

   if (qualifierIsUNSET("qal_uid") == false)
   { sprintf(qry_id," and b.bck_id='%s'",varGet("qal_uid")); }; 

   if (qualifierIsUNSET("qal_tag") == false)
   { sprintf(qry_tag," and b.bcktag like '%%%s%%'",varGet("qal_tag")); }; 
   sprintf(query,"select bp.bck_id,bp.pname,coalesce(to_char(b.bdate,'%s'),'0000-00-00 00:00:00'),b.bckdir from %s.backup_pieces bp,%s.backups b"
                 " where bp.bck_id=b.bck_id and bp.cid=b.cid and b.cid=%ld and b.bcksts=0 and b.bcktyp='CFG'",
                 varGet(GVAR_CLUDATE_FORMAT),
                 varGet(GVAR_DEPUSER),
                 varGet(GVAR_DEPUSER),
                 cluster_id);
   strcat(query,qry_before);
   strcat(query,qry_after);
   strcat(query,qry_id);
   strcat(query,qry_tag);
   
   strcat(query,"order by b.edate desc limit 1");

   char *bck_id=memAlloc(128);    bck_id[0]=0x00;
   char *pname=memAlloc(1024);    pname[0]=0x00;
   char *bck_dat=memAlloc(128);   bck_dat[0]=0x00;
   int wal_rows=DEPOquery(query,1);
   if (wal_rows == 0)
   {
      INFO("No CONFIG backup found.\n");
      memEndModule();
      return;
   }
   for(int i=0;i < wal_rows;i++)
   {
      strcpy(bck_id,DEPOgetString(i,0));
      strcpy(bck_dat,DEPOgetString(i,2));
      
      if (varExist(SOURCE_DIRECTORY) == true) { strcpy(SRCDIR,varGet(SOURCE_DIRECTORY)); }
                                         else { strcpy(SRCDIR,DEPOgetString(i,3));};
      sprintf(pname,"%s/%s",SRCDIR,DEPOgetString(i,1));

      VERBOSE("Found backup UID '%s', piece '%s' of date '%s'\n",bck_id,pname,bck_dat);
      hDF=RECMopenRead(pname);
      if (hDF == NULL)
      {
         ERROR(ERR_INVRECMFIL,"Corrupted or invalid RECM file '%s'\n",pname);
         memEndModule();
         return;
      }
      else
      {
         regex_t regex;
         if (qualifierIsUNSET("qal_file") == false)
         {
            regcomp(&regex, extractFileName(varGet("qal_file")), REG_EXTENDED|REG_NOSUB|REG_NEWLINE  );
         };
         struct zip_stat sb;
         struct zip_file *zf;
         int file_index=0;
         while( file_index < zip_get_num_entries(hDF->zipHandle, 0))
         {
            if (zip_stat_index(hDF->zipHandle, file_index, 0, &sb) == 0) 
            {
               char *filnam=extractFileName(sb.name);
               sprintf(outfile,"%s/%s",destination_folder,filnam);
               TRACE("filnam='%s' (%s)\n",filnam,sb.name);
               int accept=0;
               if (qualifierIsUNSET("qal_file") == false)
               {
                  char *qal_file=extractFileName(varGet("qal_file"));
                  TRACE("compare '%s' == '%s'\n",filnam,qal_file);
                  if (regexec(&regex, filnam,0, NULL, 0)== 0) {accept++; };
               }
               else { accept=1; };
               
               if (accept ==true && file_exists(outfile) == true)
               {
                  if (optionIsSET("opt_overwrite") == true)
                  {
                     VERBOSE("Overwrite file '%s' by request.\n",outfile);
                     unlink(outfile);
                  }
                  else
                  {
                     VERBOSE("File '%s' already present on disk.\n",outfile);
                     accept=false;
                  };
               };
               
               if (accept)
               {
                  RecmFileItemProperties *fileprop=RECMGetFileProperties(hDF,file_index);
                  VERBOSE("Restoring file '%s'\n",outfile);
                  zf = zip_fopen_index(hDF->zipHandle, file_index, 0);
                  if (!zf) 
                  {
                     ERROR(ERR_BADPIECE, "Cannot process piece '%s': %s\n",pname,zip_strerror(hDF->zipHandle));
                     memEndModule();
                     return;
                  }
                  int fd = open(outfile, O_RDWR | O_TRUNC | O_CREAT, fileprop->mode);
                  if (fd < 0) 
                  {
                     ERROR(ERR_CREATEFILE, "Cannot create file '%s': %s\n",outfile,strerror(errno));
                     memEndModule();
                     return;
                  }
                  long long sum = 0;
                  while (sum != sb.size) 
                  {
                      int len = zip_fread(zf, datablock, datablock_size);
                      if (len < 0) 
                      {
                          ERROR(ERR_BADPIECE, "Cannot process piece '%s'\n",pname);
                          memEndModule();
                          return;
                      }
                      write(fd, datablock, len);
                      sum += len;
                  }
                  close(fd);
                  // Set file attributes like it was before to added into ZIP file
                  struct utimbuf ut;
                  ut.actime=sb.mtime;
                  ut.modtime=sb.mtime;
                  utime(outfile, &ut);
                  //chmod(outfile,fileprop->mode);
                  chown(outfile,fileprop->uid,fileprop->gid);
                  zip_fclose(zf);
               }
            }
            file_index++;
         }
      }
      RECMcloseRead(hDF);
   }
   DEPOqueryEnd();
   memEndModule();
}
