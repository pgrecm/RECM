/**********************************************************************************/
/* RESTORE META command                                                           */
/* Usage:                                                                         */
/*    restore meta                                                                */                 
/*      Available Option(s):                                                      */
/*         /overwrite                 Overwrite existing file                     */
/*         /verbose                                                               */
/*      Available Qualifier(s):                                                   */
/*         /cid=<STRING>              use a backup from a different cluster       */
/*         /uid=<STRING>              use a specified backup                      */
/*         /file=<FILE>               Select file to restore                      */
/*         /directory=<PATH>          Destination folder                          */
/*         /tag=<STRING>              Search for a backup by it's TAG             */
/**********************************************************************************/
void COMMAND_RESTOREMETA(int idcmd,char *command_line)
{
   if (isConnectedAndRegistered() == false) return;

   if (varExist(GVAR_BACKUPDIR) == false)
   {
      ERROR(ERR_NOBACKUPDIR,"Backup directory not set.(See show cluster).\n");
      return;
   }
   if (directory_exist(varGet(GVAR_BACKUPDIR)) == false) 
   { 
      mkdirs(varGet(GVAR_BACKUPDIR),maskTranslate(varGet(GVAR_CLUMASK_BACKUP))); 
   }
   if (directory_exist(varGet(GVAR_BACKUPDIR)) == false)
   {
      ERROR(ERR_INVDIR,"Invalid Backup directory '%s'\n",varGet(GVAR_BACKUPDIR));
      return;
   };

   memBeginModule();
   char *destination_folder=memAlloc(1024);
   
   if (qualifierIsUNSET("qal_directory") == false)
   { 
      strcpy(destination_folder,varGet("qal_directory")); 
   }
   else
   {
      /* CHANGE : 0xA0004 Check if RECM_META_DIR exist */
      char *RECM_META_DIR=getenv(ENVIRONMENT_RECM_META_DIR);                                               // Check if environment variable RECM_META_DIR exist
      if (RECM_META_DIR != NULL) { strcpy(destination_folder,RECM_META_DIR); }
                            else { sprintf(destination_folder,"%s/recm_meta",varGet(GVAR_CLUPGDATA)); }    // By default, METADATA are generated in subdirectory 'recm_meta'
   };
   if (directory_exist(destination_folder) == false)
   {
      mkdirs(destination_folder,maskTranslate(varGet(GVAR_CLUMASK_BACKUP) ));                     // Create directory if it does not exist
   };
   if (directory_exist(destination_folder) == false)                                                    // Abort process if could not create diectory
   {
      ERROR(ERR_INVMETADIR,"Invalid META data directory '%s'\n",destination_folder);
      memEndModule();
      return; 
   };

   TRACE("destination_folder=%s\n",destination_folder);

   long restored=0;
   long datablock_size=getHumanSize(GVAR_CLUREADBLOCKSIZE);                       // Allocate a buffer. Size is coming from variable 'GVAR_CLUREADBLOCKSIZE'
   if (datablock_size < 1024) datablock_size=163840;
   char *datablock=memAlloc(datablock_size);

   int opt_verbose=optionIsSET("opt_verbose");
   int saved_verbose=globalArgs.verbosity;
   globalArgs.verbosity=opt_verbose;

   char *query=memAlloc(1024);
   char *qry_id=memAlloc(128);     qry_id[0]=0x00; 
   char *qry_file=memAlloc(128);   qry_file[0]=0x00;
   char *qry_tag=memAlloc(128);    qry_tag[0]=0x00;
   char *outfile=memAlloc(1024);   outfile[0]=0x00; 


   if (qualifierIsUNSET("qal_uid") == false)
   { sprintf(qry_id," and b.bck_id='%s'",varGet("qal_uid")); }; 

   if (qualifierIsUNSET("qal_tag") == false)
   { sprintf(qry_tag," and b.bcktag like '%%%s%%'",varGet("qal_tag")); }; 


   sprintf(query,"select bp.bck_id,b.bckdir||'/'||bp.pname,coalesce(to_char(b.bdate,'%s'),'0000-00-00 00:00:00') from %s.backup_pieces bp,%s.backups b"
                 " where bp.bck_id=b.bck_id and bp.cid=b.cid and b.cid=%s and b.bcksts=0 and b.bcktyp='META'",
                 varGet(GVAR_CLUDATE_FORMAT),
                 varGet(GVAR_DEPUSER),
                 varGet(GVAR_DEPUSER),
                 varGet(GVAR_CLUCID));
   strcat(query,qry_id);
   strcat(query,qry_tag);

   strcat(query," order by b.edate desc limit 1");

   TRACE("query=%s\n",query);

   char *bck_id=memAlloc(128);    bck_id[0]=0x00;
   char *pname=memAlloc(1024);    pname[0]=0x00;
   char *bck_dat=memAlloc(128);   bck_dat[0]=0x00;
   
   int wal_rows=DEPOquery(query,1);
   if (wal_rows == 0)
   {
      INFO("No META backup found.\n");
      globalArgs.verbosity=saved_verbose;
      memEndModule();
      return;
   }
   for(int i=0;i < wal_rows;i++)
   {
      strcpy(bck_id,DEPOgetString(i,0));
      strcpy(pname,DEPOgetString(i,1));
      strcpy(bck_dat,DEPOgetString(i,2));
      VERBOSE("Found backup UID '%s', piece '%s' of date '%s'\n",bck_id,pname,bck_dat);
      if (RECMopenRead(pname) == false)
      {
         ERROR(ERR_INVRECMFIL,"Corrupted or invalid RECM file '%s'\n",pname);
         globalArgs.verbosity=saved_verbose;
         memEndModule();
         return;
      }
      else
      {
         struct zip_stat sb;
         struct zip_file *zf;
         int file_index=0;
         while( file_index < zip_get_num_entries(zipHandle, 0))
         {
            if (zip_stat_index(zipHandle, file_index, 0, &sb) == 0) 
            {
               char *filnam=extractFileName(sb.name);
               sprintf(outfile,"%s/%s",destination_folder,filnam);
               TRACE("filnam='%s' (%s)\n",filnam,sb.name);
               int accept=false;
               if (qualifierIsUNSET("qal_file") == false)
               {
                  char *qal_file=extractFileName(varGet("qal_file"));
                  TRACE("compare '%s' == '%s'\n",filnam,qal_file);
                  if (strcmp(filnam,qal_file) == 0) {accept++; };
               }
               else { accept=true; };
              
               int flg_overwrite=0;
               if (accept ==true && file_exists(outfile) == true)
               {
                  if (optionIsSET("opt_overwrite") == true)
                  {
                     unlink(outfile);
                     flg_overwrite++;
                  }
                  else
                  {
                     WARN(WRN_FILEPRESENT,"File '%s' already present on disk.\n",outfile);
                     accept=false;
                  };
               };
               if (accept == true)
               {
                  RecmFileItemProperties *fileprop=RECMGetFileProperties(file_index);
                  if (flg_overwrite == 0) { VERBOSE("Restoring file '%s'.\n",outfile); }
                                     else { VERBOSE("Restore file '%s' (overwrite by request).\n",outfile); };
                  zf = zip_fopen_index(zipHandle, file_index, 0);
                  if (!zf) 
                  {
                     ERROR(ERR_BADPIECE, "Cannot process piece '%s': %s\n",pname,zip_strerror(zipHandle));
                     globalArgs.verbosity=saved_verbose;
                     memEndModule();
                     return;
                  }
                  int fd = open(outfile, O_RDWR | O_TRUNC | O_CREAT, fileprop->mode);
                  if (fd < 0) 
                  {
                     ERROR(ERR_CREATEFILE, "Cannot create file '%s': %s\n",outfile,strerror(errno));
                     globalArgs.verbosity=saved_verbose;
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
                          globalArgs.verbosity=saved_verbose;
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
   }
   DEPOqueryEnd();
   globalArgs.verbosity=saved_verbose;
   memEndModule();
}
