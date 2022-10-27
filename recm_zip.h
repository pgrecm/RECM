/*
@nodoc excluded from html generator
*/

/********************************************************************************/
/* Open RECM file for READ                                                      */
/********************************************************************************/
RECMDataFile *RECMopenRead(const char *recmFileName)
{
   RECMDataFile *hDF;
   hDF=malloc(sizeof(RECMDataFile));
   int zip_rc=0;
   hDF->zipHandle = zip_open(recmFileName, 0, &zip_rc);
   if (hDF->zipHandle == NULL)
   {
      ERROR(ERR_ERROPNZIP,"Could not open file '%s'\n",recmFileName); 
      free(hDF);
      return(NULL);
   };
   TRACE("ARGS(recmFileName='%s') --> RETURN (int) true\n",recmFileName);
   hDF->tarFileName=strdup(recmFileName);
   return(hDF);
}

/********************************************************************************/
/* Get number of entries from RECM file                                         */
/********************************************************************************/
long RECMGetEntriesRead(RECMDataFile *hDF)
{
   if (hDF->zipHandle == NULL) { TRACE("return (long) -1\n");return(-1); };
   long rc=zip_get_num_entries(hDF->zipHandle, 0);
   TRACE("ARGS(none) --> RETURN (long) %ld\n",rc);
   return(rc);
}

/********************************************************************************/
/* Get number of entries from RECM file                                         */
/********************************************************************************/
long RECMGetEntries(const char *recmFileName)
{
   int zip_rc=0;
   zip_t *zipHandle = zip_open(recmFileName, 0, &zip_rc);
   if (zipHandle == NULL) { TRACE("return (long) -1\n");return(-1); };
   long rc=zip_get_num_entries(zipHandle, 0);
   zip_close(zipHandle);
   zipHandle=NULL;
   TRACE("ARGS(recmFileName='%s') --> RETURN (long) %ld\n",recmFileName,rc);  
   return(rc);
}

/********************************************************************************/
/* Get number of entries from opened RECM file                                  */
/********************************************************************************/
long RECMGetEntries2(RECMDataFile *hDF)
{
   long rc=zip_get_num_entries(hDF->zipHandle, 0);
   TRACE("ARGS() --> RETURN (long) %ld\n",rc);  
   return(rc);
}

/********************************************************************************/
/* Print content of a RECM file                                                 */
/********************************************************************************/
void RECMlistContent(RECMDataFile *hDF)
{
   TRACE("ARGS(none)\n");
   struct zip_stat sb;
   char *lastmod=malloc(50);
   printf("   %-20s %10s %s\n","mdate","Size","File name");
   printf("   %-20s %10s %s\n","--------------------","----------","----------------------------------------");
   int index=1;
   int i=0;
   while( i < zip_get_num_entries(hDF->zipHandle, 0))
   {
      if (zip_stat_index(hDF->zipHandle, i, 0, &sb) == 0)
      {
         char *x=(char *)sb.name;
         x+=strlen(x)-1;
         if (x[0] != '/') 
         { // this is file, not a directory
            strftime(lastmod, 49, "%Y-%m-%d %H:%M:%S", localtime(&sb.mtime));
            printf("   %3d] %-20s %10llu %s\n",index,lastmod,sb.size,sb.name);
            index++;
         }
      }
      i++;
   };
   free(lastmod);
}

/********************************************************************************/
/* Close RECM file                                                              */
/********************************************************************************/
int RECMcloseRead(RECMDataFile *hDF)
{
   TRACE("ARGS(tarFileName='%s' pieceNumber=%d)\n",hDF->tarFileName,hDF->pieceNumber);
   int rc=0;
   rc=zip_close(hDF->zipHandle);
   if (rc != 0)
   {
      TRACE("RECM file error:%s\n",zip_strerror(hDF->zipHandle));
      return(false);
   };
   free(hDF->tarFileName);
   hDF->tarFileName=NULL;
   free(hDF);
   return(true);
}

/********************************************************************************/
/* Get file info from RECM file                                                 */
/********************************************************************************/
int RECMgetIndex(RECMDataFile *hDF,int ndx,struct zip_stat *sb)
{
   if (zip_stat_index(hDF->zipHandle, ndx, 0, sb) == 0) 
   {
      TRACE("ARGS(ndx=%d) --> RETURN (int) true\n",ndx); 
      return(true);
   }
   else
   {
      TRACE("ARGS(ndx=%d) --> RETURN (int) true\n",ndx); 
      return(false);
   };
}

void RECMClearPieceList()
{
   if (zip_filelist == NULL) zip_filelist=arrayCreate(ARRAY_NO_ORDER);
   ArrayItem *itm=arrayFirst(zip_filelist);
   while (itm != NULL)
   {
      ZIPPiece *zp=(ZIPPiece *)itm->data;
      TRACE("Freeing '%s'\n",zp->pname);
      free(zp->pname);
      arrayDelete(zip_filelist,itm);
      itm=arrayFirst(zip_filelist);
   };
}

void _internalRECMaddPiece(RECMDataFile *hDF)
{
   if (zip_filelist == NULL) zip_filelist=arrayCreate(ARRAY_NO_ORDER);
   ZIPPiece *zp=malloc(sizeof(ZIPPiece));
   zp->id=hDF->pieceNumber;
   zp->pname=strdup(hDF->tarFileName);
   ArrayItem *itm=malloc(sizeof(ArrayItem));
   itm->key=NULL;
   itm->data=zp;
   arrayAdd(zip_filelist,itm);
}

void freeRecmFile(RecmFile *rf)
{
   if (rf->bck_id != NULL) free(rf->bck_id);
   if (rf->bcktyp != NULL) free(rf->bcktyp);
   if (rf->bcktag != NULL) free(rf->bcktag);
   if (rf->begin_date != NULL) free(rf->begin_date);
   if (rf->begin_wall != NULL) free(rf->begin_wall);
   if (rf->begin_walf != NULL) free(rf->begin_walf);
   if (rf->end_date != NULL) free(rf->end_date);
   if (rf->end_wall != NULL) free(rf->end_wall);
   if (rf->end_walf != NULL) free(rf->end_walf);
   free(rf);
   TRACE("ARGS(none) --> RETURN (none)\n");
}

/********************************************************************************/
/* Get RECM file properties                                                     */
/********************************************************************************/
RecmFileItemProperties *RECMGetFileProperties(RECMDataFile *hDF,int file_index)
{
   TRACE("ARGS(file_index=%d)\n",file_index);
   zip_uint32_t datlen=50;
   char *data=(char *)zip_file_get_comment(hDF->zipHandle, file_index, &datlen, ZIP_FL_ENC_GUESS);
   TRACE("datalen=%u data='%s'\n",datlen,data);
   if (data == NULL) { TRACE("ARGS(file_index=%d) --> RETURN (itemdata) NULL\n",file_index); return(NULL); };                 // NOT a RECM file
   char *item=data;
   char *sep=strchr(item,'#');                                                                                                // First string is 'RECM'
   if (sep == NULL) { TRACE("ARGS(file_index=%d) --> RETURN (itemdata) NULL\n",file_index); return(NULL); };                  // NOT a RECM file
   sep[0]=0x00;
   if (strcmp(item,"RECM") != 0) { TRACE("ARGS(file_index=%d) --> RETURN (itemdata) NULL\n",file_index); return(NULL); };     // NOT a RECM file (bad signature)
   RecmFileItemProperties *itemData=(RecmFileItemProperties *)malloc(sizeof(RecmFileItemProperties));
   sep++;item=sep;
   sep=strchr(item,'#');                                                                                                      // Second string is 'mode'
   if (sep == NULL) { itemData->mode=maskTranslate("rwx:");
                      itemData->uid=0;
                      itemData->gid=0;
                      return(itemData);
                    };
   sep[0]=0x00;
   itemData->mode=maskTranslate(item);
   sep++;item=sep;
   sep=strchr(item,'#');                                                                                                      // third string is 'UID'
   if (sep == NULL) { itemData->mode=maskTranslate("rwx:");
                      itemData->uid=0;
                      itemData->gid=0;
                      return(itemData);
                    };
   sep[0]=0x00;
   itemData->uid=atoi(item);
   sep++;item=sep;
   sep=strchr(item,'#');                                                                                                      // forth string is 'GID'
   if (sep == NULL) { itemData->mode=maskTranslate("rwx:");
                      itemData->uid=0;
                      itemData->gid=0;
                      return(itemData);
                    };
   sep[0]=0x00;
   itemData->gid=atoi(item);
   TRACE("ARGS(file_index=%d) --> RETURN (itemdata): file_index=%d mode=%2x : uid=%u : gid=%u\n",file_index,file_index,itemData->mode,itemData->uid,itemData->gid);
   return(itemData);
}

/********************************************************************************/
/* Get RECM file properties                                                     */
/********************************************************************************/
RecmFileItemProperties *RECMGetFilePropertiesByName(RECMDataFile *hDF,const char *filename)
{
   TRACE("ARGS(filename='%s')\n",filename);
   int file_index=zip_name_locate(hDF->zipHandle, filename, ZIP_FL_ENC_GUESS);
   if( file_index < 0) return (NULL);                                           // File not found
   return (RECMGetFileProperties(hDF,file_index));
}


RecmFile *RECMGetProperties(const char *filename)
{
   TRACE("ARGS(filename='%s')\n",filename);
   int zip_rc=0;
   zip_t *zipHdl=zip_open(filename,0,&zip_rc);
   if (zipHdl == NULL) { VERBOSE("File '%s' is not a recm file.\n",filename);return(NULL); };
   int zclen=1024;
   char *zip_comment=malloc(1024);
   char *inzc=(char *)zip_get_archive_comment(zipHdl,&zclen, ZIP_FL_ENC_GUESS);
   if (inzc == NULL)   { return(NULL); };
   strcpy(zip_comment,inzc);
   TRACE("GET PROP='%s'\n",zip_comment);
   RecmFile*result=malloc(sizeof(RecmFile));
   result->processed=0;
   struct stat st;      
   stat(filename,&st);
   result->filename=strdup(filename);
   zip_close(zipHdl);                                                                                                              // Don't need any more zip file

   inzc=zip_comment;
   char *pdash=strchr(inzc,'#');                                                                                                   // backup_id
   if (pdash == NULL) { return(NULL); }
   pdash[0]=0x00;pdash++;
   result->bck_id=strdup(inzc);inzc=pdash;
   pdash=strchr(inzc,'#');
   if (pdash == NULL) { return(NULL); }
   pdash[0]=0x00;pdash++;
   result->piece=atoi(inzc);inzc=pdash;pdash=strchr(inzc,'#');                                                                     // Piece number
   if (pdash == NULL) { return(NULL); }
   pdash[0]=0x00;pdash++;
   result->bcktyp=strdup(inzc);inzc=pdash;pdash=strchr(inzc,'#');                                                                  // Backup type
   if (pdash == NULL) { return(NULL); }
   pdash[0]=0x00;pdash++;
   result->version=atoi(inzc);inzc=pdash;pdash=strchr(inzc,'#');                                                                   // PG Version (PG_VERSION content)  at backup time
   if (pdash == NULL) { return(NULL); }
   pdash[0]=0x00;pdash++;
   result->timeline=atoi(inzc);inzc=pdash;pdash=strchr(inzc,'#');                                                                  // Timeline at backup time 
   if (pdash == NULL) { return(NULL); }
   pdash[0]=0x00;pdash++;
   result->bcktag=strdup(inzc);inzc=pdash;pdash=strchr(inzc,'#');                                                                  // Backup TAG
   if (pdash == NULL) { return(NULL); }
   pdash[0]=0x00;pdash++;
   result->bcksize=atol(inzc);inzc=pdash;pdash=strchr(inzc,'#');                                                                   // Backup size
   result->zipsize=st.st_size;                                                                                                     // Size of the ZIP file
   if (pdash == NULL) { return(NULL); }
   pdash[0]=0x00;pdash++;
   result->pcount=atoi(inzc);inzc=pdash;pdash=strchr(inzc,'#');                                                                    // Backup piece count
   if (pdash == NULL) { return(NULL); }
   pdash[0]=0x00;pdash++;
   result->begin_date=strdup(inzc);inzc=pdash;pdash=strchr(inzc,'#');                                                              // Begin backup date
   if (pdash == NULL) { return(NULL); }
   pdash[0]=0x00;pdash++;
   result->begin_wall=strdup(inzc);inzc=pdash;pdash=strchr(inzc,'#');                                                              // LSN when backup start
   if (pdash == NULL) { return(NULL); }
   pdash[0]=0x00;pdash++;
   result->begin_walf=strdup(inzc);inzc=pdash;pdash=strchr(inzc,'#');                                                              // WAL file    
   if (pdash == NULL) { return(NULL); }
   pdash[0]=0x00;pdash++;
   result->end_date=strdup(inzc);inzc=pdash;pdash=strchr(inzc,'#');                                                                // End backup date

   if (pdash == NULL) { return(NULL); }
   pdash[0]=0x00;pdash++;
   result->end_wall=strdup(inzc);inzc=pdash;pdash=strchr(inzc,'#');                                                                // LSN when backup end
   if (pdash == NULL) { return(NULL); }
   pdash[0]=0x00;pdash++;
   result->end_walf=strdup(inzc);inzc=pdash;pdash=strchr(inzc,'#');                                                                // WAL file    
   if (pdash == NULL) { return(NULL); }
   pdash[0]=0x00;pdash++;
   result->options=atol(inzc);   
   free(zip_comment);
   TRACE("RecmFile Structure: piece name='%s'\n",result->filename);
   TRACE("  - Backup ID.........: '%s' (piece %d / %ld)\n",result->bck_id,result->piece,result->pcount);
   TRACE("  - Backup TYPE.......: '%s'\n",result->bcktyp);
   TRACE("  - PG version........: %d'\n",result->version);
   TRACE("  - PG timeline(TL)...: %d\n",result->timeline);
   TRACE("  - Backup TAG........: '%s'\n",result->bcktag);
   TRACE("  - Backup TYPE.......: '%s'\n",result->bcktyp);
   TRACE("  - Backup Size.......: %ld\n",result->bcksize);
   TRACE("  - Begin backup Date.: '%s'\n",result->begin_date);
   TRACE("  -       LSN.........: '%s'\n",result->begin_wall);
   TRACE("  -       WAL file....: '%s'\n",result->begin_walf);
   TRACE("  - End backup date...: '%s'\n",result->end_date);
   TRACE("  -       LSN.........: '%s'\n",result->end_wall);
   TRACE("  -       WAL file....: '%s'\n",result->end_walf);
   TRACE("  - Options...........: %ld\n",result->options);
  return(result);
}

// Set comment on ALL zip files
void RECMsetProperties(const char *backup_id)
{
   TRACE("ARGS(backup_uid '%s')\n",backup_id);
   char *zip_comment=malloc(2048);
   char *qry=malloc(2048);
   /* structure of the comment :
   bckid
   piece number/total pieces
   bcktyp
   version
   timeline
   tag
   bcksize
   piece count
   begin backup date
   begin WAL
   begin WAL filename
   end backup date
   end WAL
   end WAL filename
   option_noindex
   */
 
   sprintf(qry,"select bcktyp||'#'||"
                      "pgversion||'#'||"
                      "timeline||'#'||"
                      "bcktag||'#'||"
                      "bcksize||'#'||"
                      "pcount||'#'||"
                      "coalesce(to_char(bdate,'YYYY-MM-DD HH24:MI:SS'),'none')||'#'||"
                      "coalesce(bwall,'none')||'#'||"
                      "coalesce(bwalf,'none')||'#'||"
                      "coalesce(to_char(edate,'YYYY-MM-DD HH24:MI:SS'),'none')||'#'||"
                      "coalesce(bwall,'none')||'#'||"
                      "coalesce(bwalf,'none')||'#'||"
                      "options||'#' from %s.backups where cid=%s and bck_id='%s'",
                      varGet(GVAR_DEPUSER),
                      varGet(GVAR_CLUCID),
                      varGet(RECM_BCKID));
//   TRACE("query=%s\n",qry);
   PGresult   *res=PQexec(rep_cnx,qry);
   if (PQresultStatus(res) != PGRES_TUPLES_OK)
   {
      ERROR(ERR_DBQRYERROR,"setComment failed\n");
      free(zip_comment);
      free(qry);
      return;
   };
   ArrayItem *itm=arrayFirst(zip_filelist);
   while (itm != NULL)
   {
      ZIPPiece *zp=(ZIPPiece *)itm->data;
      sprintf(zip_comment,"%s#%d#%s",backup_id,zp->id,PQgetvalue(res,0,0));
      TRACE("SET PROP:%s\n",zip_comment);
      int zip_rc=0;
      zip_t *zipHdl=zip_open(zp->pname,0,&zip_rc);
      if (zipHdl != NULL)
      {
         int rc=zip_set_archive_comment(zipHdl, zip_comment, strlen(zip_comment));
         TRACE ("rc of zip properties=%d\n",rc);
         zip_close(zipHdl);
      }
      else { TRACE("Set zip properties failed\n"); }
      itm=arrayNext(zip_filelist);
   };
   free(zip_comment);
   free(qry);
}

void recm_progress_callback (zip_t *zH, double progression, void *data)
{
   RECMDataFile *hDF=(RECMDataFile*)data;
   
   if (hDF->parallel_count > 1) 
   {
      time_t ct=time(NULL);
      if (difftime(ct,hDF->start_time) > WORKER_DISPLAY_PROGRESSION)
      {
         printf("[Worker %d] %s - %5.0f%%\n",hDF->pieceNumber,hDF->tarFileName,(progression * 100));
         hDF->start_time=ct;
      }
   }
}

void recm_ud_free(void *data)
{
   RECMDataFile *hDF=(RECMDataFile*)data;
   TRACE ("Cleaning file %s...\n",hDF->tarFileName);
}

/********************************************************************************/
/* Create RECM file                                                             */
/********************************************************************************/
RECMDataFile * RECMcreate(const char *tarFileName,int pieceNumber,zip_uint32_t compLevel)
{
   RECMDataFile *hDF;
/*
   if (tarFileName == NULL) tarFileName=malloc(1024);
   sprintf(tarFileName,"%s/%s_%s_%s.recm",
                       varGet(GVAR_BACKUPDIR),
                       varGet(RECM_BCKID),
                       varGet(RECM_BCKSEQ),
                       varGet(RECM_BCKTYPE));
*/
   hDF=malloc(sizeof(RECMDataFile));
   
   zip_error_init(&hDF->zipError);
   int zip_rc=0;
   hDF->pieceNumber=pieceNumber;
   hDF->compLevel=compLevel;
   hDF->zipHandle = zip_open(tarFileName, ZIP_CREATE | ZIP_EXCL, &zip_rc);
   if (hDF->zipHandle == NULL)
   {
      ERROR(ERR_CREATEFILE,"Could not create RECM file '%s' (errno %d)\n",tarFileName,errno);
      free(hDF);
      return(NULL);
   };
   zip_register_progress_callback_with_state(hDF->zipHandle,0.1, &recm_progress_callback, &recm_ud_free, hDF);
   hDF->tarFileName=strdup(tarFileName);
   _internalRECMaddPiece(hDF);
   
   VERBOSE("Creating piece '%s'\n",hDF->tarFileName); 
   TRACE("ARGS(tarFileName='%s') -- RETURN (int) true\n",tarFileName);
   return(hDF);
}

/********************************************************************************/
/* Add a directory in RECM file                                                 */
/********************************************************************************/
int RECMaddDirectory(RECMDataFile *hDF,const char *directoryName,mode_t st_mode, uid_t st_uid, gid_t st_gid)
{
   char noRootDirectory[1024];
   /* Set the maskCreate code here */
   char msk[20];
   memset(msk,0x00,20);
   if ((st_mode & S_IRUSR) == S_IRUSR) strcat(msk,"R");
   if ((st_mode & S_IWUSR) == S_IWUSR) strcat(msk,"W");
   if ((st_mode & S_IXUSR) == S_IXUSR) strcat(msk,"X");
   strcat(msk,":");
   if ((st_mode & S_IRGRP) == S_IRGRP) strcat(msk,"R");
   if ((st_mode & S_IWGRP) == S_IWGRP) strcat(msk,"W");
   if ((st_mode & S_IXGRP) == S_IXGRP) strcat(msk,"X");
   strcat(msk,":");            
   if ((st_mode & S_IROTH) == S_IROTH) strcat(msk,"R");
   if ((st_mode & S_IWOTH) == S_IWOTH) strcat(msk,"W");
   if ((st_mode & S_IXOTH) == S_IXOTH) strcat(msk,"X");

   int pgdata_length=strlen(varGet(GVAR_CLUPGDATA));
   char *d=(char *)directoryName;
   d+=pgdata_length;
   memset(noRootDirectory,0x00,1024);
   strcpy(noRootDirectory,".");
   strcat(noRootDirectory,d);
   TRACE("Directory '%s' ['%s'(%u) / %u / %u] Recm:'%s'\n",directoryName,msk,st_mode,st_uid,st_gid,noRootDirectory);
   zip_int64_t index_zip_file=zip_dir_add(hDF->zipHandle, noRootDirectory, ZIP_FL_OVERWRITE|ZIP_FL_ENC_GUESS);
   if (index_zip_file < 0) 
   {
      ERROR(ERR_ZIPADDDIR,"Could not add directory '%s': %s\n",directoryName,zip_strerror(hDF->zipHandle));
      return(false);
   }
   
   // Add file properties ***************************************************

   char fileComment[30];
   sprintf(fileComment,"RECM#%s#%u#%u#",msk,st_uid,st_gid);
   int rc=zip_file_set_comment(hDF->zipHandle, index_zip_file,(char *)fileComment, strlen(fileComment)+1,ZIP_FL_ENC_GUESS);
   if (rc < 0)
   {
      ERROR(ERR_ZIPADDFILE,"Could not add directory '%s': %s [b]\n",directoryName,zip_strerror(hDF->zipHandle));
      TRACE("RETURN (int) false\n");
      return(false);
   }
   else
   {
      TRACE("Directory '%s' : setcomment='%s' [rc=%d]\n",directoryName,(char *)&fileComment,rc);
   }
   TRACE("RETURN (int) true\n");
   return(true);
}

/********************************************************************************/
/* Add a file in RECM file                                                      */
/********************************************************************************/
int RECMaddFile(RECMDataFile *hDF,const char *fileName,const char *fullName,const char *rootdir, mode_t st_mode, uid_t st_uid, gid_t st_gid)
{
   TRACE("ARGS(fileName='%s' fullName='%s' rootdir='%s')\n",fileName,fullName,rootdir);
   int rc=0;

   char noRootFileName[1024];
   char msk[20];
   memset(msk,0x00,20);
   if ((st_mode & S_IRUSR) == S_IRUSR) strcat(msk,"R");
   if ((st_mode & S_IWUSR) == S_IWUSR) strcat(msk,"W");
   if ((st_mode & S_IXUSR) == S_IXUSR) strcat(msk,"X");
   strcat(msk,":");
   if ((st_mode & S_IRGRP) == S_IRGRP) strcat(msk,"R");
   if ((st_mode & S_IWGRP) == S_IWGRP) strcat(msk,"W");
   if ((st_mode & S_IXGRP) == S_IXGRP) strcat(msk,"X");
   strcat(msk,":");            
   if ((st_mode & S_IROTH) == S_IROTH) strcat(msk,"R");
   if ((st_mode & S_IWOTH) == S_IWOTH) strcat(msk,"W");
   if ((st_mode & S_IXOTH) == S_IXOTH) strcat(msk,"X");

   char *d=(char *)fullName;
   d+=strlen(rootdir);
   memset(noRootFileName,0x00,1024);
   strcpy(noRootFileName,".");
   strcat(noRootFileName,d);

   TRACE("File '%s' ['%s'(%u) / %u / %u] Recm:'%s'\n",fileName,msk,st_mode,st_uid,st_gid,noRootFileName);
   
   zip_source_t *source = zip_source_file(hDF->zipHandle, fullName, 0, 0);
   if (source == NULL) 
   {
      ERROR(ERR_ZIPADDFILE,"Could not add file '%s'(%s): %s [a]\n",fileName,fullName,zip_strerror(hDF->zipHandle));
      TRACE("RETURN (int) false\n");
      return(false);
   }
   zip_int32_t index_zip_file=zip_file_add(hDF->zipHandle, noRootFileName, source,ZIP_FL_ENC_GUESS);
   if (index_zip_file < 0)
   {
      zip_source_free(source);
      ERROR(ERR_ZIPADDFILE,"Could not add file '%s'(%s): %s [b]\n",fileName,fullName,zip_strerror(hDF->zipHandle));
      TRACE("RETURN (int) false\n");
      return(false);
   }
   else
   {
      //Compression level set to '-1' to disable compression *******************
      if (hDF->compLevel >= 0)
      {
         if (hDF->compLevel > 9) hDF->compLevel=9;
         rc=zip_set_file_compression(hDF->zipHandle, index_zip_file, zip_method,hDF->compLevel);
         TRACE("File_compression result=%d (level %d)\n",rc,hDF->compLevel);
      }

      // Add file properties ***************************************************
      char fileComment[30];
      sprintf(fileComment,"RECM#%s#%u#%u#",msk,st_uid,st_gid);
      rc=zip_file_set_comment(hDF->zipHandle, index_zip_file, fileComment, strlen(fileComment),ZIP_FL_ENC_GUESS);
      if (rc < 0)
      {
         ERROR(ERR_ZIPADDFILE,"Could not add file '%s': %s [c]\n",fileName,zip_strerror(hDF->zipHandle));
         TRACE("RETURN (int) false\n");
         return(false);
      }
      else
      {
         TRACE("File '%s' : setcomment='%s'\n",fileName,(char *)&fileComment);
      }
   }
   TRACE("RETURN (int) true\n");
   return(true);
}

/********************************************************************************/
/* Close RECM file                                                              */
/********************************************************************************/
int RECMclose(RECMDataFile *hDF,long itemSize,long itemCnt)
{
   int rc=0;
   rc=zip_close(hDF->zipHandle);
   if (rc < 0)
   {
      zip_error_t *ze=&hDF->zipError;
      ze=zip_get_error(hDF->zipHandle);
      printf("zip_error_strerror=%s\n",zip_error_strerror(ze));
      return(false);
   };
   hDF->zipHandle=NULL;
   return(true);
}

/* separated code to be used by threads */
int RECMUpdateDepositAtClose(RECMDataFile *hDF,long itemSize,long itemCnt)
{
   long zipItemSize=0;
   if (hDF->compLevel >= 0)                                                                                                        //Compression level set to '-1' to disable compression
   {
      struct stat st;      
      int rcs=stat(hDF->tarFileName, &st);                                                                                         // Get file properties
      if (rcs != 0) 
      {
         rcs=errno;
         TRACE("Stat file '%s' error (errno %d - %s)\n",hDF->tarFileName,rcs,strerror(rcs));
         return(false);
      };
      zipItemSize=st.st_size;                                                                                                      // IN BYTES 
      float ratio=zipItemSize;
      ratio=100 - ((ratio/itemSize)*100);
      VERBOSE ("Backup Piece %s FileSize=%ld (Compression ratio: %2.2f%%)\n",hDF->tarFileName,zipItemSize,ratio);
   };
   char *query=malloc(1024);
   sprintf(query,"insert into %s.backup_pieces (cid,bck_id,pcid,pname,psize,csize,pcount) values (%s,'%s',%d,'%s',%ld,%ld,%ld)",
                 varGet(GVAR_DEPUSER),
                 varGet(GVAR_CLUCID),
                 varGet(RECM_BCKID),
                 hDF->pieceNumber,
                 extractFileName(hDF->tarFileName),itemSize,zipItemSize,itemCnt);
   int rc=DEPOquery(query,0);
   DEPOqueryEnd();
   
   free(query);
   TRACE("ARGS(itemSize=%ld itemCnt=%ld) --> RETURN (int) %d\n",itemSize,itemCnt,rc);
   if (hDF->zipHandle == NULL && hDF->tarFileName != NULL) free(hDF->tarFileName);
   free(hDF);
   return(rc == 0);
}
