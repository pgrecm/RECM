int RECMopenRead(const char *recmFileName)
{
   
   int zip_rc=0;
   zipHandle = zip_open(recmFileName, 0, &zip_rc);
   if (zipHandle == NULL) { ERROR(ERR_ERROPNZIP,"Could not open file '%s'\n",recmFileName); return(false); };
   TRACE("ARGS(recmFileName='%s') --> RETURN (int) true\n",recmFileName);
   return(true);
}

long RECMGetEntriesRead()
{
   if (zipHandle == NULL) { TRACE("return (long) -1\n");return(-1); };
   long rc=zip_get_num_entries(zipHandle, 0);
   TRACE("ARGS(none) --> RETURN (long) %ld\n",rc);
   return(rc);
}

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

void RECMlistContent()
{
   TRACE("ARGS(none)\n");
   struct zip_stat sb;
   char *lastmod=malloc(50);
   printf("      Content:\n");
   printf("          %-20s %10s %s\n","mdate","Size","File name");
   printf("          %-20s %10s %s\n","--------------------","----------","----------------------------------------");
   int index=1;
   int i=0;
   while( i < zip_get_num_entries(zipHandle, 0))
   {
      if (zip_stat_index(zipHandle, i, 0, &sb) == 0)
      {
         char *x=(char *)sb.name;
         x+=strlen(x)-1;
         if (x[0] != '/') 
         { // this is file, not a directory
            strftime(lastmod, 49, "%Y-%m-%d %H:%M:%S", localtime(&sb.mtime));
            printf("     %3d] %-20s %10llu %s\n",index,lastmod,sb.size,sb.name);
            index++;
         }
      }
      i++;
   }
}

void RECMcloseRead()
{
   TRACE("ARGS(none)\n");
   if (zipHandle != NULL) { zip_close(zipHandle); };
   zipHandle=NULL;
}

int RECMgetIndex(int ndx,struct zip_stat *sb)
{
   if (zip_stat_index(zipHandle, ndx, 0, sb) == 0) { TRACE("ARGS(ndx=%d) --> RETURN (int) true\n",ndx); return(true); }
                                              else { TRACE("ARGS(ndx=%d) --> RETURN (int) true\n",ndx); return(false);};
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

void RECMaddPiece(const char *pname,int id)
{
   TRACE("ARGS(pname='%s' id=%d)\n",pname,id);
   if (zip_filelist == NULL) zip_filelist=arrayCreate(ARRAY_NO_ORDER);
   ZIPPiece *zp=malloc(sizeof(ZIPPiece));
   zp->id=id;
   zp->pname=strdup(pname);
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

RecmFileItemProperties *RECMGetFileProperties(int file_index)
{
   zip_uint32_t datlen;
   char *data=(char *)zip_file_get_comment(zipHandle, file_index, &datlen, ZIP_FL_ENC_GUESS);
   if (data == NULL) { TRACE("ARGS(file_index=%d) --> RETURN (itemdata) NULL\n",file_index); return(NULL); };                 // NOT a RECM file
   // sprintf(fileComment,"RECM:%uld:%uld:%uld:",st.mode,st.st_uid,st_gid);
   char *item=data;
   char *sep=strchr(item,'#'); // First string is 'RECM'
   if (sep == NULL) { TRACE("ARGS(file_index=%d) --> RETURN (itemdata) NULL\n",file_index); return(NULL); };                  // NOT a RECM file
   sep[0]=0x00;
   if (strcmp(item,"RECM") != 0) { TRACE("ARGS(file_index=%d) --> RETURN (itemdata) NULL\n",file_index); return(NULL); };     // NOT a RECM file (bad signature)
   RecmFileItemProperties *itemData=(RecmFileItemProperties *)malloc(sizeof(RecmFileItemProperties));
   sep++;item=sep;
   sep=strchr(item,'#'); // Second string is 'mode'
   sep[0]=0x00;
   itemData->mode=maskTranslate(item);
   sep++;item=sep;
   sep=strchr(item,'#'); // third string is 'UID'
   sep[0]=0x00;
   itemData->uid=atoi(item);
   sep++;item=sep;
   sep=strchr(item,'#'); // forth string is 'GID'
   sep[0]=0x00;
   itemData->gid=atoi(item);
   TRACE("ARGS(file_index=%d) --> RETURN (itemdata): file_index=%d mode=%d : uid=%u : gid=%u\n",file_index,file_index,itemData->mode,itemData->uid,itemData->gid);
   return(itemData);
}

RecmFileItemProperties *RECMGetFilePropertiesByName(const char *filename)
{
   TRACE("ARGS(filename='%s')\n",filename);
   if (zipHandle == NULL) return (NULL);                                        // RECM file not open
   int file_index=zip_name_locate(zipHandle, filename, ZIP_FL_ENC_GUESS);
   if( file_index < 0) return (NULL);                                           // File not found
   return (RECMGetFileProperties(file_index));
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
   if (inzc == NULL)   { zip_close(zipHdl);VERBOSE("File '%s' is not a recm file.\n",filename);return(NULL); };
   strcpy(zip_comment,inzc);
   zip_close(zipHdl);
   TRACE("GET PROP='%s'\n",zip_comment);
   RecmFile*result=malloc(sizeof(RecmFile));
   result->processed=0;
   struct stat st;      
   stat(filename,&st);
   result->filename=strdup(filename);
   /* structure of the comment :
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
   inzc=zip_comment;
   char *pdash=strchr(inzc,'#');                                                 // backup_id
   if (pdash == NULL) { return(NULL); }
   pdash[0]=0x00;pdash++;
   result->bck_id=strdup(inzc);inzc=pdash;
   pdash=strchr(inzc,'#');
//   TRACE("result->bck_id='%s'\n",result->bck_id);
   if (pdash == NULL) { return(NULL); }
   pdash[0]=0x00;pdash++;
   result->piece=atoi(inzc);inzc=pdash;pdash=strchr(inzc,'#');                  // Piece number
//   TRACE("result->piece='%d'\n",result->piece);
   if (pdash == NULL) { return(NULL); }
   pdash[0]=0x00;pdash++;
   result->bcktyp=strdup(inzc);inzc=pdash;pdash=strchr(inzc,'#');              // Backup type
//   TRACE("result->bcktyp='%s'\n",result->bcktyp);
   if (pdash == NULL) { return(NULL); }
   pdash[0]=0x00;pdash++;
   result->version=atoi(inzc);inzc=pdash;pdash=strchr(inzc,'#');                // PG Version (PG_VERSION content)  at backup time
//   TRACE("result->version='%d'\n",result->version);
   if (pdash == NULL) { return(NULL); }
   pdash[0]=0x00;pdash++;
   result->timeline=atoi(inzc);inzc=pdash;pdash=strchr(inzc,'#');               // Timeline at backup time 
//   TRACE("result->timeline='%d'\n",result->timeline);
   if (pdash == NULL) { return(NULL); }
   pdash[0]=0x00;pdash++;
   result->bcktag=strdup(inzc);inzc=pdash;pdash=strchr(inzc,'#');               // Backup TAG
//   TRACE("result->bcktag='%s'\n",result->bcktag);
   if (pdash == NULL) { return(NULL); }
   pdash[0]=0x00;pdash++;
   result->bcksize=atol(inzc);inzc=pdash;pdash=strchr(inzc,'#');                // Backup size
//   TRACE("result->piece='%ld'\n",result->bcksize);
   result->zipsize=st.st_size;                                                  // Size of the ZIP file
//   TRACE("result->zipsize='%ld'\n",result->zipsize);
   if (pdash == NULL) { return(NULL); }
   pdash[0]=0x00;pdash++;
   result->pcount=atoi(inzc);inzc=pdash;pdash=strchr(inzc,'#');                 // Backup piece count
//   TRACE("result->pcount='%ld'\n",result->pcount);
   if (pdash == NULL) { return(NULL); }
   pdash[0]=0x00;pdash++;
   result->begin_date=strdup(inzc);inzc=pdash;pdash=strchr(inzc,'#');           // Begin backup date
//   TRACE("result->begin_date='%s'\n",result->begin_date);
   if (pdash == NULL) { return(NULL); }
   pdash[0]=0x00;pdash++;
   result->begin_wall=strdup(inzc);inzc=pdash;pdash=strchr(inzc,'#');           // LSN when backup start
//   TRACE("result->begin_wall='%s'\n",result->begin_wall);
   if (pdash == NULL) { return(NULL); }
   pdash[0]=0x00;pdash++;
   result->begin_walf=strdup(inzc);inzc=pdash;pdash=strchr(inzc,'#');           // WAL file    
//   TRACE("result->begin_walf='%s'\n",result->begin_walf);
   if (pdash == NULL) { return(NULL); }
   pdash[0]=0x00;pdash++;
   result->end_date=strdup(inzc);inzc=pdash;pdash=strchr(inzc,'#');             // End backup date
//   TRACE("result->end_date='%s'\n",result->end_date);
   if (pdash == NULL) { return(NULL); }
   pdash[0]=0x00;pdash++;
   result->end_wall=strdup(inzc);inzc=pdash;pdash=strchr(inzc,'#');             // LSN when backup end
//   TRACE("result->end_wall='%s'\n",result->end_wall);
   if (pdash == NULL) { return(NULL); }
   pdash[0]=0x00;pdash++;
   result->end_walf=strdup(inzc);inzc=pdash;pdash=strchr(inzc,'#');             // WAL file    
//   TRACE("result->end_walf='%s'\n",result->end_walf);
   if (pdash == NULL) { return(NULL); }
   pdash[0]=0x00;pdash++;
//   TRACE("last : '%s'\n",inzc);
   result->options=atol(inzc);                                                 // /noinde option is set    
/*
   TRACE("-A-\n");
   TRACE("result->options=%ld\n",result->options);
   TRACE("-B-\n");
   
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
*/
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

/********************************************************************************/
/* Create RECM file                                                             */
/********************************************************************************/
int RECMcreate()
{
   if (tarFileName == NULL) tarFileName=malloc(1024);
   sprintf(tarFileName,"%s/%s_%s_%s.recm",
                       varGet(GVAR_BACKUPDIR),
                       varGet(RECM_BCKID),
                       varGet(RECM_BCKSEQ),
                       varGet(RECM_BCKTYPE));
   zip_error_init(&zipError);
   int zip_rc=0;
   zipHandle = zip_open(tarFileName, ZIP_CREATE | ZIP_EXCL, &zip_rc);
   if (zipHandle == NULL) { ERROR(ERR_CREATEFILE,"Could not create file '%s' (errno %d)\n",tarFileName,errno);return(false);};
   RECMaddPiece(tarFileName,varGetInt(RECM_BCKSEQ));
   VERBOSE("Creating piece '%s'\n",tarFileName); 
   varAdd(SYSVAR_CURRENT_RECMFILE,tarFileName);
   TRACE("ARGS(none) -- RETURN (int) true\n");
   return(true);
}

int RECMaddDirectory(const char *directoryName)
{
   
   if (zipHandle == NULL) 
   { 
      if (RECMcreate() == false) 
      { 
         TRACE("ARGS(directoryName ='%s') -- RETURN (int) false\n",directoryName);
         return(false);
      }; 
   };
   char *noRootDirectory=malloc(1024);
   char *rootdir=varGet(GVAR_CLUPGDATA);
   char *d=(char *)directoryName;
   d+=strlen(rootdir);
   strcpy(noRootDirectory,".");
   strcat(noRootDirectory,d);
   TRACE("Root directory is '%s'\n",noRootDirectory);
   int index_zip_file=zip_dir_add(zipHandle, noRootDirectory, ZIP_FL_ENC_GUESS);
   if (index_zip_file < 0) 
   {
      ERROR(ERR_ZIPADDDIR,"Could not add directory '%s': %s\n",directoryName,zip_strerror(zipHandle));
      return(false);
   }
   
   // Add file properties ***************************************************
   struct stat st;
   stat(directoryName,&st);
   
   char *fileComment=malloc(256);
   sprintf(fileComment,"RECM#%s#%u#%u#",maskCreate(st.st_mode),st.st_uid,st.st_gid);
   zip_file_set_comment(zipHandle, index_zip_file, fileComment, strlen(fileComment),ZIP_FL_ENC_GUESS);
   free(fileComment);

   free(noRootDirectory);
   TRACE("ARGS(directoryName ='%s') -- RETURN (int) true\n",directoryName);
   return(true);
}

int RECMaddFile(const char *fileName,const char *fullName,const char *rootdir)
{
   int rc=0;
   char *noRootFileName=malloc(1024);
   char *d=(char *)fullName;
   d+=strlen(rootdir);
   strcpy(noRootFileName,".");
   strcat(noRootFileName,d);
   if (zipHandle == NULL) 
   { 
      if (RECMcreate() == false)
      {
         TRACE("ARGS(fileName='%s' fullName='%s' rootdir='%s') -- RETURN (int) false\n",fileName,fullName,rootdir);
         return(false);
      }; 
   };
   zip_source_t *source = zip_source_file(zipHandle, fullName, 0, 0);
   if (source == NULL) 
   {
      ERROR(ERR_ZIPADDFILE,"Could not add file '%s': %s [a]\n",fileName,zip_strerror(zipHandle));
      free(noRootFileName);
      return(false);
   }
   zip_int32_t index_zip_file=zip_file_add(zipHandle, noRootFileName, source,ZIP_FL_ENC_GUESS);
   if (index_zip_file < 0)
   {
      zip_source_free(source);
      ERROR(ERR_ZIPADDFILE,"Could not add file '%s': %s [b]\n",fileName,zip_strerror(zipHandle));
      free(noRootFileName);
      return(false);
   }
   else
   {
      //Compression level set to '-1' to disable compression *******************
      if (varGetInt(GVAR_COMPLEVEL) >= 0)
      {
         zip_uint32_t compression_level=varGetInt(GVAR_COMPLEVEL);
         if (compression_level < 0) compression_level=0;
         if (compression_level > 9) compression_level=9;
         rc=zip_set_file_compression(zipHandle, index_zip_file, zip_method,compression_level); /* Change : 0xA000D */
         TRACE("zip_set_file_compression result=%d (level %d)\n",rc,compression_level);
      }

      // Add file properties ***************************************************
      struct stat st;
      stat(fullName,&st);
      
      char *fileComment=malloc(128);
      sprintf(fileComment,"RECM#%s#%u#%u#",maskCreate(st.st_mode),st.st_uid,st.st_gid);
      rc=zip_file_set_comment(zipHandle, index_zip_file, fileComment, strlen(fileComment),ZIP_FL_ENC_GUESS);
      
      if (rc < 0)
      {
         ERROR(ERR_ZIPADDFILE,"Could not add file '%s': %s [b]\n",fileName,zip_strerror(zipHandle));
         free(fileComment);
         free(noRootFileName);
         return(false);
      };
      free(fileComment);
   }
   free(noRootFileName);
   TRACE("ARGS(fileName='%s' fullName='%s' rootdir='%s') -- RETURN (int) true\n",fileName,fullName,rootdir);
   return(true);
}

int RECMclose(long itemSize,long itemCnt)
{
   int rc=0;
   if (zipHandle != NULL) { rc=zip_close(zipHandle);TRACE("ZIPCLOSE:%d\n",rc); };
   zipHandle=NULL;
   
   long zipItemSize=0;
   
   //Compression level set to '-1' to disable compression *******************
   if (varGetInt(GVAR_COMPLEVEL) >= 0)
   {
      struct stat st;      
      stat(tarFileName, &st);                                                        // Get file properties
      zipItemSize=st.st_size;                                                        // IN BYTES 
      float ratio=zipItemSize;
      ratio=100 - ((ratio/itemSize)*100);
      VERBOSE ("Backup Piece %s FileSize=%ld (Compression ratio: %2.2f%%)\n",tarFileName,zipItemSize,ratio);
   }
   char *query=malloc(1024);
   sprintf(query,"insert into %s.backup_pieces (cid,bck_id,pcid,pname,psize,csize,pcount) values (%s,'%s',%s,'%s',%ld,%ld,%ld)",
                 varGet(GVAR_DEPUSER),
                 varGet(GVAR_CLUCID),
                 varGet(RECM_BCKID),
                 varGet(RECM_BCKSEQ),
                 extractFileName(tarFileName),itemSize,zipItemSize,itemCnt);
   TRACE ("Update bp:%s\n",query);
   PGresult   *res = PQexec(rep_cnx,query);
   int rr=PQresultStatus(res);
   if (PQresultStatus(res) != PGRES_COMMAND_OK)
   {
      PQclear(res);
      free(query);
      ERROR(ERR_DBQRYERROR,"Deposit query error.\nError:%s\n",PQerrorMessage(rep_cnx));
      return(false);
   };
   PQclear(res);
   free(query);
   if (tarFileName != NULL) { free(tarFileName); };
   tarFileName = NULL;
   TRACE("ARGS(itemSize=%ld itemCnt=%ld) -- RETURN (int) %d\n",itemSize,itemCnt,rc);
   return(rc == 0);
}
