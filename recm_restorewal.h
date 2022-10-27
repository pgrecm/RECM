/**********************************************************************************/
/* RESTORE WAL command                                                            */
/* Usage:                                                                         */
/*    restore wal                                                                 */                 
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
/*         /first=<WALNAME>           Restore all WAL after the given WAL         */
/*         /last=<WALNAME>            Restore all WAL until the given WAL         */
/**********************************************************************************/
/*
@command restore wal
@definition
Restore WAL file(s) backuped by '{&backup wal&}' command.
The '/directory' allow you to restore files onto a different folder.
By default, the files are restored onto the WAL directory ({&modify cluster&}, qualifier '/waldir')
(See {&backup wal&} command for more details)


@option "/overwrite"               "Overwrite existing file"                           
@option "/verbose"                 "Display more details"
@option "/cid=STRING"              "use a backup from a different cluster"  
@option "/uid=STRING"              "use a specified backup"                  
@option "/file=FILE"               "Select file to restore"                  
@option "/directory=PATH"          "Destination folder"                      
@option "/before=DATE"             "Find a backup with a given date before"  
@option "/after=DATE"              "Find a backup with a given date after "  
@option "/first=WALNAME"           "Restore all WAL after the given WAL"
@option "/last=WALNAME"            "Restore all WAL until the given WAL"
@example
@inrecm restore wal /first="00000024000000730000005A" /verbose /dir="/tmp" 
@out recm-inf: Restoring '/tmp/00000024000000730000005A'
@out recm-inf: WAL '/00000024000000730000005A' already on disk (/tmp/00000024000000730000005A)
@out recm-inf: WAL '/00000024000000730000005A' already on disk (/tmp/00000024000000730000005A)
@out recm-inf: Restoring '/tmp/00000024000000730000005B'
@out recm-inf: WAL '/00000024000000730000005B' already on disk (/tmp/00000024000000730000005B)
@out recm-inf: WAL '/00000024000000730000005B' already on disk (/tmp/00000024000000730000005B)
@out recm-inf: Restoring '/tmp/00000024000000730000005C'
@out recm-inf: Restoring '/tmp/00000024000000730000005C.00000028.backup'
@out recm-inf: Restoring '/tmp/00000024000000730000005C.00000028.backup'
@out recm-inf: Restoring '/tmp/00000024000000730000005C'
@out recm-inf: Restoring '/tmp/00000024000000730000005C.00000028.backup'
@out recm-inf: Restoring '/tmp/00000024000000730000005C'
@out recm-inf: Restoring '/tmp/00000024000000730000005D'
@out recm-inf: WAL '/00000024000000730000005D' already on disk (/tmp/00000024000000730000005D)
@out recm-inf: Restoring '/tmp/00000024000000730000005E'
@out recm-inf: WAL '/00000024000000730000005E' already on disk (/tmp/00000024000000730000005E)
@out recm-inf: Restoring '/tmp/00000024000000730000005E.00000028.backup'
@out recm-inf: Restoring '/tmp/00000024000000730000005F'
@out recm-inf: WAL '/00000024000000730000005E' already on disk (/tmp/00000024000000730000005E.00000028.backup)
@out recm-inf: WAL '/00000024000000730000005F' already on disk (/tmp/00000024000000730000005F)
@out recm-inf: Restoring '/tmp/000000240000007300000060'
@out
@inrecm
@end
*/
void COMMAND_RESTOREWAL(int idcmd,char *command_line)
{
   if (DEPOisConnected(true) == false) return;
   int cluster_id;

   if (optionIsSET("opt_verbose") == true) globalArgs.verbosity=true;                                                              // Set Verbosity

   memBeginModule();
   char *query=memAlloc(1024);
   
   // If /source is not specified, WAL directory must be defined
   if (qualifierIsUNSET("qal_source") == true)
   {
      if (varExist(GVAR_WALDIR) == false)
      {
         ERROR(ERR_NOWALDIR,"WAL directory not set.(See show cluster).\n");
         memEndModule();
         return;
      }
      cluster_id=varGetInt(GVAR_CLUCID);
   }
   else
   {
      if (qualifierIsUNSET("qal_directory") == true)
      { 
         ERROR(ERR_MISQUALVAL,"Qualifier '/directory' is mandatory with '/source'.\n"); 
         memEndModule();
         return;
      };
      // Need to get CID of source cluster
      sprintf(query,"select count(*) from %s.clusters where clu_nam='%s'",
                       varGet(GVAR_DEPUSER),
                       varGet("qal_source"));
      int rows=DEPOquery(query,0);
      if (DEPOgetInt(0,0) == 0)
      {
         ERROR(ERR_BADCLUNAME,"Unknown cluster name '%s'•\n",varGet("qal_source"));
         DEPOqueryEnd();
         memEndModule();
         return;
      }
      DEPOqueryEnd();
   
      sprintf(query,"select CID from %s.clusters where clu_nam='%s'",
                       varGet(GVAR_DEPUSER),
                       varGet("qal_source"));
      rows=DEPOquery(query,0);
      cluster_id=DEPOgetInt(0,0);
      TRACE("cluster_id=%d\n",cluster_id);
      DEPOqueryEnd();
   }
   char *destination_folder=memAlloc(1024);

   if (qualifierIsUNSET("qal_directory") == false)
   { strcpy(destination_folder,varGet("qal_directory")); }
   else
   { strcpy(destination_folder,varGet(GVAR_WALDIR)); };

   if (directory_exist(destination_folder) == false) 
   { mkdirs(destination_folder,maskTranslate(varGet(GVAR_CLUMASK_BACKUP))); };

   TRACE("destination_folder=%s\n",destination_folder);

   long restored=0;
   long datablock_size=getHumanSize(GVAR_CLUREADBLOCKSIZE);                       // Allocate a buffer. Size is coming from variable 'GVAR_CLUREADBLOCKSIZE'
   if (datablock_size < 1024) datablock_size=163840;
   char *datablock=memAlloc(datablock_size);


   char *qry_first=memAlloc(512);  qry_first[0]=0x00; 
   char *qry_last=memAlloc(512);   qry_last[0]=0x00; 
   char *qry_before=memAlloc(512); qry_before[0]=0x00; 
   char *qry_after=memAlloc(512);  qry_after[0]=0x00; 
   char *qry_id=memAlloc(512);     qry_id[0]=0x00; 
   char *qry_file=memAlloc(1024);  qry_file[0]=0x00; 
   char *zipfile=memAlloc(1024);   zipfile[0]=0x00; 
   char *outfile=memAlloc(1024);   outfile[0]=0x00; 
   char *walfile=memAlloc(512);    walfile[0]=0x00; 

   if (qualifierIsUNSET("qal_first")== false)
   { sprintf(qry_first," and bw.edate >= (select min(bw2.edate) from %s.backup_wals bw2 where bw2.cid=%d and upper(bw2.walfile) like upper('%s%%'))",
                       varGet(GVAR_DEPUSER),
                       cluster_id,
                       varGet("qal_first"));
   };
   if (qualifierIsUNSET("qal_last")== false)
   { sprintf(qry_last, " and bw.edate <= (select max(bw3.edate) from %s.backup_wals bw3 where bw3.cid=%d and upper(bw3.walfile) like upper('%s%%'))",
                       varGet(GVAR_DEPUSER),
                       cluster_id,
                       varGet("qal_last"));
   };

   if (qualifierIsUNSET("qal_before")== false)
   { sprintf(qry_before," and bw.edate < to_timestamp('%s','%s')",varGet("qal_before"),varGet(GVAR_CLUDATE_FORMAT)); };

   if (qualifierIsUNSET("qal_after") == false)
   { sprintf(qry_after," and bw.edate > to_timestamp('%s','%s')",varGet("qal_after"),varGet(GVAR_CLUDATE_FORMAT)); };

   if (qualifierIsUNSET("qal_uid") == false)
   { sprintf(qry_id," and bp.bck_id='%s'",varGet("qal_uid")); }; 

   if (qualifierIsUNSET("qal_file") == false)
   { sprintf(qry_file," and upper(bw.walfile) like upper('%s%%')",varGet("qal_file")); }; 

   sprintf(query,"select bw.bck_id,'./'||bw.walfile,to_char(bw.edate,'%s'),bp.pname,b.bckdir from %s.backup_wals bw"
                 "  left join %s.backup_pieces bp on (bp.cid=bw.cid and bw.bck_id=bp.bck_id)"
                 "  left join %s.backups       b  on (b.cid=bw.cid  and bw.bck_id=b.bck_id) "
                 " where bw.cid=%d and b.bckdir is not null ",
                 varGet(GVAR_CLUDATE_FORMAT),
                 varGet(GVAR_DEPUSER),
                 varGet(GVAR_DEPUSER),
                 varGet(GVAR_DEPUSER),
                 cluster_id);
   strcat(query,qry_before);
   strcat(query,qry_after);
   strcat(query,qry_id);
   strcat(query,qry_file);
   strcat(query,qry_first);
   strcat(query,qry_last);
   strcat(query,"order by bw.edate");

   int wal_rows=DEPOquery(query,1);
      if (wal_rows == 0)
   {
      INFO("No WAL backup found.\n");
      memEndModule();
      return;
   }
   
   for(int i=0;i < wal_rows;i++)
   {
      strcpy(walfile,DEPOgetString(i,1));
      sprintf(zipfile,"%s/%s",DEPOgetString(i,4),DEPOgetString(i,3));
      
      TRACE("bckid=%s file=%s RECMfile=%s\n",DEPOgetString(i,0),DEPOgetString(i,1),zipfile);
      char *slash=strchr(walfile,'/');
      slash++;
      sprintf(outfile,"%s/%s",destination_folder,slash);
      if (file_exists(outfile) == true)
      {
         char *wf=strstr(walfile,"./");
         if (wf != NULL) { wf++; } else { wf=walfile;};
         char *remove_ext=strstr(wf,".");
         if (remove_ext != NULL) remove_ext[0]=0x00;
         VERBOSE("WAL '%s' already on disk (%s)\n",wf,outfile);
        continue;
      };
      VERBOSE("Restoring '%s'\n",outfile);
      RECMDataFile *hDF=RECMopenRead(zipfile);
      if (hDF == NULL)
      {
         ERROR(ERR_INVRECMFIL,"Corrupted or invalid RECM file '%s'\n",zipfile);
         memEndModule();
         return;
      }
      struct zip_stat sb;
      struct zip_file *zf;
      int file_index=zip_name_locate(hDF->zipHandle, walfile, ZIP_FL_ENC_GUESS);     // Locate file in RECM file
      if (zip_stat_index(hDF->zipHandle,file_index, 0, &sb) == 0)
      {
         TRACE ("scan '%s' == '%s'\n",sb.name,walfile);
         if (strstr(sb.name,walfile) != NULL)
         {
            RecmFileItemProperties *fileprop=RECMGetFileProperties(hDF,file_index);
            TRACE("fileprop:mode='%s' / uid=%d / gid=%d\n",maskCreate(fileprop->mode),fileprop->uid,fileprop->gid);
            zf = zip_fopen_index(hDF->zipHandle,file_index, 0);
            if (!zf) 
            {
               ERROR(ERR_INVRECMFIL, "Invalid file '%s': %s\n",zipfile,zip_strerror(hDF->zipHandle));
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
                    ERROR(ERR_BADPIECE, "Cannot process piece '%s'\n",zipfile);
                    memEndModule();
                    return;
                }
                write(fd, datablock, len);
                sum += len;
            }
            close(fd);
            zip_fclose(zf);
            // Set file attributes like it was before to added into ZIP file
            struct utimbuf ut;
            ut.actime=sb.mtime;
            ut.modtime=sb.mtime;
            utime(outfile, &ut);
            //chmod(outfile,fileprop->mode);
            chown(outfile,fileprop->uid,fileprop->gid);
            restored++;
         }
      }
      RECMcloseRead(hDF);
   }
   DEPOqueryEnd();
   memEndModule();
}
