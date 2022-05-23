/**********************************************************************************/
/* export config command                                                          */
/* Usage:                                                                         */
/*      export config /cluster [/file=xxx]                                        */
/*      export config /deposit [/file=xxx]                                        */
/**********************************************************************************/
void COMMAND_EXPORTCONFIG(int idcmd,char *command_line)
{
   if (varGetLong(GVAR_CLUCID) == 0)
      WARN(WRN_NOTREGISTERED,"Not registered to any deposit.\n");

   memBeginModule();
   char *filename=memAlloc(1024);
   char *msg=memAlloc(1024);
   char *query=memAlloc(1024);
   FILE *fh=NULL;
   char r[4];
   int in_file=false;
   if (qualifierIsUNSET("qal_file") == false)
   {
      strcpy(filename,varGet("qal_file"));
      fh=fopen(filename, "w");
      in_file=(fh != NULL);
      if (fh == NULL) fh=stdout;
   }
   else
   {
      fh=stdout;
   }
   if (optionIsSET("opt_deposit") == true)
   {
      if (DEPOisConnected(true) == false) { memEndModule();return; };
      
      fprintf(fh,"#########################################################################\n");
      sprintf(msg,"# Export of deposit %s",varGet(GVAR_DEPNAME));
      while (strlen(msg) < 54) strcat(msg," ");
      strcat(msg,date_system("%Y-%m-%d %Hh%M"));
      strcat(msg," ##");
      fprintf(fh,"%s\n",msg);
      fprintf(fh,"#########################################################################\n");
      if (varGet(GVAR_DEPDESCR) != NULL && strcmp(varGet(GVAR_DEPDESCR),"(null)") != 0) fprintf(fh,"modify deposit/description=\"%s\";\n",varGet(GVAR_DEPDESCR));
      if (varGet(GVAR_DEPHOST)  != NULL && strcmp(varGet(GVAR_DEPHOST),"(null)")  != 0) fprintf(fh,"modify deposit/host=\"%s\";\n",       varGet(GVAR_DEPHOST));
      if (varGet(GVAR_DEPPORT)  != NULL && strcmp(varGet(GVAR_DEPPORT),"(null)")  != 0) fprintf(fh,"modify deposit/port=\"%s\";\n",       varGet(GVAR_DEPPORT));
      if (varGet(GVAR_DEPUSER)  != NULL && strcmp(varGet(GVAR_DEPUSER),"(null)")  != 0) fprintf(fh,"modify deposit/usr=\"%s\";\n",        varGet(GVAR_DEPUSER));
      if (varGet(GVAR_DEPPWD)   != NULL && strcmp(varGet(GVAR_DEPPWD),"(null)")   != 0) fprintf(fh,"modify deposit/pwd=\"%s\";\n",        varGet(GVAR_DEPPWD));
      if (varGet(GVAR_DEPDB)    != NULL && strcmp(varGet(GVAR_DEPDB),"(null)")    != 0) fprintf(fh,"modify deposit/db=\"%s\";\n",         varGet(GVAR_DEPDB));
      fprintf(fh,"\n");
   }
   if (optionIsSET("opt_cluster") == true)
   {
      if (CLUisConnected(true) == false) { memEndModule();return; };
      fprintf(fh,"#########################################################################\n");
      sprintf(msg,"# Export of cluster %s (CID=%s)",varGet(GVAR_CLUNAME),varGet(GVAR_CLUCID));
      while (strlen(msg) < 54) strcat(msg," ");
      strcat(msg,date_system("%Y-%m-%d %Hh%M"));
      strcat(msg," ##");
      fprintf(fh,"%s\n",msg);
      fprintf(fh,"#########################################################################\n");
      fprintf(fh,"register cluster/cid=%s;\n",varGet(GVAR_CLUCID));
      if (varGet(GVAR_CLUREADBLOCKSIZE)    != NULL && strcmp(varGet(GVAR_CLUREADBLOCKSIZE),"(null)")    != 0) fprintf(fh,"modify cluster/cid=%s/blksize=%s;\n",         varGet(GVAR_CLUCID),varGet(GVAR_CLUREADBLOCKSIZE));
      if (varGet(GVAR_CLUDATE_FORMAT)      != NULL && strcmp(varGet(GVAR_CLUDATE_FORMAT),"(null)")      != 0) fprintf(fh,"modify cluster/cid=%s/date_format=\"%s\";\n", varGet(GVAR_CLUCID),varGet(GVAR_CLUDATE_FORMAT));
      if (varGet(GVAR_CLUAUTODELWAL)       != NULL && strcmp(varGet(GVAR_CLUAUTODELWAL),"(null)")       != 0) fprintf(fh,"modify cluster/cid=%s/delwal=%s;\n",          varGet(GVAR_CLUCID),varGet(GVAR_CLUAUTODELWAL));
      if (varGet(GVAR_CLUBACKUP_MAXSIZE)   != NULL && strcmp(varGet(GVAR_CLUBACKUP_MAXSIZE),"(null)")   != 0) fprintf(fh,"modify cluster/cid=%s/maxsize=%s;\n",         varGet(GVAR_CLUCID),varGet(GVAR_CLUBACKUP_MAXSIZE));
      if (varGet(GVAR_CLUBACKUP_MAXFILES)  != NULL && strcmp(varGet(GVAR_CLUBACKUP_MAXFILES),"(null)")  != 0) fprintf(fh,"modify cluster/cid=%s/maxfiles=%s;\n",        varGet(GVAR_CLUCID),varGet(GVAR_CLUBACKUP_MAXFILES));
      if (varGet(GVAR_CLURETENTION_FULL)   != NULL && strcmp(varGet(GVAR_CLURETENTION_FULL),"(null)")   != 0) fprintf(fh,"modify cluster/cid=%s/full=\"%s\";\n",        varGet(GVAR_CLUCID),varGet(GVAR_CLURETENTION_FULL));
      if (varGet(GVAR_CLURETENTION_CONFIG) != NULL && strcmp(varGet(GVAR_CLURETENTION_CONFIG),"(null)") != 0) fprintf(fh,"modify cluster/cid=%s/cfg=\"%s\";\n",         varGet(GVAR_CLUCID),varGet(GVAR_CLURETENTION_CONFIG));
      if (varGet(GVAR_CLURETENTION_META)   != NULL && strcmp(varGet(GVAR_CLURETENTION_META),"(null)")   != 0) fprintf(fh,"modify cluster/cid=%s/meta=\"%s\";\n",        varGet(GVAR_CLUCID),varGet(GVAR_CLURETENTION_META));
      if (varGet(GVAR_CLUDESCR)            != NULL && strcmp(varGet(GVAR_CLUDESCR),"(null)")            != 0) fprintf(fh,"modify cluster/cid=%s/description=\"%s\";\n", varGet(GVAR_CLUCID),varGet(GVAR_CLUDESCR));
      if (varGet(GVAR_CLUUSER)             != NULL && strcmp(varGet(GVAR_CLUUSER),"(null)")             != 0) fprintf(fh,"modify cluster/cid=%s/usr=\"%s\";\n",         varGet(GVAR_CLUCID),varGet(GVAR_CLUUSER)); 
      if (varGet(GVAR_CLUPWD)              != NULL && strcmp(varGet(GVAR_CLUPWD),"(null)")              != 0) fprintf(fh,"modify cluster/cid=%s/pwd=\"%s\";\n",         varGet(GVAR_CLUCID),varGet(GVAR_CLUPWD));
      if (varGet(GVAR_CLUDB)               != NULL && strcmp(varGet(GVAR_CLUDB),"(null)")               != 0) fprintf(fh,"modify cluster/cid=%s/db=\"%s\";\n",          varGet(GVAR_CLUCID),varGet(GVAR_CLUDB));  
      if (varGet(GVAR_CLUPORT)             != NULL && strcmp(varGet(GVAR_CLUPORT),"(null)")             != 0) fprintf(fh,"modify cluster/cid=%s/port=\"%s\";\n",        varGet(GVAR_CLUCID),varGet(GVAR_CLUPORT));  
      if (varGet(GVAR_WALDIR)              != NULL && strcmp(varGet(GVAR_WALDIR),"(null)")              != 0) fprintf(fh,"modify cluster/cid=%s/waldir=\"%s\";\n",      varGet(GVAR_CLUCID),varGet(GVAR_WALDIR));  
      if (varGet(GVAR_BACKUPDIR)           != NULL && strcmp(varGet(GVAR_BACKUPDIR),"(null)")           != 0) fprintf(fh,"modify cluster/cid=%s/backupdir=\"%s\";\n",   varGet(GVAR_CLUCID),varGet(GVAR_BACKUPDIR));  
      if (varGet(GVAR_COMPLEVEL)           != NULL && strcmp(varGet(GVAR_COMPLEVEL),"(null)")           != 0) fprintf(fh,"modify cluster/cid=%s/compression=%s;\n",     varGet(GVAR_CLUCID),varGet(GVAR_COMPLEVEL)); 
      if (varGetInt(GVAR_REINDEXCONCURRENTLY) == true) { strcpy(r,"yes"); }
                                                  else { strcpy(r,"no"); };
      fprintf(fh,"modify cluster/cid=%s/concurrently=\"%s\";\n",varGet(GVAR_CLUCID),r);
      
      sprintf(query,"select rep_usr,rep_pwd,clu_addr,rep_opts from %s.clusters where CID=%s",
                       varGet(GVAR_DEPUSER),
                       varGet(GVAR_CLUCID));
      int rows=DEPOquery(query,0);
      TRACE("[rc=%d]\n",rows);
      if (DEPOgetString(0,0) != NULL && strcmp(DEPOgetString(0,0),"(null)") != 0) fprintf(fh,"modify cluster/cid=%s/repusr=\"%s\";\n", varGet(GVAR_CLUCID), DEPOgetString(0,0));
      if (DEPOgetString(0,1) != NULL && strcmp(DEPOgetString(0,0),"(null)") != 0) fprintf(fh,"modify cluster/cid=%s/reppwd=\"%s\";\n", varGet(GVAR_CLUCID), unscramble(DEPOgetString(0,1)));
      if (DEPOgetString(0,2) != NULL && strcmp(DEPOgetString(0,2),"(null)") != 0) fprintf(fh,"modify cluster/cid=%s/ip=\"%s\";\n",     varGet(GVAR_CLUCID), DEPOgetString(0,2));
      if (DEPOgetString(0,3) != NULL && strcmp(DEPOgetString(0,3),"(null)") != 0) fprintf(fh,"modify cluster/cid=%s/repopts=\"%s\";\n",varGet(GVAR_CLUCID), DEPOgetString(0,3));
      DEPOqueryEnd();
      fprintf(fh,"\n");
   };
   if (in_file == true) { fclose(fh); };
   memEndModule();
}

