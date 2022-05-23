

#define ERR_SECNOTFND  1
#define ERR_SECLOADED  0
#define LOCK_CFGFILE "/tmp/.recmlckupdate"

/********************************************************************************/
/* Construct configuration file '<home>/.recm/recm_<cluster_name>.conf'         */
/* You can define the environment variable 'ENVIRONMENT_RECM_CONFIG'            */
/* In that case, you have to define the full file name                          */
/* Example :                                                                    */
/*           export RECM_CONFIG='/var/lib/recm/recm.conf'                       */
/********************************************************************************/
char *buildConfigurationFile()
{
   char *configuration_file=malloc(1024);
   char* pConfig=getenv (ENVIRONMENT_RECM_CONFIG);                                                                                 // Change configuration file location using 'RECM_CONFIG'
   if (pConfig != NULL) { strcpy(configuration_file,pConfig); }                                                                    
                   else { char* pHome=getenv (ENVIRONMENT_HOME);                                                                   // Default is '<HOME>/.recm/recm.conf'
                          strcpy(configuration_file,pHome);                                                                        
                          strcat(configuration_file,"/");                                                                          
                          strcat(configuration_file,".recm");                                                                      
                          mkdir (configuration_file,S_IRWXU|S_IRGRP|S_IROTH); 
                          strcat(configuration_file,"/recm.conf");                                                                 // Use default (<home>/.recm/recm.conf)
                       };
   varAdd("@config_file",configuration_file);
   free(configuration_file);
   return(varGet("@config_file"));
}


void writeSection(FILE *Hw,const char * prefix,const char * sectionName,int with_header,int kind)
{
   TRACE("Updating section (%d) '%s'\n",kind,sectionName);
   switch (kind)
   {
   case SECTION_DEFAULT :
               if (with_header != 0) fprintf(Hw,"\n[default]\n");
               fprintf(Hw,"%s=%s\n",SYSVAR_PROMPT,               varGet(SYSVAR_PROMPT));
               fprintf(Hw,"%s=%s\n",SYSVAR_DATE_FORMAT,          varGet(SYSVAR_DATE_FORMAT));
               fprintf(Hw,"%s=%s\n",SYSVAR_HISTDEEP,             varGet(SYSVAR_HISTDEEP));
               fprintf(Hw,"%s=%s\n",SYSVAR_PGBIN,                varGet(SYSVAR_PGBIN));
               fprintf(Hw,"%s=%s\n",SYSVAR_TIMEZONE,             varGet(SYSVAR_TIMEZONE));
                break;
   case SECTION_CLUSTER :
               if (with_header != 0) fprintf(Hw,"\n[%s_%s]\n",prefix,sectionName);
               fprintf(Hw,"%s=%s\n",GVAR_CLUDESCR  ,varGet(GVAR_CLUDESCR));
               fprintf(Hw,"%s=%s\n",GVAR_CLUHOST   ,varGet(GVAR_CLUHOST));
               fprintf(Hw,"%s=%s\n",GVAR_CLUPORT   ,varGet(GVAR_CLUPORT));
               fprintf(Hw,"%s=%s\n",GVAR_CLUUSER   ,varGet(GVAR_CLUUSER));
               fprintf(Hw,"%s=%s\n",GVAR_CLUPWD    ,scramble(varGet(GVAR_CLUPWD)));
               fprintf(Hw,"%s=%s\n",GVAR_CLUDB     ,varGet(GVAR_CLUDB));
               fprintf(Hw,"%s=%s\n",GVAR_REGDEPO   ,varGet(GVAR_REGDEPO));
               fprintf(Hw,"%s=%s\n",GVAR_CLUCID    ,varGet(GVAR_CLUCID));
               fprintf(Hw,"%s=%s\n",GVAR_CLUNAME   ,varGet(GVAR_CLUNAME));
               fprintf(Hw,"%s=%s\n",GVAR_CLUVERSION,varGet(GVAR_CLUVERSION));
               fprintf(Hw,"%s=%s\n",GVAR_CLUPGDATA ,varGet(GVAR_CLUPGDATA));
               fprintf(Hw,"%s=%s\n",GVAR_PGCTL     ,varGet(GVAR_PGCTL));
               fprintf(Hw,"%s=%s\n",GVAR_SOCKPATH  ,varGet(GVAR_SOCKPATH));

               break;
   case SECTION_DEPOSIT :
               if (with_header != 0) fprintf(Hw,"\n[%s_%s]\n",prefix,sectionName);
               fprintf(Hw,"%s=%s\n",GVAR_DEPDESCR,varGet(GVAR_DEPDESCR));
               fprintf(Hw,"%s=%s\n",GVAR_DEPHOST ,varGet(GVAR_DEPHOST ));
               fprintf(Hw,"%s=%s\n",GVAR_DEPPORT ,varGet(GVAR_DEPPORT ));
               fprintf(Hw,"%s=%s\n",GVAR_DEPDB   ,varGet(GVAR_DEPDB   ));
               fprintf(Hw,"%s=%s\n",GVAR_DEPUSER ,varGet(GVAR_DEPUSER ));
               fprintf(Hw,"%s=%s\n",GVAR_DEPPWD  ,scramble(varGet(GVAR_DEPPWD  )));
               fprintf(Hw,"%s=%s\n",GVAR_DEPNAME ,varGet(GVAR_DEPNAME ));
               break;
   }
}

void lock_configFile()
{
   TRACE("Wait for lock...\n");
   int wait_10=10;
   while (file_exists(LOCK_CFGFILE) == true && wait_10 > 0)
   {
      TRACE("Is still locked (%d)...\n",wait_10);
      sleep(1);
      wait_10--;
   }
   FILE *Hw=fopen(LOCK_CFGFILE, "w");
   fprintf(Hw,"--Lock--\n");
   fclose(Hw);
}

void unlock_configFile()
{
   if (file_exists(LOCK_CFGFILE) == true) 
   {
      TRACE("Unlocked\n");
      unlink(LOCK_CFGFILE);
   }
   else
   {
      TRACE("Was not locked.\n");
   }
}

void SaveConfigFile(const char *prefix,const char *pSection,int kind)
{
   if (globalArgs.configChanged == 0) return;
   char *configFile=buildConfigurationFile();
   char *sectionName=malloc(128);
   if (prefix == NULL) { strcpy(sectionName,pSection); }
                  else { sprintf(sectionName,"%s_%s",prefix,pSection); };

   char *configBackup=malloc(1024);
   char *currentSection=malloc(128);
   
   lock_configFile();
   
   sprintf(configBackup,"%s.old",configFile);
   if (file_exists(configBackup)==true) unlink(configBackup);
   if (file_exists(configFile)==true) rename(configFile,configBackup);
   
   int updated=0;
   FILE *Hr=fopen(configBackup, "r");
//   if (Hr == NULL) { return;};
   FILE *Hw=fopen(configFile, "w");
   if (Hw == NULL) 
   { 
      free(configBackup);
      free(currentSection);
      free(sectionName);
      unlock_configFile();
      return;
   };
   if (Hr != NULL)
   {
      char *line=malloc(2048);
      while (fgets(line, 2040, Hr) != NULL)
      {
         char *posSECTION=strstr(line,"[");  
         if (posSECTION == line)
         {
            posSECTION++;
            char *endSection=strchr(posSECTION,']');
            if (endSection != NULL) endSection[0]=0x00;
            strcpy(currentSection,posSECTION);
            fprintf(Hw,"[%s]\n",currentSection);
         }
         else
         {
            TRACE("Checking section '%s' <==> '%s'\n",currentSection,sectionName);
            if (strcmp(currentSection,sectionName) == 0)
            { 
               if (updated == 0) { writeSection(Hw,prefix,pSection,0,kind); updated++; }
            }
            else 
            { 
               fprintf(Hw,"%s",line);
            }
         }
      }
      free(line);
      fclose(Hr);
   };
   if (updated == 0) { writeSection(Hw,prefix,pSection,1,kind); }
   fclose(Hw);
   globalArgs.configChanged=0;
   free(configBackup);
   free(currentSection);
   free(sectionName);
   unlock_configFile();
   TRACE("END\n");
}


/********************************************************************************/
int LoadConfigFile(const char *prefix,const char *pSection)
{
   TRACE("ARGS: prefix='%s' pSection='%s'\n",prefix,pSection);
   int result=ERR_SECNOTFND;
   char *configuration_file=buildConfigurationFile();
   FILE* fh=fopen(configuration_file, "r");
   if (fh == NULL) { return(result); };

   lock_configFile();

   int line_size = 1024;
   char *currentSection=malloc(128);
   char* sectionName=malloc(128);
   if (prefix == NULL) { strcpy(sectionName,pSection); }
                  else { sprintf(sectionName,"%s_%s",prefix,pSection); };
   char* line=malloc(line_size);
   char *keyword=malloc(line_size);
   char *value=malloc(line_size);
   
   int ln=0;
   while (fgets(line, line_size, fh) != NULL)
   {
       ln++;
       char *rc=strchr(line,'\n');
       if (rc != NULL) rc[0]=0x00;
       if (strlen(line) == 0) continue;
       char *posREM=strstr(line,"#");                                           // '#' in first position is for comments
       if (posREM == line) continue;
       char *posSECTION=strstr(line,"[");  
       if (posSECTION == line)
       {
          posSECTION++;
          char *endSection=strchr(posSECTION,']');
          if (endSection != NULL) endSection[0]=0x00;
          strcpy(currentSection,posSECTION);
          TRACE("Start section '%s'\n",currentSection);
          continue;
       }
       TRACE("Compare '%s' with '%s'\n",currentSection,sectionName);
       if (strcmp(currentSection,sectionName) != 0) continue;
       TRACE("In section '%s'\n",currentSection);
       char *separator=strchr(line,'=');
       if (separator == NULL) continue;
       separator[0]=0x00;
       separator++;
       TRACE("In separator '%s'\n",separator);
       strcpy(keyword,line);
       strcpy(value,separator);
       TRACE("[%s] Loading %s='%s'\n",currentSection,keyword,value);
       int scrambled=0;
       if (strcmp(keyword,GVAR_CLUPWD) == 0) scrambled++;
       if (strcmp(keyword,GVAR_DEPPWD) == 0) scrambled++;
       if (scrambled > 0) { varAdd(keyword,unscramble(value)); }
                     else { varAdd(keyword,value); };
       result=ERR_SECLOADED;
   };
   fclose(fh);
   free(value);
   free(keyword);
   free(line);
   free(sectionName);
   free(currentSection);

   globalArgs.configChanged=0;
   unlock_configFile();
   return(result);
}


void DisplaySavedClusters(const char *prefix)
{
   TRACE("ARGS: prefix='%s'\n",prefix);
   //int result=ERR_SECNOTFND;
   char *configuration_file=buildConfigurationFile();
   
   lock_configFile();
   
   FILE* fh=fopen(configuration_file, "r");
   if (fh == NULL) { return; };

    
   int count=0;
   int line_size = 1024;
   char* line=malloc(line_size);
   int ln=0;
   while (fgets(line, line_size, fh) != NULL)
   {
       ln++;
       char *rc=strchr(line,'\n');
       if (rc != NULL) rc[0]=0x00;
       if (strlen(line) == 0) continue;
       char *posREM=strstr(line,"#");                                           // '#' in first position is for comments
       if (posREM == line) continue;
       char *posSECTION=strstr(line,"[");
       if (posSECTION == NULL) continue;
       char *endSection=strchr(posSECTION,']');
       if (endSection != NULL) endSection[0]=0x00;
       posSECTION++;
       if (strstr(posSECTION,prefix) != NULL)
       {
          posSECTION+=strlen(prefix)+1; 
          if (count == 0) INFO("Available clusters : ");
          printf("%s ",posSECTION);
          count++;
       }
   };
   if (count ==0)
   {
      INFO("You never connect any cluster. No list to provide.\n");
   }
   else
   {
      printf("\n");
   };
   fclose(fh);
   free(line);
   unlock_configFile();
   return;
}
