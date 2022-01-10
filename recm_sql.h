/**********************************************************************************/
/* SQL command                                                                    */
/* Usage:                                                                         */
/*    set deposit                                                                 */                 
/*      Available Qualifier(s):                                                   */
/*         /deposit=<QUERY>       Execute a SQL command on DEPOSIT                */
/*         /cluster=<QUERY>       Execute a SQL command on CLUSTER                */
/**********************************************************************************/
void COMMAND_SQL(int idcmd,char *command_line)
{
   memBeginModule();
   int colsize[512];
   char *query=memAlloc(2048);
   char *line_tit=memAlloc(2048);
   char *line_sep=memAlloc(2048);
   char *line_dta=memAlloc(2048);
   char *label=memAlloc(512);
   char *data=memAlloc(1024);
              
   if (qualifierIsUNSET("qal_cluster") == false)
   {
      sprintf(query,"%s",varGet("qal_cluster"));
      line_tit[0]=0x00;
      line_sep[0]=0x00;
      int rows=CLUquery(query,0);
      int nFields = PQnfields(CLUgetRes()); // Get number of fields
      for (int c=0;c < nFields;c++)
      {
         colsize[c]=strlen(PQfname(CLUgetRes(),c));
         for (int n=0; n < rows;n++)
         {
            if ( PQgetlength(CLUgetRes(), n, c) > colsize[c]) colsize[c]=PQgetlength(CLUgetRes(), n, c);
         }
         if (colsize[c] > 50) colsize[c]=50;
      };
      for (int n=0; n < rows;n++)
      {
         line_dta[0]=0x00;
         for (int c=0;c < nFields;c++)
         {
           if (n == 0)
           {
              sprintf(label,"%s",PQfname(CLUgetRes(),c));
              strcat(line_sep,"+");      
              strcat(line_tit,"|");
              for (int x=0;x < colsize[c];x++) { strcat(line_sep,"-"); };
              while (strlen(label) < colsize[c]) { strcat(label," ");};
              strcat(line_tit,label);
              if (c >= (nFields-1)) { strcat(line_sep,"+"); strcat(line_tit,"|"); }
           };
           if (PQgetisnull(CLUgetRes(),n,c) == 1){ sprintf(data,"|<null>");}
                                            else { sprintf(data,"|%s",PQgetvalue(CLUgetRes(),n,c));};
           while (strlen(data) <= colsize[c]) { strcat(data," ");};
           strcat(line_dta,data);
           if (c >= (nFields-1)) { strcat(line_dta,"|"); };
         };
         if (n == 0) printf("%s\n%s\n%s\n",line_sep,line_tit,line_sep);
         printf("%s\n",line_dta);
      };
      switch(rows)
      {
         case -1 : printf("%s\n",CLUgetErrorMessage());break;
         case  0 : printf("no row.\n");break;
         case  1 : printf("%s\n1 row.\n",line_sep);break;
         default : printf("%s\n%d Rows.\n",line_sep,rows);
      };
      CLUqueryEnd();
   };
   if (qualifierIsUNSET("qal_deposit") == false)
   {
      sprintf(query,"%s",varGet("qal_deposit"));
      line_tit[0]=0x00;
      line_sep[0]=0x00;
      int rows=DEPOquery(query,0);
      int nFields = PQnfields(DEPOgetRes()); // Get number of fields
      for (int c=0;c < nFields;c++)
      {
         colsize[c]=strlen(PQfname(DEPOgetRes(),c));
         for (int n=0; n < rows;n++)
         {
            if ( PQgetlength(DEPOgetRes(), n, c) > colsize[c]) colsize[c]=PQgetlength(DEPOgetRes(), n, c);
         }
         if (colsize[c] > 50) colsize[c]=50;
      };
      for (int n=0; n < rows;n++)
      {
         line_dta[0]=0x00;
         for (int c=0;c < nFields;c++)
         {
           if (n == 0)
           {
              sprintf(label,"%s",PQfname(DEPOgetRes(),c));
              strcat(line_sep,"+");      
              strcat(line_tit,"|");
              for (int x=0;x < colsize[c];x++) { strcat(line_sep,"-"); };
              while (strlen(label) < colsize[c]) { strcat(label," ");};
              strcat(line_tit,label);
              if (c >= (nFields-1)) { strcat(line_sep,"+"); strcat(line_tit,"|"); }
           };
           if (PQgetisnull(DEPOgetRes(),n,c) == 1){ sprintf(data,"|<null>");}
                                            else { sprintf(data,"|%s",PQgetvalue(DEPOgetRes(),n,c));};
           while (strlen(data) <= colsize[c]) { strcat(data," ");};
           strcat(line_dta,data);
           if (c >= (nFields-1)) { strcat(line_dta,"|"); };
         };
         if (n == 0) printf("%s\n%s\n%s\n",line_sep,line_tit,line_sep);
         printf("%s\n",line_dta);
      };
      switch(rows)
      {
         case -1 : printf("%s\n",DEPOgetErrorMessage());break;
         case  0 : printf("no row.\n");break;
         case  1 : printf("%s\n1 row.\n",line_sep);break;
         default : printf("%s\n%d Rows.\n",line_sep,rows);
      };
      DEPOqueryEnd();
    };

    memEndModule();
}
