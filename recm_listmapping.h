/**********************************************************************************/
/* SET MAPPING command                                                             */
/* Usage:                                                                         */
/*    list mqpping                                                                 */                 
/**********************************************************************************/
void COMMAND_LISTMAPPING(int idcmd,char *command_line)
{
   if (optionIsSET("opt_verbose") == true) globalArgs.verbosity=true;                                                              // Set Verbosity
   
   printf("List mappings\n"); 
   
   int cnt=0;
   if (target_list == NULL) target_list=arrayCreate(ARRAY_UP_ORDER);
   ArrayItem *itm=arrayFirst(target_list);
   while (itm != NULL)
   {
      TargetPiece *tp=(TargetPiece *)itm->data;
      if (cnt == 0) { printf ("%-15s %-20s %s\n","Type","Name","Mapping");
                      printf ("%-15s %-20s %s\n","---------------","--------------------","------------------------------");
                    };
      char typ[20];
      char inf[80];
      memset(inf,0x00,80);
      switch(tp->tgtype)
      {
      case TARGET_TABLESPACE : strcpy(typ,"TABLESPACE");
                               strcpy(inf,"");
                               if (directory_exist(tp->value) == false)
                               {
                                  strcat(inf," [ Not Exist ]"); 
                               }
                               else
                               {
                                  if (directoryIsWriteable(tp->value) == false) {strcat(inf," [ Not Writeable ]");  };
                                  if (directoryIsEmpty(tp->value) == false)     {strcat(inf," [ Not Empty ]");  };
                               };
                               break;
      default: strcpy(typ,"Unknown");
      }; 
      printf ("%-15s %-20s %s%s\n",typ,tp->name,tp->value,inf);
      itm=arrayNext(target_list);
      cnt++;
   };
   if (cnt == 0) { printf("No mapping defined.\n"); };
   return;
}
