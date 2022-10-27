/**********************************************************************************/
/* LIST MAPPING command                                                           */
/* Usage:                                                                         */
/*    list mapping                                                                */                 
/**********************************************************************************/
/*
@command list mapping
@definition
Display all mapping defined by command '{&set mapping&}'.
Mapping allow you to restore tablespaces onto a different file system.
<b>Remark:</b>
the mapping are defined at session level only.

@example
@inrecm  {&set mapping&} /tablespace=tbs1 /directory="/tmp/tbs1" /force 
@out recm-inf: Try to create directory '/tmp/tbs1'
@out recm-inf: Successfully created directory '/tmp/tbs1'
@inrecm list mapping 
@out List mappings
@out Type            Name                 Mapping
@out --------------- -------------------- ------------------------------
@out TABLESPACE      tbs1                 /tmp/tbs1
@out 
@inrecm
@end
 
*/
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
