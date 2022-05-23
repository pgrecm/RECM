/**********************************************************************************/
/* DELETE MAPPING command                                                             */
/* Usage:                                                                         */
/*    set mapping                                                                  */                 
/*      Available Qualifier(s):                                                   */
/*         /tablespace=<tbsname>      Tablespace name to remap                    */
/*         /directory=<PATH>          New path for tablespace at restore time     */
/*         /delete                    Remove target definition                    */
/**********************************************************************************/
void COMMAND_DELMAPPING(int idcmd,char *command_line)
{
   if (optionIsSET("opt_verbose") == true) globalArgs.verbosity=true;                                                              // Set Verbosity

   if (target_list == NULL) target_list=arrayCreate(ARRAY_UP_ORDER);

   // set target /all
   if (optionIsSET("opt_all") == true)
   {
      ArrayItem *itm=arrayFirst(target_list);
      while (itm != NULL)
      {
         TargetPiece *tp=(TargetPiece *)itm->data;
         TRACE("Deleting MAPPING '%s'\n",tp->name);
         free(tp->name);
         free(tp->value);
         arrayDelete(target_list,itm);
         itm=arrayFirst(target_list);
      };        
   };
   if (qualifierIsUNSET("qal_tablespace") == false)                                                                                // set target /tablespace=name /directory="<new_path>"
   {
       ArrayItem *fnd=NULL;
      fnd=arrayFind(target_list,varGet("qal_tablespace"));                                                                         // update existing entry
      if (fnd != NULL && strcmp(fnd->key,varGet("qal_tablespace")) == 0)
      {
         VERBOSE("Remove mapping for tablespace '%s'\n",varGet("qal_tablespace"));
         arrayDelete(target_list,fnd);
      }
      else
      {
         ERROR(ERR_MAPNOTFND,"Mapping '%s' not found.\n",varGet("qal_tablespace"));
      }
   }
   else
   {
      ERROR(ERR_MISSQUAL,"Missing qualifier '/tablespace'.\n");
   }
   return;
}
