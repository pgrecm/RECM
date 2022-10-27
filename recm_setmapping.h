/**********************************************************************************/
/* SET MAPPING command                                                             */
/* Usage:                                                                         */
/*    set mapping                                                                  */                 
/*      Available Qualifier(s):                                                   */
/*         /tablespace=<tbsname>      Tablespace name to remap                    */
/*         /directory=<PATH>          New path for tablespace at restore time     */
/*         /delete                    Remove target definition                    */
/**********************************************************************************/
/*
@command set mapping
@definition
Define a mapping.
A mapping allow you to define a tablespace mapping, to move tablespace location to a different place at restore time.
With the {&show backup&} command, with '/directories' option, you can have the list of existing tablespaces.
(See also {&list mapping&} for more details.)
<b>Remark:</b>
The mapping are defined at session level only.

@example
@inrecm  set mapping /tablespace=tbs1 /directory="/tmp/tbs1" /force 
@out recm-inf: Try to create directory '/tmp/tbs1'
@out recm-inf: Successfully created directory '/tmp/tbs1'
@inrecm {&list mapping&} 
@out List mappings
@out Type            Name                 Mapping
@out --------------- -------------------- ------------------------------
@out TABLESPACE      tbs1                 /tmp/tbs1
@out 
@inrecm
@end
 
*/
void COMMAND_SETMAPPING(int idcmd,char *command_line)
{
   if (optionIsSET("opt_verbose") == true) globalArgs.verbosity=true;                                                              // Set Verbosity

   if (target_list == NULL) target_list=arrayCreate(ARRAY_UP_ORDER);

   // set target /reset
   if (optionIsSET("opt_reset") == true)
   {
      ArrayItem *itm=arrayFirst(target_list);
      while (itm != NULL)
      {
         TargetPiece *tp=(TargetPiece *)itm->data;
         TRACE("Freeing TARGET '%s'\n",tp->name);
         free(tp->name);
         free(tp->value);
         arrayDelete(target_list,itm);
         itm=arrayFirst(target_list);
      };        
   };
   if (qualifierIsUNSET("qal_tablespace") == false)                                                                                // set target /tablespace=name /directory="<new_path>"
   {
      if (qualifierIsUNSET("qal_directory") == true && optionIsSET("opt_delete") == false)                                         // '/directory' is mandatory with tablespace remap
      { 
         ERROR(ERR_MISSDIR,"Incomplete target tablespace. Qualifier '/directory' is mandatory\n");
         return;
      }
      if (directory_exist(varGet("qal_directory")) == false && optionIsSET("opt_force") == false && optionIsSET("opt_delete") == false)                                                                       // Directory must exist
      {
         ERROR(ERR_DIRNOTFND,"Directory '%s' does not exist (Use '/force' to create it).\n",varGet("qal_directory"));
         return;
      }
      if (optionIsSET("opt_force") == true && optionIsSET("opt_delete") == false)                                                                                        // try to create directory
      {
         INFO("Try to create directory '%s'\n",varGet("qal_directory"));
         mkdirs(varGet("qal_directory"),maskTranslate(varGet(GVAR_CLUMASK_RESTORE)));
         if (directory_exist(varGet("qal_directory")) == false)                                                                    // If still not...abort
         {
            ERROR(ERR_DIRNOTFND,"Could not create directory '%s'.\n",varGet("qal_directory"));
            return;
         }
         INFO("Successfully created directory '%s'\n",varGet("qal_directory"));
      }
      
      if (optionIsSET("opt_delete") == false && directoryIsWriteable(varGet("qal_directory")) == false)                                                                  // Directory must be 'writeable' too
      {
         WARN(WRN_BADDIRPERM,"Do not have write access into directory '%s'.\n",varGet("qal_directory"));
      }

      if (optionIsSET("opt_delete") == false && directoryIsEmpty(varGet("qal_directory")) == false)                                                                      // Directory must be empty
      {
         ERROR(ERR_DIRNOTEMPTY,"Directory '%s' must be empty.\n",varGet("qal_directory"));
         return;
      }
      ArrayItem *fnd=NULL;
      fnd=arrayFind(target_list,varGet("qal_tablespace"));                                                                         // update existing entry
      if (fnd != NULL && strcmp(fnd->key,varGet("qal_tablespace")) == 0)
      {
         if (optionIsSET("opt_delete") == true)
         {
            VERBOSE("Remove tablespace target name '%s'\n",varGet("qal_tablespace"));
            arrayDelete(target_list,fnd);
         }
         else
         {
            TargetPiece *tp=(TargetPiece *)fnd->data;
            free(tp->value);
            tp->value=strdup(varGet("qal_directory"));
            VERBOSE("Set tablespace target name '%s' to DIRECTORY '%s'\n",tp->name,tp->value);
         }
      }
      else                                                                                                                         // or Add new entry
      {
         TargetPiece *tp=malloc(sizeof(TargetPiece));
         tp->tgtype=TARGET_TABLESPACE;
         tp->name=strdup(varGet("qal_tablespace"));
         tp->value=strdup(varGet("qal_directory"));
         tp->t1=NULL;
         tp->t2=NULL;
         tp->t3=NULL;
         arrayAddKey(target_list,tp->name,tp);
         VERBOSE("Set New tablespace target name '%s' to DIRECTORY '%s'\n",tp->name,tp->value);
      };
   };
   return;
}
