# RECM
<H2>Release 1.2 is now available.</H2>
<br><b>improvements</b><br>
<blockquote>
* Backup FULL and WAL operation in parallel is now possible, with the new option '/parallel=n'
  By default, the value of the parallel is 1.
  The backups are performed via threads. This improvement increase the speed of the backup drastically.<br>
* New option '/rp' for 'backup full', to create a restore point at the end of the backup to facilitate a restore without too many WAL files.<br>
* REMAP tablespaces is now possible.<br>
* New command 'set mapping' allow you , before to launch the restore/duplicate,  to define the new location for each tablespaces.
  Command: set mapping/tablespace=<mytbs>  /directory=”/my_new_location/”<br>
* Improvement of the 'show backup' command to allow you to see the tablespaces that are used  by the database at backup time.<br>
  Command: show backup/uid=xxxxxxx /directories
* New command 'list mapping', to list defined mappings for tablespaces.
* Command: list mapping<br>
* new command 'status'. Have a quick overview of the status of the cluster and the deposit.<br>
* New command 'upgrade deposit', to upgrade the DEPOSIT database.<br>
  This allow you to upgrade your deposit from version 1.0 to version 1.2
* 'list wal' display WAL files not already backuped.
* Backup WAL take care of slot usage.<br>
* New command 'delete mapping', to remove a defined mapping for tablespaces.<br>
Command: delete mapping
* 'show backup' command allow short 'uid' search. For quick display of  specific backup, left part of the UID can be specified while the UID found is unique. 
</blockquote>

<br><b>Fixes</b><br>
<blockquote>
* restore meta with option '/cid' is not working<br>
* Comma separated list of UID for modify backup command<br>
* Help command was not accepting some keywords...<br>
* Lost of configuration file content. Implemented transaction mechanism for this. <br>
* Wrong number of pieces for WAL backups.<br>
* 'verify backup' command does not set 'incomplete' backup when piece count is not equal to the pieces found.<br>
* Add more checks to validate FULL backups.<br>
* Restore wal '/first' and '/last' failed<br>
* Deletion of WAL files skipped when slot is present.<br>
* Backup terminate date not set for CFG and META backups.<br>
* 'list wal' command display now WAL files not yet backuped.<br>
* Last Update date and creation date inverted in 'status' command.<br>
* Layout changed on 'show backup' command.<br>
* Missing '/repusr','/reppwd' and '/repopts' on 'show cluster' command.<br>
</blockquote>

