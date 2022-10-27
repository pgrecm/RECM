/* RECM                                                                           */
/* To build, you need the following components installed :                        */
/*     - ncurses                                                                  */
/*     - libzip                                                                   */
/*     - libpq                                                                    */
/**********************************************************************************/

#include <pwd.h>
#include <unistd.h>
#include <sys/errno.h>
#include <time.h>
//#include <wchar.h>
#include <termios.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <utime.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <unistd.h>
#include <limits.h>
#include <dirent.h>
#include <stdbool.h>
#include <libpq-fe.h>
#include <zip.h>
#include <sys/statvfs.h>
#include <sys/param.h>
#include <sys/mount.h>
#include <regex.h>
#include <regex.h>
#include <ncurses.h>
#include <ifaddrs.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <getopt.h>
#include <getopt.h>
#include <pthread.h>


#define VERSION_11 "1.1.0"                                                      /* Previous DEPOSIT versions. Required for Deposit upgrade */
#define VERSION_12 "1.2.0"                                                      /* Previous DEPOSIT versions. Required for Deposit upgrade */

#define CURRENT_VERSION "1.2.0"                                                 /* Current RECM version  last change : 25/07/2022 */

#if defined(BUILD_ID)
#define CURRENT_BUILD  BUILD_ID
#else
#define CURRENT_BUILD   1                                                       /* Build date       2 = 25/07/2022 */
#endif


//#define use_libzip_18
             
/* Thread specific variables */
#define THREAD_RUNNING         0                                                /* Thread table item status code : running */
#define THREAD_FREE            1                                                /* Thread table item status code : available */
#define THREAD_END_SUCCESSFULL 2                                                /* Thread table item status code : thread end successfully */
#define THREAD_END_FAILED      4                                                /* Thread table item status code : thread end with failure */
#define WORKER_DISPLAY_PROGRESSION 3                                            /* time (in sec) between the display of worker process progression */
#define MAX_THREADS            32                                               /* Max size of the threads table */

#define TREE_ITEM_FILE         1                                                /* Object is a FILE */
#define TREE_ITEM_DIR          2                                                /* Object is a SUB-DIRECTORY */
#define TREE_ITEM_LINK         4                                                /* Object is a LINK */
                                                                                
#define TREE_ITEM_INDEX        8                                                /* Object is PostgreSQL index (NNNNNNN)     */
#define TREE_ITEM_INDEX_VM    16                                                /* Index visibility map       (NNNNNNN_vm)  */
#define TREE_ITEM_INDEX_FSM   32                                                /* Index Free Space Map       (NNNNNNN_fsm) */
#define TREE_ITEM_LOGFILE     64                                                /* PostgreSQL cluster log file (will never be saved */
                                                                                
#define HARD_LIMIT_MINFILES 10                                                  /* hard limit : minimum accepted value */
#define HARD_LIMIT_MINSIZE  "10M"                                               /* hard limit : minimum accepted value */
#define HARD_LIMIT_MINWALFILES 10                                               /* hard limit : minimum accepted value */
#define HARD_LIMIT_MINWALSIZE  "4M"                                             /* hard limit : minimum accepted value */

typedef struct TreeItem TreeItem;
struct TreeItem
{
   TreeItem *next;
   off_t     size;                                                              // Object size
   uid_t     st_uid;                                                            // User ID of owner
   gid_t     st_gid;                                                            // Group ID of owner
   mode_t    st_mode;                                                           // File type and mode
   time_t    mtime;                                                             // File mtime (used for WAL backups)
   char     *name;                                                              // Name of the objec
   unsigned  flag;                                                              // Flags
   TreeItem *path;                                                              // Directory where the object is stored
   int       sequence;                                                          // Group number (RECM block number)
   int       used;                                                              // Indicate the object has already been added to the RECM file
};

typedef struct TreeEntry TreeEntry;
struct TreeEntry
{
   TreeItem *first;                                                             // First item in the tree
   TreeItem *last;                                                              // Last item in the tree
   TreeItem *currentFolder;                                                     // Last item in the tree
   long entries;                                                                // Number of entries (files)
   long long totalsize;                                                         // Total size of the tree
};


typedef struct RECMDataFile RECMDataFile;
struct RECMDataFile
{
   int pieceNumber;                                                             // Piece number of the backup
   int parallel_count;                                                          // number of parallel processes
   zip_uint32_t compLevel;                                                      // Compresssion level
   char *tarFileName;                                                           // ZIP file name
   zip_t *zipHandle;                                                            // ZIP hande
   zip_error_t zipError;                                                        // ZIP Error structure
   time_t start_time;                                                           // Creation start time
};

typedef struct ThreadData ThreadData;
struct ThreadData
{
   int          idThread;
   int          state;                                                          // Return code of the thread
   int          rc;                                                             // Return code of the thread
   int          parallel_count;                                                 // number of parallel processes
   long         pieceNumber;                                                    // ID of the process 
   pthread_t    threadID;                                                       // Thread process data
   char         *bck_id;                                                        // Backup UID
   char         *bck_typ;                                                       // Backup TYPE : 'FULL','WAL'
   TreeEntry    *pgdataTree;                                                    // PGDATA directory structure
   char         *rootdirectory;                                                 // path of the ROOT directory
   RECMDataFile *zH;                                                            // RECM handle
};



static ThreadData threads[MAX_THREADS];                                         // Thread table
pthread_mutex_t mutex1 = PTHREAD_MUTEX_INITIALIZER;                             // Thread MUTEX flag

unsigned long thread_all_file_processed;                                        // Number of files processed by all the thread
unsigned long thread_all_size_processed;                                        // Total file size processed by all the thread 
unsigned long verification_limitFile;                                           // Verification counters
unsigned long verification_limitSize;                                           // Verification counters



/* Environment variables */
#define ENVIRONMENT_RECM_META_DIR         "RECM_METADIR"                        /* (CHANGE : 0xA0004 ) METADATA custom generation folder              */
#define ENVIRONMENT_RECM_CONFIG           "RECM_CONFIG"                         /* Change default recm.conf file name and location                    */
#define ENVIRONMENT_HOME                  "HOME"                                /* Unix home folder                                                   */
#define ENVIRONMENT_RECM_PGBIN            "RECM_PGBIN"                          /* Force RECM tool to use a specific version                          */
#define ENVIRONMENT_RECM_WALCOMP_EXT      "RECM_WALC_EXT"                       /* If  WAL files are compressed, they have by default '.gz' extension */
#define ENVIRONMENT_RECM_RESTORE_COMMAND  "RECM_RESTORE_COMMAND"                /* If  WAL files are compressed, they have by default '.gz' extension */
#define ENVIRONMENT_RECM_COMPRESS_METHOD  "RECM_COMPRESS_METHOD"                /* Change default compression method (Change : 0xA000D)               */

/* Backup options */
#define BACKUP_OPTION_NOINDEX       1
#define BACKUP_OPTION_EXCLUSIVE     2
#define BACKUP_OPTION_COMPRESSED    4
#define BACKUP_OPTION_KEEPFOREVER   8
#define BACKUP_OPTION_MASK        511

#define DEFAULT_MAXFILES 50
#define DEFAULT_MAXSIZE  10*1024*1024

long flagSetOption(long flag,long bit)
{
   if ((flag & bit) != bit)  return (flag^bit);
   return(flag);
}

long flagUnSetOption(long flag,long bit)
{
   if ((flag & bit) == bit) return(flag-bit);
   return(flag);
}


long backupSetOption(long flag,long bit)
{
   if ((flag & bit) != bit)  return (flag^bit);
   return(flag);
}

long backupUnSetOption(long flag,long bit)
{
   if ((flag & bit) == bit) return(flag-bit);
   return(flag);
}


typedef struct index_details index_details;
struct index_details
{
   char *    ndx_own;                                                           // owner of the index
   char *    ndx_cat;                                                           // catalog/database of the index
   char *    ndx_sch;                                                           // schema of the index
   char *    ndx_nam;                                                           // Index Name
   char *    ndx_tbl;                                                           // Index table name
   char *    ndx_rfn;                                                           // relfilenode (name of the file in the folder)
   char *    ndx_row;                                                           // owner of the index
   char *    ndx_ddl;                                                           // create indes statement (taken from indexdef column in pg_indexes)
} ;


typedef struct RecmFileItemProperties RecmFileItemProperties;                   // RECM Item file properties
struct RecmFileItemProperties
{
   mode_t    mode;                                                              // File type and mode
   uid_t     uid;                                                               // User ID of owner
   gid_t     gid;                                                               // Group ID of owner
};

typedef struct ArrayItem ArrayItem;                                             // Memory dynamic array
struct ArrayItem
{
   ArrayItem *prv;
   ArrayItem *nxt;
   char *key;                                                                   // Key, if array is sorted
   void *data;                                                                  // user data
};

typedef struct Array Array;
struct Array
{
   ArrayItem *first;
   ArrayItem *last;
   ArrayItem *current;
   int sorted;                                                                  // 0=not sorted   1=up Order   2=Down Order
   int count;                                                                   // Number of item in array
};

struct globalArgs_t                                                             // Global argument table
{
    int  configChanged;                                                         // Signal configuration has changed
    int  verbosity;                                                             // -v verbosity
    int  verbosity_bd;                                                          // -v verbosity state before 'debug/on'
    int  debug;                                                                 // -g debug 
    int  exitRequest;                                                           // 
    int  exitOnError;                                                           // set exit_on_error=yes|no           
    char *host;                                                                 // -h host
    char *port;                                                                 // -p port
    char *username;                                                             // -U user
    char *userpass;                                                             // -P userpassword
    int  isCommand;                                                             // -c argiment is a command to execute
    char *arguments;                                                            // if option '-c' is present, this is a command, otherwise, this is a script to execute
    FILE *htrace;                                                               // when using @debug/on/file=xxxx, file handle of the trace file
    char *tracefile;                                                            // when using @debug/on/file=xxxx, trace file name
} globalArgs;

#define TRACE(...) ({ if (globalArgs.debug == true) { fprintf(stderr,"recm-dbg(%d)[%s]: ",__LINE__,__FUNCTION__);fprintf(stderr,__VA_ARGS__); };if (globalArgs.htrace != NULL){fprintf(globalArgs.htrace,"recm-dbg(%d)[%s]: ",__LINE__,__FUNCTION__);fprintf(globalArgs.htrace,__VA_ARGS__);}})
#define TRACENP(...) ({ if (globalArgs.debug == true) { fprintf(stderr,__VA_ARGS__); };if (globalArgs.htrace != NULL) { fprintf(globalArgs.htrace,__VA_ARGS__);}})

#define INFO(fmt, ...)      ({ fprintf(stderr,"recm-inf: ");fprintf(stderr,fmt, ## __VA_ARGS__); })
#define VERBOSE(fmt, ...)   ({ if (globalArgs.verbosity == true) { fprintf(stderr,"recm-inf: ");fprintf(stderr,fmt, ## __VA_ARGS__);}; })
#define WARN(err,fmt, ...)  ({ fprintf(stderr,"recm-wrn(%x04): ",err);fprintf(stderr,fmt, ## __VA_ARGS__); })
#define ERROR(err,fmt, ...) ({ fprintf(stderr,"recm-err(%x04): ",err);fprintf(stderr,fmt, ## __VA_ARGS__);if (globalArgs.htrace != NULL){fprintf(globalArgs.htrace,"recm-err(%d-%d)[%s]: ",err,__LINE__,__FUNCTION__);fprintf(globalArgs.htrace,fmt, ## __VA_ARGS__);};if (globalArgs.exitOnError == true) { exit(err); }; })


// Error code 
#define ERR_SUCCESS          0
#define ERR_INVOPTION        1 
#define ERR_MISQUALVAL       2 
#define ERR_BADCOMMAND       3 
#define ERR_NOPGDATA         4
#define ERR_FILNOTFND        5
#define ERR_CNXFAILED        6
#define ERR_NOTCONNECTED     6
#define ERR_DEPOEXIST        7
#define ERR_NOTDEPOSIT       8
#define ERR_BADDEPOVER       9
#define ERR_WRONGDEPOSIT    10
#define ERR_ALREADYREG      11
#define ERR_NOTREGISTERED   12
#define ERR_MISSQUOTE       13
#define ERR_FILNOACCESS     14
#define ERR_ZIPADDDIR       15
#define ERR_ZIPADDFILE      16
#define ERR_ERROPNZIP       17
#define ERR_CREATEFILE      18
#define ERR_NOBACKUPDIR     19
#define ERR_INVDIR          20
#define ERR_INVALIDMASK     21
#define ERR_NODIRACCESS     22
#define ERR_NOWALDIR        23 
#define ERR_QUALNOVALUE     24
#define ERR_INVRECMFIL      25
#define ERR_BADPIECE        26
#define ERR_DIROPEN         27
#define ERR_BADVALUE        28
#define ERR_MISSMINOPT      29
#define ERR_DELFAILED       30
#define ERR_DIRNOTEMPTY     31
#define ERR_INVDATFMT       32
#define ERR_NOFREESPACE     33
#define ERR_UNSUPPORTED     34
#define ERR_BADVERSION      35
#define ERR_RESTOREFAILED   36
#define ERR_REINDEXFAIL     37
#define ERR_STOPFAILED      38
#define ERR_CREATRPFAILED   39
#define ERR_BADOPTION       40
#define ERR_DELRPFAILED     41
#define ERR_INVCHARINNAME   42
#define ERR_NAMEEXIST       43
#define ERR_BADCLUNAME      44
#define ERR_SUSPENDED       45
#define ERR_DONOTCONNECT    46
#define ERR_CIDNOTFOUND     47
#define ERR_CLUSTERENABLED  48
#define ERR_BACKUPNOTFND    49
#define ERR_INVALIDPGBIN    50
#define ERR_SHELLERROR      51
#define ERR_INVALIDFORMAT   52
#define ERR_BADLEVEL        53
#define ERR_BADARCHMODE     54
#define ERR_BADARCHCMD      55
#define ERR_INVDBNAME       56
#define ERR_INVSCHNAME      57
#define ERR_INVTBLNAME      58
#define ERR_INVRPNAME       59
#define ERR_INVCLUID        60
#define ERR_MISSCONFIG      61
#define ERR_INVTRANFMT      62
#define ERR_INVPRMNAME      63
#define ERR_INVMETADIR      64
#define ERR_UPGRADEFAIL     65
#define ERR_NOLSNFND        66
#define ERR_ALLOCERROR      67
#define ERR_INVBCKTYPE      68
#define ERR_DIRNOTFND       69
#define ERR_BADDIRPERM      70
#define ERR_CIDALREADYUSED  71
#define ERR_FULLREQUIRED    72
#define ERR_INVALIDCID      73
#define ERR_BCKINPROGRESS   74
#define ERR_MISSDIR         75
#define ERR_STHFAILED       76

#define ERR_INTERNALERROR   199
#define IE_SETPRM           "0x001"          /* ERR_INTERNALERROR Sub-error when changing parameter*/
#define IE_RELOADCONF       "0x002"          /* ERR_INTERNALERROR Sub-error during reload config   */
#define IE_RUNNINGBCK       "0x003"          /* ERR_INTERNALERROR Sub-error during cancel backup   */

#define WRN_INVCOMPLVL       201
#define WRN_BADVERSION       202
#define WRN_STILINRECOVERY   203
#define WRN_NOTREGISTERED    204
#define WRN_NOTDEPOSITCNX    205
#define WRN_RECONOTSTARTED   206
#define WRN_NOCHANGE         207
#define WRN_INVALIDFILTER    208
#define WRN_KEEPWAL          209
#define WRN_GAPFOUND         210
#define WRN_CLUDISABLED      211
#define WRN_INVQUALIFIER     212
#define WRN_FILEPRESENT      213
#define WRN_DIRNOTFND        214
#define WRN_BADDIRPERM       215
#define WRN_BADLSN           216
#define WRN_LOCKEDNCOMPLETE  217
#define WRN_CHANGEDNAME      218
#define WRN_SLOTPRESENT      219

#define ERR_DBQRYERROR     101
#define ERR_DROPDB         102
#define ERR_CREATEDB       103
#define ERR_CREATEROLE     104
#define ERR_GRANTDB        105
#define ERR_GRANTROLE      106
#define ERR_CREATESCHEMA   107
#define ERR_GRANTSCHEMA    108
#define ERR_GRANTUSAGE     109
#define ERR_CREATTABLE     110
#define ERR_BEGBCKFAIL     111
#define ERR_ENDBCKFAIL     112
#define ERR_MISPRIV        113
#define ERR_CLUNONAME      114
#define ERR_MISSQUAL       115
#define ERR_MAPNOTFND      116

#define WRN_CLUNONAME    200
#define WRN_NOARCHIVE    201    

struct commands      // Definition of commands, options and qualifiers
{
   int  id;
   char *cmd;
   char *opt;
   char *qal;
   char *hlp;
};
/*

connect cluster /host=127.0.0.1 /port=5432 /user=postgres /password=postgres /database=postgres  
create deposit /name="depo_prod" /user=recmadm /password="recmadm" /database=depo_prod
register cluster /name=depo_prod /user=recmadm /password="recmadm" /database=depo_prod /host=127.0.0.1 /port=5432

*/
static char *last_pg_ctl_message=NULL;
static PGconn     *clu_cnx=NULL;
static PGconn     *rep_cnx=NULL;
static PGconn     *aux_cnx=NULL;
static zip_int32_t zip_method=ZIP_CM_DEFAULT;

#define PGresID_LIMIT 32

typedef struct RecmPQRes RecmPQRes;
struct RecmPQRes
{
   PGresult *res;
   long     rows;
};

#define CNX_MAX_LEVELS 16

static int PGresID=0;
RecmPQRes DEPOresult[CNX_MAX_LEVELS];

static int PGcluID=0;
PGresult *CLUresult[CNX_MAX_LEVELS];

static int PGauxID=0;
PGresult *AUXresult[CNX_MAX_LEVELS];

// Rention MAX value
#define RETENTION_DAYS_MAX 365
#define RETENTION_COUNT_MAX  52

#define RETENTION_MODE_DAYS    1
#define RETENTION_MODE_COUNT   2


// Backup status (STRING)
#define RECM_BACKUP_STATE_AVAILABLE_S  "AVAILABLE"
#define RECM_BACKUP_STATE_OBSOLETE_S   "OBSOLETE"
#define RECM_BACKUP_STATE_INCOMPLETE_S "INCOMPLETE"
#define RECM_BACKUP_STATE_RUNNING_S    "RUNNING"
#define RECM_BACKUP_STATE_FAILED_S     "FAILED"

// Backup status (NUMERIC)
#define RECM_BACKUP_STATE_AVAILABLE   0
#define RECM_BACKUP_STATE_OBSOLETE    1
#define RECM_BACKUP_STATE_INCOMPLETE  2
#define RECM_BACKUP_STATE_RUNNING     3
#define RECM_BACKUP_STATE_FAILED      4

#define RECM_BCKTYP_FULL        "FULL"
#define RECM_BCKTYP_WAL         "WAL"
#define RECM_BCKTYP_CFG         "CFG"
#define RECM_BCKTYP_META        "META"

#define RECM_LOGDIR             "@logdir"
#define RECM_WALSTART           "@walStart"
#define SYSVAR_HOSTNAME         "@hostname"
#define SYSVAR_CMDFILE          "@cmdfile"
#define SYSVAR_TARGET           "@target"
#define SYSVAR_DEPOSIT          "@deposit"
#define SYSVAR_LASTLSN          "@lastLSN"
#define RECM_BCKTYPE            "BckType"
#define RECM_VERSION            "@RecmVersion"
#define RECM_BCKID              "@backupID"
#define RECM_BCKSEQ             "@sequence"
#define RECM_START_LSN          "@lsn_start" 
#define RECM_STOP_LSN           "@lsn_stop" 
#define RECM_START_WALFILE      "@wal_start" 
#define RECM_STOP_WALFILE       "@wal_stop" 
#define RECM_PG_CTL             "@cluster_pgctl"

// Variables from configuration file : default
#define SYSVAR_PROMPT                 "Prompt"
#define SYSVAR_DATE_FORMAT          "@date_format"
#define SYSVAR_HISTDEEP             "@historic_Deep"

#define SYSVAR_AUTODELWAL           "@auto_delete_wal"
#define SYSVAR_DELAYDELWAL          "@delay_delete_wal"
#define SYSVAR_READBLOCKSIZE        "@read_block_size"
#define SYSVAR_BACKUP_MAXSIZE       "@backup_maxsize"
#define SYSVAR_BACKUP_MAXFILES      "@backup_maxfile"
#define SYSVAR_BACKUP_CFGFILES      "@backup_cfg_files" 
#define SYSVAR_BACKUP_EXCLDIR       "@backup_excldir"
#define SYSVAR_RETENTION_FULL       "@retentionFull"
#define SYSVAR_RETENTION_CONFIG     "@retentionConfig"

#define SYSVAR_PGBIN                "@defaultPGBIN"
#define SYSVAR_TIMEZONE             "@defaultTimeZone"

// Cluster status
#define CLUSTER_STATE_ENABLED         "ENABLED"
#define CLUSTER_STATE_DISABLED        "DISABLED"

// set cluster/backup_mask="RWX:R--:R--"

// Type of section from configuration file
#define SECTION_DEFAULT 0
#define SECTION_CLUSTER 1
#define SECTION_DEPOSIT 2

// Variables used with 'set source' for restoring backup without beeing connected to any cluster
#define SOURCE_DIRECTORY "SRCDIR"
#define SOURCE_BLKSIZE   "BLKSIZ"


// Variables from configuration file : Cluster
#define GVAR_CLUDESCR   "CluDescription"
#define GVAR_CLUIP      "CluIP"
#define GVAR_CLUHOST    "CluHost"
#define GVAR_CLUPORT    "CluPort"
#define GVAR_CLUUSER    "CluUser"
#define GVAR_CLUPWD     "CluPwd"
#define GVAR_CLUDB      "CluDB"
#define GVAR_CLUNAME    "CluName"
#define GVAR_REGDEPO    "CluRegDepo"
#define GVAR_CLUCID     "CluCID"
#define GVAR_CLUVERSION "CluVersion"
#define GVAR_CLUTL      "CluTL"
#define GVAR_CLUPGDATA  "CluPGDATA"
#define GVAR_WALDIR     "CluWALdir"
#define GVAR_BACKUPDIR  "CluBackupdir"
#define GVAR_COMPLEVEL  "CluCompressionLevel"
#define GVAR_SOCKPATH   "CluUSP"
#define GVAR_PGCTL      "CluPG_CTL"
#define GVAR_CLUTZ      "CluTimeZone"
#define GVAR_CLUDS      "CluDateStyle"

#define GVAR_CLUDATE_FORMAT          "CluDate_format"
#define GVAR_CLUMASK_BACKUP          "CluBackup_dir_mask"
#define GVAR_CLUMASK_RESTORE         "CluRestore_dir_mask"
#define GVAR_CLUAUTODELWAL           "CluAuto_delete_wal"
#define GVAR_CLUREADBLOCKSIZE        "CluRead_block_size"
#define GVAR_CLUBACKUP_MAXSIZE       "CluBackup_maxsize"
#define GVAR_CLUBACKUP_MAXFILES      "CluBackup_maxfile"
#define GVAR_CLUBACKUP_MAXWALSIZE    "CluBackup_maxWalSize"
#define GVAR_CLUBACKUP_MAXWALFILES   "CluBackup_maxWalFile"
#define GVAR_CLURETENTION_FULL       "CluRetentionFull"
#define GVAR_CLURETENTION_CONFIG     "CluRetentionConfig"
#define GVAR_CLURETENTION_META       "CluRetentionMeta"    
#define GVAR_REINDEXCONCURRENTLY     "CLUConcurrentlyReIndex"

// Variables from configuration file : Deposit
#define GVAR_DEPDESCR       "DepDescription"
#define GVAR_DEPHOST        "DepoHost"
#define GVAR_DEPPORT        "DepoPort"
#define GVAR_DEPDB          "DepoDB"
#define GVAR_DEPUSER        "DepoUser"
#define GVAR_DEPPWD         "DepoPwd"
#define GVAR_DEPNAME        "DepoName"

#define VAR_SET_VALUE       "<SET>"
#define VAR_UNSET_VALUE     "<UNSET>"
#define OPTION_SET_VALUE    "<OPT_SET>"
#define OPTION_UNSET_VALUE  "<OPT_UNSET>"
#define QAL_UNSET           "<MISSING>"

#define GVAR_CLUFULLVER     "CluFulLVersion"


// Variables from configuration file : Auxiliary connection
#define GVAR_AUXDESCR       "AuxDescription"
#define GVAR_AUXHOST        "AuxHost"
#define GVAR_AUXPORT        "AuxPort"
#define GVAR_AUXUSER        "AuxUser"
#define GVAR_AUXPWD         "AuxPwd"
#define GVAR_AUXDB          "AuxDB"
#define GVAR_AUXNAME        "AuxName"
#define GVAR_AUXCID         "AuxCID"
#define GVAR_AUXVERSION     "AuxVersion"
#define GVAR_AUXFULLVER     "AuxFulLVersion"
#define GVAR_AUXTL          "AuxTL"
#define GVAR_AUXPGDATA      "AuxPGDATA"

// Command line index
#define CMD_EXIT             1
#define CMD_QUIT             3
#define CMD_HELP             2
#define CMD_LISTBACKUP       1001
#define CMD_LISTRP           1002
#define CMD_LISTWAL          1003
#define CMD_RESTOREWAL       1004
#define CMD_RESTORECFG       1005
#define CMD_RESTOREMETA      1006
#define CMD_REGFILES         1007
#define CMD_DUPLICATEPARTIAL 1008
#define CMD_SETSOURCE        1009
#define CMD_SETMAPPING       1010
#define CMD_LISTMAPPING      1011
#define CMD_DELMAPPING       1012

#define CMD_BACKUPWAL        1201
#define CMD_BACKUPCFG        1202
#define CMD_BACKUPFULL       1203
#define CMD_CREATEDEPOSIT    1204
#define CMD_SHOWBACKUP       1205
#define CMD_CNXCLUSTER       1206
#define CMD_REGCLUSTER       1207
#define CMD_RELOAD           1208
#define CMD_SAVE             1209
#define CMD_MODIFYDEPOSIT    1210
#define CMD_SETCONFIG        1212
#define CMD_CANCELBACKUP     1213
#define CMD_VERIFYBACKUP     1214
#define CMD_SHOWDEPOSIT      1215
#define CMD_SHOWCONFIG       1216
#define CMD_SHOWCLUSTER      1218
#define CMD_DELETEBACKUP     1219
#define CMD_SWITCHWAL        1220
#define CMD_HISTORY          1221
#define CMD_RESTOREFULL      1222
#define CMD_CREATERP         1223
#define CMD_CNXDEPOSIT       1224
#define CMD_DELETERP         1225
#define CMD_DUPLICATEFULL    1226
#define CMD_LISTCLUSTER      1227
#define CMD_DELETECLUSTER    1228
#define CMD_MODIFYCLUSTER    1229
#define CMD_MODIFYBACKUP     1230 
#define CMD_BACKUPMETA       1231
#define CMD_DISCONCLU        1232
#define CMD_DISCONDEPO       1233
#define CMD_DISCONAUX        1234
#define CMD_STATCLUSTER      1235
#define CMD_STATDEPOSIT      1236
#define CMD_CLRSCR           1237
#define CMD_SQL              1238
#define CMD_RESTOREPARTIAL   1239
#define CMD_CNXTARGET        1240
#define CMD_EXPCFG           1241
#define CMD_STATUS           1242
#define CMD_UPGRADEDEPOSIT   1244

// DEBUGGING functions
// @SHOW VARIABLES : show variables /name="<search>"
#define SYS_SHOWVARIABLE     1217
// @DEBUG : debug on/off /file="<tracefile>"
#define SYS_DEBUG            1243

// M = Mandatory
// D = date
// S = String
// N = Numeric

struct optHelp  // command Help definition structure
{
   int  id;
   char *opt;
   char *hlp;
};

// definition of : static struct optHelp recm_help_options[]
#include "recm_opthelp.h"
// definition of : commands recm_commands[]
#include "recm_commands.h"

typedef struct ZIPPiece ZIPPiece;
struct ZIPPiece
{
   int  id;
   char *pname;
};

typedef struct Restore Restore;
struct Restore
{
    int    kind;         // 0=DATA 1=WAL
    char   *bck_id;      // Backup UID
    char   *piece;       // zip file
    char   *file;        // file to restore
};

// Content of the zip comment 
typedef struct RecmFile RecmFile;
struct RecmFile
{
   int   processed;
   char *filename;
   char *bck_id;    
   int  piece;      
   char *bcktyp;        // 'FULL' | 'WAL' | 'CFG'     
   int  version;        // PG_VERSION value    
   int  timeline;       // Timeline when the backup has been created   
   char *bcktag;        // Tag value        
   long bcksize;        // Size of the backup (not the zip file)
   long zipsize;
   long  pcount;         // number of files in zip file
   char *begin_date;    // Begin backup date   
   char *begin_wall;    // LSN when backup start
   char *begin_walf;    // WAL file                 
   char *end_date;      // End backup date                               
   char *end_wall;      // LSN when backup end
   char *end_walf;      // WAL file             
   long options;        // Option '/noindex'
};   


typedef struct RecmDir RecmDir;
struct RecmDir
{
    long   fcnt;   // Number of files in the directory
    long   tsiz;   // Total size of the directory
    mode_t perm;   // Permission of the directory
    uid_t  uid;    // user ID of file
    gid_t  gid;    // group ID of file
    char   *lnk;   // this is a link
};


#define TARGET_TABLESPACE 1

typedef struct TargetPiece TargetPiece;
struct TargetPiece
{
    int    tgtype;   // Target type 'TARGET_TABLESPACE'
    char   *name;    // Target 'name'
    char   *value;   // Target 'value'
    void   *t1;      // used by 'duplicate'
    void   *t2;      // used by 'duplicate'
    void   *t3;
};


static Array   *oid_indexes=NULL;                                               // oid of all indexes at backup time
static Array   *zip_filelist=NULL;                                              // List of all pieces created during backup
static Array   *target_list=NULL;
static long    sequence;                                                        // Sequence counter used by 'backup' command to create pieces.

static void *memory_allocated[512];
static int memory_allocated_pointer=0;

void memBeginModule()
{
   memory_allocated_pointer++;
   memory_allocated[memory_allocated_pointer]=0x00;                             // Reserve first '0x00'
   TRACE("ARGS(none) MAP=%d\n",memory_allocated_pointer);
}

void memEndModule()
{
  TRACE("ARGS(none) MAP Value=%d ",memory_allocated_pointer);
  while (memory_allocated[memory_allocated_pointer] != 0x00 )
  {
     free(memory_allocated[memory_allocated_pointer]);
     memory_allocated[memory_allocated_pointer]=0x00;
     memory_allocated_pointer--;
  }
  if (memory_allocated_pointer > 0) memory_allocated_pointer--;                 // Avoid first '0x00' to be removed
  TRACENP("Final=%d\n",memory_allocated_pointer);
}

void memInit()
{
   memory_allocated_pointer=0;
   memory_allocated[memory_allocated_pointer]=0x00;                             // Reserve first '0x00'
}
 

void *memAlloc(int size)
{
   void *mem=malloc(size);
   if (mem == NULL)
   {
      ERROR(ERR_ALLOCERROR,"Allocation of %dByte(s) failed (%d)\n",size,errno);
      exit(1);
   }
   memory_allocated_pointer++;
   memory_allocated[memory_allocated_pointer]=mem;
   TRACE("ARGS(size=%d) -- RETURN (pointer) [Success]\n",size);
   return(mem);
}


/**********************************************************************************/
/* Create Memory Tree                                                             */
/**********************************************************************************/
TreeEntry *treeCreate()
{
   TreeEntry *root=malloc(sizeof(TreeEntry));
   root->totalsize=0;
   root->entries=0;
   root->first=NULL;
   root->last=NULL;
   root->currentFolder=NULL;
   return(root);
}

/**********************************************************************************/
/* Destroy Memory Tree and its content                                            */
/**********************************************************************************/
void treeDestroy(TreeEntry *root)
{
   TRACE("Cleanup start.\n");
   TreeItem *itm=root->first;
   while (itm != NULL)
   {
      free(itm->name);
      TreeItem *nxt=itm->next;
      free(itm);
      itm=nxt;
   }
   free(root);
   TRACE("Cleanup End.");
}

/**********************************************************************************/
/* Memorize all objects from directories                                          */
/**********************************************************************************/
void treeAddItem(TreeEntry *tree,const char *filename,unsigned flag,struct stat *st)
{
   TreeItem *itm=malloc(sizeof(TreeItem));
   itm->flag=flag;
   itm->name=strdup(filename);
   itm->size=st->st_size;
   itm->st_mode=st->st_mode;
   itm->st_uid=st->st_uid; 
   itm->st_gid=st->st_gid; 
   itm->mtime=st->st_mtime;
   itm->next=NULL;
   if (flag != TREE_ITEM_FILE)
   {
      tree->currentFolder=itm;itm->path=NULL; 
      TRACE("treeAddItem(Directoryname='%s' [path:'%s'])\n",filename,tree->currentFolder->name);
   }
   else
   {
      itm->path=tree->currentFolder;
      if (tree->currentFolder != NULL)
      {
         TRACE("treeAddItem(filename='%s' [path:'%s'])\n",filename,tree->currentFolder->name);
      }
      else
      {
         TRACE("treeAddItem(filename='%s' [path:'<NULL>'])\n",filename);
      }
   };
   if (tree->last == NULL)
   {
      tree->first=itm;
      tree->last=itm;
   }
   else
   {
      tree->last->next=itm;
      tree->last=itm;
   }
   tree->entries++;
   tree->totalsize+=st->st_size;
}

/********************************************************************************/
/* Initialize Thread array                                                      */
/********************************************************************************/
void threadInitalizeTable()
{
   thread_all_file_processed=0;                                    // Reset Statistics counters
   thread_all_size_processed=0;                                    // Reset Statistics counters  
   int idThread;
   for(idThread=0; idThread < MAX_THREADS; idThread++) 
   {
      threads[idThread].state=THREAD_FREE;                         // Reset ALL threadID.  When a thread is finish, this value is set to THREAD_FREE.
      threads[idThread].rc=0; 
      threads[idThread].bck_id=malloc(128);
      threads[idThread].bck_typ=malloc(10);
      threads[idThread].rootdirectory=malloc(1024);
   };
   threads[0].pieceNumber=THREAD_FREE;
}

/********************************************************************************/
/* Check how many thread is currently running                                   */
/********************************************************************************/
int threadCountRunning(int maxThreads)
{
   int count=0;
//   pthread_mutex_lock( &mutex1 );                                               // Begin of critical section
   int idThread=0;
   while (idThread < MAX_THREADS) 
   {
      if (threads[idThread].state == THREAD_RUNNING) count++;
      idThread++;
   };
//   pthread_mutex_unlock( &mutex1 );                                             // End of critical section
   return(count);
}

/********************************************************************************/
/* Get Available thread table item for next thread                              */
/* used in 'backup full' and backup wal' command                                */
/********************************************************************************/
int ThreadGetAvailableProcess(int maxThreads)
{
   pthread_mutex_lock( &mutex1 );                                               // Begin of critical section
   int selected= -1;
   int count=20;
   while (count > 0 && selected == -1)
   {
      int idThread=0;
      while (idThread < MAX_THREADS && threads[idThread].state == THREAD_RUNNING) { idThread++; };
      if (idThread < MAX_THREADS && threads[idThread].state != THREAD_RUNNING) 
      {
         selected=idThread;
         threads[idThread].state=THREAD_FREE;
      }
      else
      {
         pthread_mutex_unlock( &mutex1 );                                       // End of critical section during wait...
         printf("*** Wait ***\n");
         sleep(1);
         count--;
         pthread_mutex_lock( &mutex1 );                                         // Begin of critical section
      };
   }
   pthread_mutex_unlock( &mutex1 );                                             // End of critical section
   return (selected);
}

/**********************************************************************************/
/* Used for 'TAG keyword'                                                         */
/*      for 'restore point name'                                                  */
/* Accept only letters and number and '_'.                                        */
/**********************************************************************************/
int isValidName(const char *buffer)
{
   char *p=(char *)buffer;
   int rc=true;
   while (rc == true && p[0] != 0x00)
   {
      switch (p[0])
      {
      case 45 : ;              // '-'
      case 48 ... 57  : ;      // '0'..'9'
      case 97 ... 122 : ;      // 'a'..'z' 
      case 65 ... 90  : ;      // 'A'..'Z'
      case 95         : break; // '_'
      default: rc=false;
      }
      p++;
   }
   return(rc);
}

char *varGet(const char *pnam);
void varAdd(const char *pnam,const char *pval);

void printSection(const char *section)
{
   printf(" [%s]\n",section);
}

void printOption(const char *description,const char *qualifier,const char *value)
{
   char inf[80];
   strcpy(inf,description);
   if (strlen(qualifier) > 0) { strcat(inf," ( ");
                                strcat(inf,qualifier);
                                strcat(inf," )");
                              };
   while (strlen(inf) < 50) {strcat(inf,"."); }
   printf("     %-45s : '%s'\n",inf,value);
}


long hexBin(const char *wal)
{
    int n=strlen(wal);
    int unit=1;
    char *w=(char *)wal;
    long reponse=0;
    w+=(n-1);
    while (n > 0)
    {
       int x=w[0];
       if (x < 58) { x=x-48; } else { x=(x-65)+10; }; // uppercase A-F
       reponse=reponse+(x*unit);
       unit=(unit *16);
       w--;
       n--;
    }
    return(reponse);
}

char *WALFILE_FROM_LSN(const char *lsn,int timeline)
{
/*
recm_db=# select pg_walfile_name('22/7D000060');
     pg_walfile_name      
--------------------------
 00000001000000220000007D
(1 row)

*/
   static char *result=NULL;
   if (result == NULL) result=malloc(128);
   strcpy(result,lsn);
   char *slash=strchr(result,'/');
   if (slash == NULL) { return(NULL); };
   slash[0]=0x00;
   slash++;
   char *left=malloc(20);
   char *right=malloc(20);
   strcpy(right,slash);
   strcpy(right,"00000000");
   right[6]=slash[0];
   right[7]=slash[1];

   strcpy(left,result);
   sprintf(result,"%08X%8s%8s",timeline,left,right);
   char *x=result;
   while (x[0] != 0x00) 
   {
      if (x[0] == ' ') x[0]='0';
      x++;
   };
   free(right);
   free(left);
   return(result);
}

// 9/9D000060
#define LSN_ISLT 1
#define LSN_ISGE 2
#define LSN_1ISBAD -1
#define LSN_2ISBAD -2

int LSN_COMPARE(const char*lsn1,const char *lsn2)
{ 
  long LSN1_TL;
  long LSN1_LSN;
  long LSN1_NDX;
  long LSN2_TL;
  long LSN2_LSN;
  long LSN2_NDX;
  
  char * right=malloc(17);
  char *lsn=strdup(lsn1);
  char *slash=strchr(lsn,'/');
  if (slash == NULL) { free(lsn);free(right);return(LSN_1ISBAD); };
  
  char *left=lsn;
  slash[0]=0x00;slash++;
  sprintf(right,"%16s",slash);
  slash=right;
  while ( slash[0] != 0x00) { if (slash[0] == ' ') { slash[0]='0'; }; slash++;};
  LSN1_TL=hexBin(left);

  char * lls=right;lls+=8;
  LSN1_NDX=hexBin(lls);
  lls[0]=0x00;lls++;
  LSN1_LSN=hexBin(right);
  
  free(lsn);
  lsn=strdup(lsn2);
  slash=strchr(lsn,'/');
  if (slash == NULL) { free(lsn);free(right);return(LSN_2ISBAD); };
  left=lsn;
  slash[0]=0x00;slash++;
  sprintf(right,"%16s",slash);
  slash=right;
  while ( slash[0] != 0x00) { if (slash[0] == ' ') { slash[0]='0'; }; slash++;};
  LSN2_TL=hexBin(left);
  
  lls=right;lls+=8;
  LSN2_NDX=hexBin(lls);
  lls[0]=0x00;lls++;
  LSN2_LSN=hexBin(right);
  TRACE ("LSN1 TL=%ld LSN=%ld NDX=%010ld\n",LSN1_TL,LSN1_LSN,LSN1_NDX);
  TRACE ("LSN2 TL=%ld LSN=%ld NDX=%010ld\n",LSN2_TL,LSN2_LSN,LSN2_NDX);
  free(lsn);
  if (LSN1_TL < LSN2_TL)   { return(LSN_ISLT); };
  if (LSN1_LSN < LSN2_LSN) { return(LSN_ISLT); };
  if (LSN1_NDX < LSN2_NDX) { return(LSN_ISLT); };
  free(right);
  return(LSN_ISGE);
}

char *scramble(const char *pass)
{
   
   int n=666;
   int w=0;
   char *p=(char *)pass;
   static char res[130];
   static char hx[4];
   sprintf(res,"%03X",n);
   while (p[0] != 0x00)
   {
      int c=(short)p[0];
      sprintf(hx,"%03X",(c+n));
      strcat(res,hx);
      n--;
      w++;
      p++;
   }
   while (w < 24)
   {
      sprintf(hx,"%03X",(n+27));
      strcat(res,hx);
      n--;
      w++;
   }
   sprintf(hx,"%03X",n);
   strncpy(res,hx,3);
   TRACE("ARGS(pass='%s') --> RETURN (string) '%s'\n",pass,res);
   return(res);
}

char *unscramble(const char *pass)
{
   int n=666;
   char *p=(char *)pass;
   static char res[130];
   int v=0;
   if (p[0] > 57) { v=v+ (256*((p[0]-65)+10)); } else  {v=v+(256*(p[0]-48)); }
   p++;
   if (p[0] > 57) { v=v+ (16*(p[0]-65)+10); } else  {v=v+(16*(p[0]-48)); }
   p++;
   if (p[0] > 57) { v=v+ (p[0]-65)+10; } else  {v=v+(p[0]-48); }
   p++;
   int c=0;
   static char hx[4];
   res[0]=0x00;
   while (p[0] != 0x00)
   {
      v=0;
      c=(short)p[0];
      if (c > 57) { v=v+ (256*((c-65)+10)); } else  {v=v+(256*(c-48)); }
      p++;
      c=(short)p[0];
      if (c > 57) { v=v+ (16*((c-65)+10)); } else  {v=v+(16*(c-48)); }
      p++;
      c=(short)p[0];
      if (c > 57) { v=v+ (c-65)+10; } else  {v=v+(c-48); }
      p++;
      if ((v-n) != 27)
      {
         sprintf(hx,"%c",(v-n));
         strcat(res,hx);
      };
      n--;
   }
   TRACE("ARGS(pass='%s') --> RETURN (string) '%s'\n",pass,res);
   return(res);
}

long getWALtimeline(const char *wal)
{
   char *pW=(char *)wal;
   char *WAL_TL=malloc(10);    
   strncpy(WAL_TL,pW,8);
   char *p=WAL_TL;
   p+=8;     
   p[0]=0x00;
   long result=hexBin(WAL_TL);
   free(WAL_TL);
   TRACE("ARGS(wal='%s') -- RETURN (long) %ld\n",wal,result);
   return(result);
}

/**********************************************************************************/
/* Compute next WAL file name                                                     */
/**********************************************************************************/
char *ComputeNextWAL(const char *wal)
{
    static char *result=NULL;
    if (result == NULL) result=malloc(128);
    char *p;
    char *pW=(char *)wal;
    char *WAL_TL=malloc(10);    strncpy(WAL_TL,pW,8);       p=WAL_TL;p+=8;     p[0]=0x00; pW+=8;
    char *WAL_LOG=malloc(18);   strncpy(WAL_LOG,pW,8);      p=WAL_LOG;p+=8;    p[0]=0x00; pW+=8;
    char *WAL_OFFSET=malloc(10);strncpy(WAL_OFFSET,pW,8);   p=WAL_OFFSET;p+=8; p[0]=0x00;   
    long TL=hexBin(WAL_TL);
    long LOG=hexBin(WAL_LOG);
    long OFFSET=hexBin(WAL_OFFSET);
    OFFSET++;
    if (OFFSET > 255)
    {
       OFFSET=0;
       LOG++;
       if (LOG > 16777215)
       {
          TL++;
          LOG=0;
       };
       if (TL > 16777215)
       {
          TL=1;
       };
    }
    free(WAL_OFFSET);
    free(WAL_LOG);
    free(WAL_TL);
    sprintf(result,"%08lX%08lX%08lX",TL,LOG,OFFSET);
    TRACE("ARGS(wal='%s') [ Split WAL : TL=%ld LOG=%ld OFFSET=%ld ] --> RETURN (string) '%s'\n",wal,TL,LOG,OFFSET,result);
    return(result);
}



int maskIsValid(const char *mask)
{
   char *pmask=(char *)mask;
   while (pmask[0] != 0x00)
   {
      if (strchr("rwxRWX:-",pmask[0]) == NULL) { ERROR(ERR_INVALIDMASK,"Mask format must be 'rwx:rwx:rwx'\n"); return(false); };
      pmask++;
   }
   return(true);
}

char *maskCreate(mode_t mode)
{
   static char msk[20];
   memset(msk,0x00,20);
   if ((mode & S_IRUSR) == S_IRUSR) strcat(msk,"R");
   if ((mode & S_IWUSR) == S_IWUSR) strcat(msk,"W");
   if ((mode & S_IXUSR) == S_IXUSR) strcat(msk,"X");
   strcat(msk,":");
   if ((mode & S_IRGRP) == S_IRGRP) strcat(msk,"R");
   if ((mode & S_IWGRP) == S_IWGRP) strcat(msk,"W");
   if ((mode & S_IXGRP) == S_IXGRP) strcat(msk,"X");
   strcat(msk,":");            
   if ((mode & S_IROTH) == S_IROTH) strcat(msk,"R");
   if ((mode & S_IWOTH) == S_IWOTH) strcat(msk,"W");
   if ((mode & S_IXOTH) == S_IXOTH) strcat(msk,"X");
   //TRACE("ARGS(mode=%d (%02x)) --> RETURN (char *) '%s'\n",mode,mode,msk);
   return(&msk[0]);
}

mode_t maskTranslate(const char *mask)
{
   mode_t mode=0;
   char *pmask=(char *)mask;
   int grp=0; // 0=owner 1=group 2=other
   while (pmask[0] != 0x00)
   {
      switch(pmask[0])
      {
      case '-' : break; // skp this character   
      case ':' : grp++;
                 break;
      case 'r' : ;
      case 'R' : switch(grp)
                 {
                 case 0 : mode=mode | S_IRUSR;break;
                 case 1 : mode=mode | S_IRGRP;break;
                 case 2 : mode=mode | S_IROTH;break;
                 };
                 break;
      case 'w' : ;
      case 'W' : switch(grp) 
                 {
                 case 0 : mode=mode | S_IWUSR;break;
                 case 1 : mode=mode | S_IWGRP;break;
                 case 2 : mode=mode | S_IWOTH;break;
                 };
                 break;
      case 'x' :;
      case 'X' : switch(grp) 
                 {
                 case 0 : mode=mode | S_IXUSR;break;
                 case 1 : mode=mode | S_IXGRP;break;
                 case 2 : mode=mode | S_IXOTH;break;
                 };
                 break;
      default: ERROR(ERR_INVALIDMASK,"Mask format must be 'rwx:rwx:rwx'\n"); 
               return(false);
      };
      pmask++;
   };
   char verif[20];strcpy(verif,"msk=");
   if ((mode & S_IRUSR) == S_IRUSR) { strcat(verif,"R"); } else { strcat(verif,"-"); };
   if ((mode & S_IWUSR) == S_IWUSR) { strcat(verif,"W"); } else { strcat(verif,"-"); };
   if ((mode & S_IXUSR) == S_IXUSR) { strcat(verif,"X"); } else { strcat(verif,"-"); };
   if ((mode & S_IRGRP) == S_IRGRP) { strcat(verif,"R"); } else { strcat(verif,"-"); };
   if ((mode & S_IWGRP) == S_IWGRP) { strcat(verif,"W"); } else { strcat(verif,"-"); };
   if ((mode & S_IXGRP) == S_IXGRP) { strcat(verif,"X"); } else { strcat(verif,"-"); };
   if ((mode & S_IROTH) == S_IROTH) { strcat(verif,"R"); } else { strcat(verif,"-"); };
   if ((mode & S_IWOTH) == S_IWOTH) { strcat(verif,"W"); } else { strcat(verif,"-"); };
   if ((mode & S_IXOTH) == S_IXOTH) { strcat(verif,"X"); } else { strcat(verif,"-"); };
   TRACE("ARGS(mask='%s') --> RETURN (mod_t) %x VERIF='%s'\n",mask,mode,verif);
   return(mode);
}

/**********************************************************************************/
/* Extract retntion count from syntax: 'count:<n>' or 'days:<n>'                  */
/**********************************************************************************/
int retentionCount(char *pbuffer)
{
   
   if (pbuffer == NULL) { TRACE("ARGS(pbuffer='%s') --> RETURN (int) 7 [DEFAULT]\n",pbuffer);return(7); };
   char *count=strchr(pbuffer,':');
   if (count == NULL) { TRACE("ARGS(pbuffer='%s') --> RETURN (int) 7 [DEFAULT]\n",pbuffer);return(7); };
   count++;
   { TRACE("ARGS(pbuffer='%s') --> RETURN (int) %d\n",pbuffer,atoi(count)); };
   return(atoi(count));
}

char *getFileNameOnly(char *filename)
{
   char *res=filename;
   res+=strlen(filename);
   while (res != filename && res[0] != '/') { res--;};
   if (res[0] == '/') res++;
   TRACE("ARGS(filename='%s') --> RETURN (char *) '%s'\n",filename,res);
   return(res);
}

/**********************************************************************************/
/* Get Free space on specified directory in MB                                    */
/**********************************************************************************/
long freeSpaceMB(char *dirname)
{
   struct statvfs fs;
   int rc=statvfs(dirname, &fs);
   if (rc != 0)
   {
      TRACE("ARGS(dirname='%s') --> RETURN (long) 0 (err:%d)\n",dirname,errno);
      return(0);
   };
   long result=((fs.f_bsize * fs.f_bavail) / 1024) /1024;
   TRACE("ARGS(dirname='%s') --> RETURN (long) %ld\n",dirname,result);
   return( result);
}

int directoryIsEmpty(char *dirname) 
{
  int n = 0;
  struct dirent *d;
  DIR *dir = opendir(dirname);
  if (dir == NULL) return true;
  while ((d = readdir(dir)) != NULL) 
  {
      if(++n > 2)
      break;
  }
  closedir(dir);
  if (n <= 2) //Directory Empty
    return true;
  else
    return false;
}

/**********************************************************************************/
/* Verify if directory is writeable                                               */
/**********************************************************************************/
int directoryIsWriteable(const char *directory)
{
   char *fn=malloc(1024);
   sprintf(fn,"%s/recm_$$.tmp",directory);
   FILE *FH=fopen(fn,"w");
   if (FH == NULL) return(false);
   fprintf(FH,"#test write\n");
   fclose(FH);
   unlink(fn);
   return(true);
}   

/**********************************************************************************/
/* Verify if directory exist                                                      */
/**********************************************************************************/
int directory_exist(const char *directory)
{
   struct stat st;
   if (stat(directory, &st) == 0 && S_ISDIR(st.st_mode)) { return(true); } 
   return(false);
}

/**********************************************************************************/
/* Verify if file exist                                                           */
/**********************************************************************************/
int file_exists (char *filename) 
{
   struct stat st;
   if (stat (filename, &st) == 0) { return(true); }
   return(false);
}


/**********************************************************************************/
/* Create directory tree                                                          */
/**********************************************************************************/
int mkdirs(const char *path,mode_t mode)
{
   int rc=0;
   char *wrkpath=malloc(strlen(path)+10);
   strcpy(wrkpath,path);

   char *end=wrkpath;
   end++;                            // First character is  '/'...bypass it
   end=strchr(end,'/');
   while (end != NULL && rc == 0)
   {
      end[0]=0x00;
      if (directory_exist(wrkpath) == false) rc=mkdir(wrkpath,mode);
      end[0]='/';
      end++;
      end=strchr(end,'/');
   };
   if (directory_exist(path) == false) rc=mkdir(path,mode);
   TRACE("ARGS(Path='%s' mode='%s') --> RETURN (int) %d\n",path,maskCreate(mode),rc);
   return(rc);
}

/**********************************************************************************/
/* Display unzip progression (in text)                                            */
/**********************************************************************************/
void displayProgressBar(int curpos,int itemcount,int displayWidth,const char *info)
{
   static int highestinfo=0;
   double position=((1-(curpos / itemcount))*100);
   double offset=((trunc( (double) 100-position)/100)*displayWidth);
   char *barre=malloc(2048);
   memset(barre,0x00,displayWidth+2);
   if (curpos >= itemcount)
   {
      memset(barre,'#',80);
      position=0.0;
   }
   else
   {
      memset(barre,' ',displayWidth);
      memset(barre,'#',offset);
   };
   if (info != NULL && strlen(info) > highestinfo) highestinfo=strlen(info);
   if (info != NULL) { strcat(barre," ");strcat(barre,info); }
                else { for (int i=0; i < highestinfo; i++) { strcat(barre," "); }; };

   printf("%2.2f%% %s\r",(100-position),barre);
   fflush(stdout);
   if (curpos >= itemcount) printf("\n");
   free(barre);
}

/**********************************************************************************/
/* Create pg_hba.conf file after a duplicate                                      */
/* the created file authorize any connection from localhost                       */
/**********************************************************************************/
void create_hba_file(const char *pgdata)
{
   char *FileName=malloc(1024);
   char *backupFileName=malloc(1024);
   sprintf(FileName,"%s/pg_hba.conf",pgdata);
   sprintf(backupFileName,"%s/original_pg_hba.conf",pgdata);
   if (file_exists(backupFileName)==true) unlink(backupFileName);
   if (file_exists(FileName)==true) rename(FileName,backupFileName);
   FILE *FH=fopen(FileName,"w");
   fprintf(FH,"# TYPE  DATABASE        USER            ADDRESS                 METHOD\n");
   fprintf(FH,"\n");
   fprintf(FH,"# \"local\" is for Unix domain socket connections only\n");
   fprintf(FH,"local   all             all                                     trust\n");
   fprintf(FH,"# IPv4 local connections:\n");
   fprintf(FH,"host    all             all             127.0.0.1/32            trust\n");
   fprintf(FH,"# IPv6 local connections:\n");
   fprintf(FH,"host    all             all             ::1/128                 trust\n");
   fclose(FH);
   TRACE ("ARGS(pgdata='%s') --> RETURN (none)\n",pgdata);
}


/**********************************************************************************/
/* Create default postgresql.conf file after a duplicate                          */
/**********************************************************************************/
void create_config_file(const char *pgdata,long portnumber,const char *newname)
{
   char *FileName=malloc(1024);
   sprintf(FileName,"%s/postgresql.conf",pgdata);
   FILE *FH=fopen(FileName,"w");
   fprintf(FH,"unix_socket_directories = '%s'\n",varGet(GVAR_SOCKPATH));
   fprintf(FH,"port=%ld\n",portnumber);
   fprintf(FH,"hba_file = '%s/pg_hba.conf'\n",pgdata);
   fprintf(FH,"ident_file = '%s/pg_ident.conf'\n",pgdata);
   fprintf(FH,"max_connections = 100\n");
   fprintf(FH,"shared_buffers = 128MB\n");
   fprintf(FH,"dynamic_shared_memory_type = posix\n");
   fprintf(FH,"wal_level = replica\n");
   fprintf(FH,"max_wal_size = 1GB\n");
   fprintf(FH,"min_wal_size = 80MB\n");
   fprintf(FH,"archive_mode=off\n");
   fprintf(FH,"archive_command = ''\n");
   fprintf(FH,"archive_timeout = 60\n");
   fprintf(FH,"logging_collector = on\n");
   fprintf(FH,"log_directory = 'log'\n");
   fprintf(FH,"log_filename = 'postgresql-%%d.log'\n");
   fprintf(FH,"log_file_mode = 0777\n");
   fprintf(FH,"log_truncate_on_rotation = on\n");
   fprintf(FH,"log_min_messages = warning\n");
   fprintf(FH,"log_min_error_statement = warning\n");
   fprintf(FH,"log_timezone = '%s'\n",varGet(SYSVAR_TIMEZONE));
   if (newname == NULL) { fprintf(FH,"cluster_name = 'R%ld'\n",portnumber); }
                   else { fprintf(FH,"cluster_name = '%s'\n",newname); }
   fprintf(FH,"datestyle = 'iso, mdy'\n");
   fclose(FH);
   free(FileName);
   TRACE ("ARGS(pgdata='%s' portnumber=%ld newname='%s') --> RETURN (none)\n",pgdata,portnumber,newname);
}

/**********************************************************************************/
/* Create signal file (above version 11)                                          */
/**********************************************************************************/
void pgCreateSignalRecoveryFile(int forStandby,const char *pgdata)
{
   char *signalFileName=malloc(1024);
   if (forStandby == true) { sprintf(signalFileName,"%s/standby.signal",pgdata); }
                      else { sprintf(signalFileName,"%s/recovery.signal",pgdata); };
   FILE *FH=fopen(signalFileName,"w");
   fprintf(FH,"Launch recovery...\n");
   fclose(FH);
   free(signalFileName);
   TRACE("ARGS(pgdata='%s') --> RETURN (none)\n",pgdata);
}


/**********************************************************************************/
/* Return, in Bytes a value specified like '10M'                                  */
/* Support 'G for GB, M for mega, K for KiloBytes, upper or lower case            */
/**********************************************************************************/
long getHumanSize(const char *inbuffer)
{
   char *wrkstr=malloc(100);
   char *res=wrkstr;
   char c='b'; 
   char *cpos=strchr(inbuffer,c);
   if (cpos == NULL) { c='M';cpos=strchr(inbuffer,c); };
   if (cpos == NULL) { c='g';cpos=strchr(inbuffer,c); };
   if (cpos == NULL) { c='G';cpos=strchr(inbuffer,c); };
   if (cpos == NULL) { c='k';cpos=strchr(inbuffer,c); };
   if (cpos == NULL) { c='K';cpos=strchr(inbuffer,c); };
   if (cpos == NULL) { c='b'; }; // bytes
//   printf("getsize(%s) : c='%c' s='%s'\n",inbuffer,c,cpos);
   cpos=(char *)inbuffer;
   while (cpos[0] != 0x00)
   {
      if (strchr("0123456789",cpos[0]) != NULL) { res[0]=cpos[0]; res++; };
//      printf("getsize(%s) : res='%s'\n",inbuffer,wrkstr);
      cpos++;
   };
//   printf("res=%s\n",wrkstr);
   res[0]=cpos[0]; // Take the terminator string
   long result;
   result=atol(wrkstr);
   switch (c)
   {
      case 'G' : ;
      case 'g' : result=result*1024;
      case 'M' : ;
      case 'm' : result=result*1024;
      case 'K' : ;
      case 'k' : result=result*1024;
      default : ; // Byte (value as it)
   };
   TRACE ("ARGS(inbuffer='%s') --> RETURN (long)%ld\n",inbuffer,result);
   free(wrkstr);
   return(result);
}


/**********************************************************************************/
/* Extract the file name from a full name                                         */
/**********************************************************************************/
char *extractFileName(const char *filename)
{
   char *filenameOnly=(char *)filename;
   char *lastslash=strchr(filename,'/');
   if (lastslash==NULL) return((char *)filename);
   while (lastslash != NULL)
   {
      lastslash++;
      filenameOnly=lastslash;
      lastslash=strchr(lastslash,'/');
   }
   return(filenameOnly);
}

/**********************************************************************************/
/* Extract the path from a full name                                              */
/**********************************************************************************/
char *extractFilePath(char *filename)
{
   static char *filepath=NULL;
   char *filepathOnly=filename;
   if (filepath!=NULL) free(filepath);
   filepath=strdup(filename);
   char *lastslash=strchr(filepath,'/');
   while (lastslash != NULL)
   {
      filepathOnly=lastslash;
      lastslash++;
      lastslash=strchr(lastslash,'/');
   }
   if (filepathOnly != NULL) filepathOnly[0]=0x00;
   return(filepath);
}


/********************************************************************************/
/* Search for a string in a comma separated list                                */
/********************************************************************************/
int findStringInList(const char *rootdir,char *list,const char* item)
{
   char *fullName=malloc(2048);
   strcpy(fullName,",");
   strcat(fullName,rootdir); 
   strcat(fullName,"/"); 
   strcat(fullName,item);
   int fnd=0;
   if (strstr(list,fullName) != NULL) { fnd++; }
   free(fullName);
   TRACE ("ARGS(rootdir='%s' list='%s' item='%s') --> RETURN (int) %d\n",rootdir,list,item,fnd);
   return(fnd);
}


/**********************************************************************************/
/* Convert a string to Upper case                                                 */
/**********************************************************************************/
void strToUpper(char *outbuffer,const char *inbuffer)
{
   int i=0;
   strcpy(outbuffer,inbuffer);
   char *buf=outbuffer;
   while (i < strlen(inbuffer))
   {
      buf[0]=(unsigned char)toupper((unsigned char)buf[0]);
      buf++;
      i++;
   };
}

/**********************************************************************************/
/* Convert a string to Lower case                                                 */
/**********************************************************************************/
void strToLower(char *inbuffer)
{
   int i=0;
   char *buf=inbuffer;
   while (i < strlen(inbuffer))
   {
      buf[0]=(unsigned char)tolower((unsigned char)buf[0]);
      buf++;
      i++;
   };
//   TRACE("outbuffer=%s\n",inbuffer);
}

/**********************************************************************************/
/* Return current login username                                                  */
/**********************************************************************************/
const char *getUserName()
{
  uid_t uid = geteuid();
  struct passwd *pw = getpwuid(uid);
  if (pw)
  {
    return pw->pw_name;
  }
  return "";
}

/**********************************************************************************/
/* Return left part of the retention                                              */
/* Retentionis composed into two keywords  '<kind>:<count>'                       */
/* where:                                                                         */
/*   <kind> can be :                                                              */
/*       'count' : Keep <count> backups available                                 */
/*       'days'  : keep all backups not older than <count> days.                  */
/*   <count> is the number                                                        */
/* Example:                                                                       */
/*    'count:7' when doing one FULL per day, you keep backups of a whole wheek    */
/*    'days:7'  Keep backups not older than 7 days. Don't take care of the number */
/*              of backups you do every day                                       */ 
/**********************************************************************************/
char *retentionModeString(char *pbuffer)
{
   static char *buffer=NULL;
   if (buffer == NULL) buffer=malloc(20);
   strToUpper(buffer,pbuffer);
   char *mode=buffer;
   char *count=strchr(buffer,':');
   count[0]=0x00;
   TRACE("ARGS(puffer='%s') --> RETURN (char *) '%s'\n",pbuffer,mode);
   return(mode);
}

/**********************************************************************************/
// Extact path from archive_command
// Syntax is always : 'cp %p /my/directory/%f'
//               or 'test ! -f /dir/%f && cp -p %p /dir/%f'
/**********************************************************************************/
char *pgExtractWalDestination(const char *pbuffer)
{
   static char *buffer=NULL;
   if (buffer== NULL) buffer=(char *)malloc(1024);
   strcpy(buffer,pbuffer);
   char *p=strstr(buffer,"%p");
   char *f=strstr(p,"%f");
   if (p == NULL) return (NULL);
   if (f == NULL) return (NULL);
   p+=2; // skip '%p'
   while (p[0]==' ' || p[0] == '\t') { p++; };
   f--;
   f[0]=0x00;
   TRACE("ARGS(puffer='%s') --> RETURN (char *) '%s'\n",pbuffer,p);
   return(p);
}

/**********************************************************************************/
/* Accept only 'count|days'                                                       */
/**********************************************************************************/
int retentionMode(char *pbuffer)
{
   char *buffer=strdup(pbuffer);
   char *mode=buffer;
   char *count=strchr(buffer,':');
   count[0]=0x00;
   TRACE("mode='%s'\n",mode);
   if (strcmp(mode,"count")==0) { free(buffer); TRACE("ARGS(pbuffer='%s') --> RETURN (int) RETENTION_MODE_COUNT\n",pbuffer);return (RETENTION_MODE_COUNT); };
   if (strcmp(mode,"days")==0)  { free(buffer); TRACE("ARGS(pbuffer='%s') --> RETURN (int) RETENTION_MODE_DAYS\n",pbuffer); return (RETENTION_MODE_DAYS);  };
   free(buffer);
   TRACE("ARGS(pbuffer='%s') --> RETURN (int) RETENTION_MODE_COUNT\n",pbuffer);
   return(RETENTION_MODE_COUNT);
}


/**********************************************************************************/
/* Variable table                                                                 */
/**********************************************************************************/
typedef struct Variable Variable;
struct Variable
{
   Variable *prv;
   Variable *nxt;
   char *nam;
   char *val;
};


static Variable *varFirst;
static Variable *varLast;

void varInternalAdd(const char *pnam,const char *pval)
{
//   TRACE("ARGS: pnam='%s' pval='%s'\n",pnam,pval);
   Variable *wrk=malloc(sizeof(Variable)+1);
   wrk->nam=strdup(pnam);
   wrk->val=strdup(pval);
   if (varFirst == NULL)
   {
//      TRACE("Added first item\n",pnam,pval);
      wrk->prv=NULL;
      wrk->nxt=NULL;
      varFirst=wrk;
      varLast=wrk;
      return;
   };
//   TRACE("Added item '%s'='%s'\n",pnam,pval);
   wrk->prv=varLast;
   varLast->nxt=wrk;
   wrk->nxt=NULL;
   varLast=wrk;
}

int varExist(const char *pnam)
{
   Variable *wrk=varFirst;
   while (wrk != NULL && strcmp(wrk->nam,pnam) != 0) { wrk=wrk->nxt; };
   if (wrk == NULL) return(false);
   if (strcmp(wrk->val,"(null)") == 0) return(false);
   if (strcmp(wrk->val,"(NULL)") == 0) return(false);
   return(true);
}

char *varGet(const char *pnam)
{
   Variable *wrk=varFirst;
   while (wrk != NULL && strcmp(wrk->nam,pnam) != 0) { wrk=wrk->nxt; };
   //printf("varGet name=%s : %ld\n",pnam,(long)wrk);
   if (wrk == NULL) { TRACE("varGet('%s') is NULL\n",pnam);return(NULL);};
   return(wrk->val);
}

// Double quotes if required
char *asLiteral(const char *buffer)
{
   static char *result=NULL;
   if (result == NULL) result=malloc(1024);
   result[0]=0x00;
   char *dst=result;
   char *src=(char *)buffer;
   while (src[0] != 0x00)
   {
      if (src[0]=='\'') {dst[0]=src[0];dst++; };
      dst[0]=src[0];
      src++;
      dst++;
   };
   dst[0]=0x00;
   return(result);
}


long varGetLong(const char *pnam)
{
   Variable *wrk=varFirst;
   while (wrk != NULL && strcmp(wrk->nam,pnam) != 0) { wrk=wrk->nxt; };
   
   if (wrk == NULL) return(0);
   long result=atol(wrk->val);
   TRACE("ARGS(name=%s) --> RETURN (long) %ld\n",pnam,result);
   return(result);
}

int varGetInt(const char *pnam)
{
   Variable *wrk=varFirst;
   while (wrk != NULL && strcmp(wrk->nam,pnam) != 0) { wrk=wrk->nxt; };
   //printf("varGet name=%s : %ld\n",pnam,(long)wrk);
   if (wrk == NULL) return(0);
   int result=atoi(wrk->val);
   TRACE("ARGS(name=%s) --> RETURN (int) %d\n",pnam,result);
   return(result);
}

void qualifierSET(const char *pname,const char *pvalue) { varAdd(pname,pvalue); }
void qualifierUNSET(const char *pname) { varAdd(pname,QAL_UNSET); }

int qualifierIsUNSET(const char *pnam) 
{
   Variable *wrk=varFirst;
   while (wrk != NULL && strcmp(wrk->nam,pnam) != 0) { wrk=wrk->nxt; };
   //printf("varGet name=%s : %ld\n",pnam,(long)wrk);
   if (wrk == NULL) return(false);
   if (strcmp(QAL_UNSET,wrk->val)== 0) return(true);
   return(false);
}

// an option is an argument without value
void SETOption(const char *pnam){ varAdd(pnam,OPTION_SET_VALUE); }
void UNSETOption(const char *pnam) { varAdd(pnam,OPTION_UNSET_VALUE); }

int CLUSTER_Prm_IsSet(const char *pnam)
{
   Variable *wrk=varFirst;
   while (wrk != NULL && strcmp(wrk->nam,pnam) != 0) { wrk=wrk->nxt; };
   if (wrk == NULL) return(false);
   if (strcmp(OPTION_SET_VALUE,wrk->val)== 0) return(true);
   return(false);
}

int optionIsSET(const char *pnam)
{
   Variable *wrk=varFirst;
   while (wrk != NULL && strcmp(wrk->nam,pnam) != 0) { wrk=wrk->nxt; };
   if (wrk == NULL) return(false);
   if (strcmp(OPTION_SET_VALUE,wrk->val)== 0) return(true);
   return(false);
}

int optionIsUNSET(const char *pnam)
{
   Variable *wrk=varFirst;
   while (wrk != NULL && strcmp(wrk->nam,pnam) != 0) { wrk=wrk->nxt; };
   if (wrk == NULL) return(false);
   if (strcmp(OPTION_UNSET_VALUE,wrk->val)== 0) return(true);
   return(false);
}

/* varRESET clear content of variable like 'undefined' */

void varRESET(const char *pnam)
{
   varAdd(pnam,VAR_UNSET_VALUE);
}

int varIsSET(const char *pnam)
{
   Variable *wrk=varFirst;
   while (wrk != NULL && strcmp(wrk->nam,pnam) != 0) { wrk=wrk->nxt; };
   if (wrk == NULL)                          { TRACE("ARGS(pnam='%s') --> RETURN (int) false[NULL]\n",pnam); return(false);};
   if (strcmp(VAR_UNSET_VALUE,wrk->val)== 0) { TRACE("ARGS(pnam='%s') --> RETURN (int) false\n",pnam);       return(false);};
   TRACE("ARGS(pnam='%s') --> RETURN (int) true\n",pnam);
   return(true);
}

int varIsUNSET(const char *pnam)
{
   Variable *wrk=varFirst;
   while (wrk != NULL && strcmp(wrk->nam,pnam) != 0) { wrk=wrk->nxt; };
   if (wrk == NULL)                          { TRACE("ARGS(pnam='%s') --> RETURN (int) false[NULL]\n",pnam); return(false);};
   if (strcmp(VAR_UNSET_VALUE,wrk->val)== 0) { TRACE("ARGS(pnam='%s') --> RETURN (int) true\n",pnam);        return(true);};
   TRACE("ARGS(pnam='%s') --> RETURN (int) false\n",pnam);
   return(false);
}

/**********************************************************************************/
/* YES|NO                                                                         */
/* accept all values like 'yes|no' 'on|off' '1|0'                                 */
/**********************************************************************************/
#define YESNO_BAD 2
#define YESNO_YES true
#define YESNO_NO  false

int isYESNOvalid(const char *value)
{
   char *response=malloc(128);
   sprintf(response,",%s,",value);
   if (strstr(",yes,oui,on,1,ya,y,YES,OUI,ON,1,YA,y,",response) != NULL) return(YESNO_YES);
   if (strstr(",no,non,off,0,ne,n,NO,NON,OFF,0,NE,N,",response) != NULL) return(YESNO_NO);
   free(response);
   return(YESNO_BAD);
}


int varIsTRUE(const char *pnam)
{
   Variable *wrk=varFirst;
   while (wrk != NULL && strcmp(wrk->nam,pnam) != 0) { wrk=wrk->nxt; };
   if (wrk == NULL) return(false);
   if (isYESNOvalid(wrk->val) == YESNO_YES) return(true);
   return(false);
}

void varDel(const char *pnam)
{
   Variable *wrk=varFirst;
   while (wrk != NULL && strcmp(wrk->nam,pnam) != 0) { wrk=wrk->nxt;};
   if (wrk == NULL) return;
   Variable *prv=wrk->prv;
   Variable *nxt=wrk->nxt;
   if (nxt != NULL && prv != NULL)
   {
      prv->nxt=nxt;
      nxt->prv=prv;
      free(wrk->nam);
      free(wrk->val);
      free(wrk);
   }
   else
   {
      if (prv==NULL && nxt==NULL)
      {
         varFirst=NULL;
         varLast=NULL;
         free(wrk->nam);
         free(wrk->val);
         free(wrk);
         return;
      };
      if (prv==NULL) // We are on the first item
      {
         nxt->prv=NULL;
         varFirst=nxt;
         free(wrk->nam);
         free(wrk->val);
         free(wrk);
         return;
      }
      if (nxt==NULL)
      {
         prv->nxt=NULL;
         varLast=prv;
         free(wrk->nam);
         free(wrk->val);
         free(wrk);
         return;
      }
   }
}

void varAdd(const char *pnam,const char *pval)
{
//   TRACE("ARGS: pnam='%s' pval='%s'\n",pnam,pval);
   if (pval == NULL) return;
   Variable *wrk=varFirst;
   int fnd=0;
   while (fnd==0 && wrk != NULL && strcmp(wrk->nam,pnam) != 0) { wrk=wrk->nxt;};

   if (wrk == NULL) 
   { 
      varInternalAdd(pnam,pval); 
   }
   else
   { 
//      TRACE("update\n");
      if (wrk->val == NULL) { wrk->val=strdup(pval); }
                       else { if (strcmp(wrk->val,pval) != 0)
                              {
                                 free(wrk->val);
                                 wrk->val=strdup(pval);
                              };
                            };
   };
}

void varAddLong(const char *pnam,long pval)
{
   char value[20];
   sprintf(&value[0],"%ld",pval);
   varAdd(pnam,&value[0]);
}

void varAddInt(const char *pnam,int pval)
{
   char value[20];
   sprintf(&value[0],"%d",pval);
   varAdd(pnam,&value[0]);
}

void varDump(int including_system)
{
   Variable *wrk=varFirst;
   while (wrk != NULL)
   {
      if (including_system == 0)
      {
         if (wrk->nam[0] != '@') printf("\t %-20s='%s'\n",wrk->nam,wrk->val);
      }
      else
      {
         printf("\t %-20s='%s'\n",wrk->nam,wrk->val);
      };
      wrk=wrk->nxt;
   }
}

void CLUqueryEnd();
char *CLUgetString(int row,int col);
long CLUgetLong(int row,int col);
int CLUgetInt(int row,int col);
int CLUquery(const char *query,int with_retry);

int DEPOquery(const char *query,int with_retry);
void DEPOqueryEnd();
int DEPOgetLength(int row,int col);
char *DEPOgetString(int row,int col);
long DEPOgetLong(int row,int col);
PGresult *DEPOgetRes();


void CLUconfigReset(const char *varnam)
{
   varDel(varnam);
}

void CLUloadData();
void SaveConfigFile(const char *prefix, const char *sectionName,int kind);
// allowed format :
// %Y year
// %m month
// %d day
// %H:%M:%S
char *date_system(const char *format)
{
   static char outstr[200];
   time_t t;
   struct tm *tmp;
   t = time(NULL);
   tmp = localtime(&t);
   strftime(outstr, sizeof(outstr), format, tmp);
   return(outstr);
}

char *displayTime(struct timeval *tv)
{
   static char outstr[200];
   time_t t;
   struct tm *tmp;
   t = tv->tv_sec;
   tmp = localtime(&t);
   strftime(outstr, sizeof(outstr), "%Y-%m-%d %H:%M:%S", tmp);
   return(outstr);
}


#define PGPRIV_SUPERUSER   1
#define PGPRIV_REPLICATION 2
#define PGPRIV_BYPASSRLS   3

int CLUcheckPrivilege(int priv)
{
  char *spriv=malloc(20);
  int  colpriv=0;
  switch(priv) 
  {
    case PGPRIV_SUPERUSER   : strcpy(spriv,"SUPERUSER");   colpriv=1; break;
    case PGPRIV_REPLICATION : strcpy(spriv,"REPLICATION"); colpriv=2; break;
    case PGPRIV_BYPASSRLS   : strcpy(spriv,"BYPASS_RLS");  colpriv=3; break;
  };
  int rc=CLUquery("select rolname,rolsuper,rolreplication,rolbypassrls from pg_roles where rolname=current_user",0);
  if (rc == 1 && strcmp(CLUgetString(0,colpriv),"t")  == 0)
  {
     rc=true;
  }
  else
  {
     ERROR(ERR_MISPRIV,"User '%s' must have '%s' privilege.\n",CLUgetString(0,0),spriv);
     rc=false;
  }
  free(spriv);
  TRACE("ARGS(priv=%d) [ colpriv=%d result='%s'] --> RETURN (int) %d\n",priv,colpriv,CLUgetString(0,colpriv),rc);
  CLUqueryEnd();
  return(rc); 
}

/********************************************************************************/
/* translate variabbles to destination buffer                                   */
/* Varable are identified by '{name}'                                           */
/********************************************************************************/
void varTranslate(char *outbuffer,const char *inbuffer,int recursive)
{
   if (recursive > 3)
   {
      strcpy(outbuffer,inbuffer);
      TRACE("ARGS(outbuffer='%s' inbuffer='%s' recursive=%d) More than 3 recursive call\n",outbuffer,inbuffer,recursive);
      return;
   };
   char *tmpbuff=malloc(1024);
   char *wrkbuff=malloc(1024);
   char *tmpvar=NULL;
   tmpvar=date_system("%Y");varAdd("YYYY",tmpvar);
   tmpvar=date_system("%y");varAdd("YY",tmpvar);
   tmpvar=date_system("%m");varAdd("MM",tmpvar);
   tmpvar=date_system("%d");varAdd("DD",tmpvar);
   tmpvar=date_system("%H");varAdd("HH",tmpvar);
   tmpvar=date_system("%M");varAdd("MI",tmpvar);
   tmpvar=date_system("%S");varAdd("SS",tmpvar);
   tmpvar=malloc(1024);
   strcpy(tmpbuff,inbuffer);
   Variable *wrk=varFirst;
   while (wrk != NULL)
   {
      strcpy(tmpvar,"{");strcat(tmpvar,wrk->nam);strcat(tmpvar,"}");
      char *position=strstr(tmpbuff,tmpvar);
      while (position != NULL)
      {
         strncpy(wrkbuff,tmpbuff,position-tmpbuff);
         char *ew=wrkbuff;ew+=(position-tmpbuff); ew[0]=0x00;
         char *eposition=strstr(position,"}");eposition++;
         strcat(wrkbuff,wrk->val);
         strcat(wrkbuff,eposition);
         wcscpy( (wchar_t *)wrkbuff,(wchar_t *)wrkbuff);
         strcpy(tmpbuff,wrkbuff);
         position=strstr(tmpbuff,tmpvar);
      }
      strcpy(tmpvar,"{");strcat(tmpvar,wrk->nam);strcat(tmpvar,"/u}");
      position=strstr(tmpbuff,tmpvar);
      while (position != NULL)
      {
         strcpy(wrkbuff,tmpbuff);
         char * wposition=strstr(wrkbuff,tmpvar);
         wposition[0]=0x00;
         char *eposition=strstr(position,"}");eposition++;
         char *tu=malloc(strlen(wrk->val)+2);strToUpper(tu,wrk->val);
         strcat(wrkbuff,tu);
         free(tu);
         if (strlen(eposition) > 0) strcat(wrkbuff,eposition);
         strcpy(tmpbuff,wrkbuff);
         position=strstr(tmpbuff,tmpvar);
      }
      strcpy(tmpvar,"{");strcat(tmpvar,wrk->nam);strcat(tmpvar,"/l}");
      position=strstr(tmpbuff,tmpvar);
      while (position != NULL)
      {
         strcpy(wrkbuff,tmpbuff);
         char * wposition=strstr(wrkbuff,tmpvar);
         wposition[0]=0x00;
         char *eposition=strstr(position,"}");eposition++;
         char *tu=strdup(wrk->val);strToLower(tu);
         strcat(wrkbuff,tu);
         free(tu);
         strcat(wrkbuff,eposition);
         strcpy(tmpbuff,wrkbuff);
         position=strstr(tmpbuff,tmpvar);
      }
      wrk=wrk->nxt;
   };
   char *verify=strstr(tmpbuff,"}");
   if (recursive < 3 && verify != NULL) { varTranslate(tmpbuff,tmpbuff,recursive+1); };
   strcpy(outbuffer,tmpbuff);
   //TRACE("--> RETURN (string) '%s'\n",outbuffer);
}


#define ARRAY_NO_ORDER 0
#define ARRAY_UP_ORDER 1
#define ARRAY_DOWN_ORDER 2


//********************************************************************************
// Create new ARRAY
//********************************************************************************
Array* arrayCreate(int sort)
{
   Array *result=malloc(sizeof(Array));
   result->first=NULL;
   result->last=NULL;
   result->current=NULL;
   result->count=0;
   result->sorted=sort;
   return(result);
}

//********************************************************************************
// Find first item in ARRAY
//********************************************************************************
* arrayFind(Array *array,const char * key)
{
   array->current=array->first;
   while (array->current != NULL && strcmp(array->current->key,key) < 0) { array->current=array->current->nxt; };
   return(array->current);
}

//********************************************************************************
// get next item in ARRAY (after arrayFind)
//********************************************************************************
ArrayItem* arrayNext(Array *array)
{
   if (array->current == NULL) return(NULL);
//   printf("ARRAYNEXT:%s",array->current->key);
   array->current=array->current->nxt;
   return(array->current);
}

//********************************************************************************
// get previous item in ARRAY (after arrayFind)
//********************************************************************************
ArrayItem* arrayPrevious(Array *array)
{
   if (array->current == NULL) return(NULL);
   array->current=array->current->prv;
   return(array->current);
}

ArrayItem* arrayFirst(Array *array) { array->current=array->first;return(array->current); }
ArrayItem* arrayLast (Array *array) { array->current=array->last; return(array->current); }

//********************************************************************************
// get current item in ARRAY (after arrayFind)
//********************************************************************************
ArrayItem* arrayCurrent(Array *array){ return(array->current); }

void arraySetCurrent(Array *array,ArrayItem*item)
{
   array->current=item;
}

//********************************************************************************
// Extract item from ARRAY
//********************************************************************************
int arrayDetach(Array *array,ArrayItem* item)
{
   if (item == NULL) return(0);
   ArrayItem* wNXT=item->nxt;
   ArrayItem* wPRV=item->prv;
  
   if (array->last == item) // this is the LAST item
   {
      if (array->first == array->last) // There was only 1 item ?
      {
         array->first=NULL;
         array->last=NULL;
      }
      else
      {
         array->last=wPRV;
         if (wPRV != NULL) wPRV->nxt=NULL;
      };
      array->count--;
      return(0);
   };
   if (array->first == item) // this is the LAST item
   {
      if (array->first == array->last) // There was only 1 item ?
      {
         array->first=NULL;
         array->last=NULL;
      }
      else
      {
         array->first=wNXT;
         if (wNXT != NULL) wNXT->prv=NULL;
      };
      array->count--;
      return(0);
   };

   if (wPRV != NULL) wPRV->nxt=wNXT;
   if (wNXT != NULL) wNXT->prv=wPRV;
   if (wNXT == NULL) array->last=wPRV;
   if (wPRV == NULL) array->first=wNXT;
   array->count--;
   return(0);
}

void arrayDelete(Array *array,ArrayItem* item)
{
    int n=arrayDetach(array,item);
    if (n != 0) return;
    if (item->key != NULL) free(item->key);
    if (item->data!= NULL) free(item->data);
    free(item);
}

void arrayClear(Array *array)
{
    ArrayItem *d=arrayFirst(array);
    while (d != NULL)
    {
        arrayDelete(array,d);
        d=arrayFirst(array);
    };
}


int arrayCount(Array *array)
{
   return(array->count);
}

//********************************************************************************
// Add new item in ARRAY
//********************************************************************************
void arrayAdd(Array *array,ArrayItem* item)
{
   if (array->first == NULL)
   {
      item->nxt=NULL,
      item->prv=NULL;
      array->first=item;
      array->last=item;
      array->count++;
   }
   else
   {
      ArrayItem *wrk;
      switch(array->sorted)
      {
      case ARRAY_NO_ORDER : // NOT sorted
          array->last->nxt=item;
          item->prv=array->last;
          item->nxt=NULL;
          //item->key=NULL;                                   // NO KEY defined in this mode
          array->last=item;
          break;
      default :
          wrk=array->first;
          if (array->sorted == ARRAY_UP_ORDER) { while (wrk != NULL && strcmp(item->key,wrk->key) > 0) {wrk=wrk->nxt;};}
                                         else  { while (wrk != NULL && strcmp(item->key,wrk->key) < 0) {wrk=wrk->nxt;};};
          if (wrk== NULL) // Add item to the end of the array
          {
             item->prv=array->last;
             array->last->nxt=item;
             array->last=item;
             item->nxt=NULL;
          }
          else            // Insert item at current position of the array
          {
              if (wrk == array->first) { item->nxt=array->first;
                                         array->first->prv=item;
                                         item->prv=NULL;
                                         array->first=item;
                                       }
                                  else {
                                         item->nxt=wrk;
                                         item->prv=wrk->prv;
                                         wrk->prv=item;
                                         if (item->prv != NULL) item->prv->nxt=item;
                                       };
          };
          break;
     };
     array->count++;
   };
}

void arrayAddNoKey(Array *array,void *data)
{
   ArrayItem* item=malloc(sizeof(ArrayItem));
   item->key=NULL;
   item->data=data;
   arrayAdd(array,item);
   array->sorted=ARRAY_NO_ORDER;
}

void arrayAddKey(Array *array,const char*key,void *data)
{
   ArrayItem* item=malloc(sizeof(ArrayItem));
   item->key=strdup(key);
   item->data=data;
   arrayAdd(array,item);
}

void arrayPush(Array *array,void *data)
{
    ArrayItem* item=malloc(sizeof(ArrayItem));
    item->key=NULL;
    item->data=data;
    ArrayItem* wrk=array->first;
    if (wrk== NULL) // Add item to the end of the array
    {
       array->first=item;
       array->last=item;
       item->nxt=NULL;
       item->prv=NULL;
    }
    else            // Insert item at current position of the array
    {
       item->nxt=array->first;
       array->first->prv=item;
       item->prv=NULL;
       array->first=item;
    }
}

ArrayItem *arrayPop(Array *array)
{
   ArrayItem* wrk=array->first;
   arrayDetach(array,wrk);
   return(wrk);
}

long hex2int(const char *hex)
{
    unsigned long val = 0;
    char *a=(char *)hex;
    a+=(strlen(hex)-1);
    int unit=1;
    while (a != (char *)hex)
    {
       if(a[0] <= 57) val += (a[0]-48)*unit;
                 else val += (a[0]-55)*unit;
       a--;
       unit=unit*16;
    }
    return val;
}

/********************************************************************************/
/* Create UNIQUE IDENTIFIER.  accessible by variable name 'uid'                 */
/********************************************************************************/
void createUID()
{
   struct timespec ts;
   clock_gettime(CLOCK_REALTIME, &ts);
   char *uid_string=malloc(30);
   sprintf(uid_string,"%04lx%08lx%08lx",varGetLong(GVAR_CLUCID), ts.tv_sec,ts.tv_nsec);
   varAdd(RECM_BCKID,uid_string);
   free(uid_string);
}

#define PGCTL_ACTION_START 1
#define PGCTL_ACTION_STOP  2
#define PGCTL_NO_OUTPUT    0
#define PGCTL_OUTPUT       1
#define ERROR_PGCTL_FATAL 16384

int shell(const char *command_line,int display_log)
{
   TRACE("ARGS(command_line='%s' display_log=%d)\n",command_line,display_log);
   char buf[1024];
   FILE *fp;
   if ((fp = popen(command_line, "r")) == NULL) { return (ERR_SHELLERROR); };
   buf[0]=0x00;
   int stop=0;
   while (stop == 0 && fgets(buf, 1024, fp) != NULL) 
   {
      TRACE("SHELL:%s",buf);
      if (display_log != 0) INFO("%s\n",buf);
    }
   pclose(fp);
   TRACE("--> RETURN (int) SUCCESS\n");
   return ERR_SUCCESS;
}

int pg_ctl(int action,const char *pgctl,int display_log)
{
   int rc=0;
   char buf[1024];
   FILE *fp;
   char *cmd=malloc(1024);
   if (action == PGCTL_ACTION_STOP) { sprintf(cmd,"%s stop -D %s\n",pgctl,varGet(GVAR_CLUPGDATA)); }
                               else { sprintf(cmd,"%s start -D %s\n",pgctl,varGet(GVAR_CLUPGDATA)); }
   if ((fp = popen(cmd, "r")) == NULL) 
   {
      TRACE("ARGS(action=%d pgctl='%s' display_log=%d) [cmd='%s'] --> RETURN (int) 1\n",action,pgctl,display_log,cmd);
      free(cmd); 
      return 1;
   };
   buf[0]=0x00;
   int stop=0;
   while (stop == 0 && fgets(buf, 1024, fp) != NULL) 
   {
      TRACE("PG_CTL:%s",buf);
      if (display_log != 0) printf("%s\n",buf);
      char *msg=strstr(buf,"] FATAL:");
      if (msg != NULL )
      {
         msg+=2;
         if (last_pg_ctl_message != NULL) free(last_pg_ctl_message);
         last_pg_ctl_message=strdup(msg);
         rc=ERROR_PGCTL_FATAL;
      };
      if (action == PGCTL_ACTION_STOP  && strstr(buf,"server stopped") != NULL) stop++;
      if (action == PGCTL_ACTION_START && strstr(buf,"server started") != NULL) stop++;
   }
   if (pclose(fp)) { free(cmd); return 1; };
   TRACE("ARGS(action=%d pgctl='%s' display_log=%d) [cmd='%s']--> RETURN (int) %d\n",action,pgctl,display_log,cmd,rc);
   free(cmd);
   return rc;
}


// Find pg_ctl program
char *pg_ctl_locate()
{
   
   VERBOSE("Searching for 'pg_ctl'\n");
   static char *buf=NULL;
   if (buf == NULL) buf=malloc(1024);
   FILE *fp;
   if ((fp = popen("find / -name pg_ctl 2>/dev/null|head -1", "r")) == NULL)
   {
      TRACE("ARGS(none) --> RETURN (char *) NULL\n");
      return NULL;
   }
   buf[0]=0x00;
   int stop=0;
   while (stop == 0 && fgets(buf, 1024, fp) != NULL) 
   {
      TRACE("buf=%s\n",buf);
      if (strstr(buf,"/pg_ctl") != NULL) stop++; 
   }
   if (pclose(fp)) return NULL;
   char *x=strchr(buf,'\n');
   if (x != NULL) x[0]=0x00;
   if (strlen(buf) == 0) return NULL;
   TRACE("ARGS(none) --> RETURN (char *) %s\n",buf);
   return(buf);
}

//********************************************************************************
// Above version 11,  we use 'postgresql.auto.conf' file
//********************************************************************************
void pgAddRecoveryParameters(const char *directory,const char *WALdir,const char *methode,const char *restore_date,int WalIsCompressed,int isInclusive,int isPaused)
{
   TRACE("ARGS(directory='%s' WALdir='%s' methode='%s' restore_date='%s')\n",directory,WALdir,methode,restore_date);   
   char *autoFileName=malloc(1024);
   char *backupName=malloc(1024);
   sprintf(autoFileName,"%s/postgresql.auto.conf",directory);
   strcpy(backupName,autoFileName);
   strcat(backupName,".old");
   if (file_exists(backupName)==true) unlink(backupName);
   if (file_exists(autoFileName)==true) rename(autoFileName,backupName);
   FILE *fW=fopen(autoFileName,"w");
   FILE *fR=fopen(backupName,"r");
   if (fR != NULL)
   {
      char *buffer=malloc(2048);
      while (fgets(buffer, 2040, fR) != NULL)
      {
         int remove_line=0;
         if (strstr(buffer,"recovery_target_time")      == buffer) remove_line++;
         if (strstr(buffer,"restore_point_name")        == buffer) remove_line++;
         if (strstr(buffer,"recovery_target_timeline")  == buffer) remove_line++;
         if (strstr(buffer,"recovery_target_inclusive") == buffer) remove_line++;
         if (strstr(buffer,methode)                     == buffer) remove_line++;
         if (remove_line==0) fprintf(fW,"%s",buffer);
      }
      fclose(fR);
      free(buffer);
   };
   char* RECM_RESTORE_COMMAND=getenv (ENVIRONMENT_RECM_RESTORE_COMMAND);
   if (RECM_RESTORE_COMMAND != NULL) 
   {
      fprintf(fW,"restore_command='%s'\n",RECM_RESTORE_COMMAND);
   }
   else
   {
      if (WalIsCompressed == true) 
      { 
         fprintf(fW,"restore_command='[ -r %s/%s.gz ] && gunzip -c %s/%s.gz > %s || cp %s/%s %s'\n",
                 WALdir,"%f",
                 WALdir,"%f","%p",
                 WALdir,"%f","%p");
      }
      else
      {
         fprintf(fW,"restore_command='cp %s/%s %s'\n",WALdir,"%f","%p"); 
         TRACE("restore_command='cp %s/%s %s'\n",WALdir,"%f","%p");
      };
   };
   fprintf(fW,"%s='%s'\n",methode,restore_date);
   TRACE("%s='%s'\n",methode,restore_date);
   if (isPaused == true) { fprintf(fW,"recovery_target_action = 'pause'\n"); }
                    else { fprintf(fW,"recovery_target_action = 'promote'\n"); };
   if (isInclusive == true) { fprintf(fW,"recovery_target_inclusive = true\n"); }
                       else { fprintf(fW,"recovery_target_inclusive = false\n"); }
   fclose(fW);

   free(backupName);
   free(autoFileName);
}

//********************************************************************************
// Above version 11,  we use 'postgresql.auto.conf' file
//********************************************************************************
char *autocompleteUID(int cluster_id,const char *uid)
{
   static char result[128];
   char query[255];
   sprintf(query,"select count(*) from %s.backups where cid=%d and bck_id like '%%%s'",varGet(GVAR_DEPUSER),cluster_id,uid);
   int rows=DEPOquery(query,0);
   rows=DEPOgetInt(0,0);
   DEPOqueryEnd();
   if (rows == 1) { sprintf(query,"select bck_id from %s.backups where cid=%d and bck_id like '%%%s'",varGet(GVAR_DEPUSER),cluster_id,uid);
                    rows=DEPOquery(query,0);
                    strcpy(result,DEPOgetString(0,0));
                    DEPOqueryEnd();
                  }
             else { sprintf(query,"select array_to_string(array_agg(bck_id),',') from %s.backups where cid=%d and bck_id like '%%%s'",varGet(GVAR_DEPUSER),cluster_id,uid);
                    rows=DEPOquery(query,0);
                    INFO("Non unique UID. Found '%s'\n",DEPOgetString(0,0));
                    DEPOqueryEnd();
                    return(NULL);
                  };
   return(&result[0]);
}

//********************************************************************************
// Above version 11,  we use 'postgresql.auto.conf' file
//********************************************************************************
void pgAddRecoveryParametersForStandby(int cluster_id,const char *directory,const char *WALdir,const char *methode,const char *restore_date,int WalIsCompressed)
{
   TRACE("ARGS(directory='%s' WALdir='%s' methode='%s' restore_date='%s')\n",directory,WALdir,methode,restore_date);   
   char *autoFileName=malloc(1024);
   char *backupName=malloc(1024);
   sprintf(autoFileName,"%s/postgresql.auto.conf",directory);
   strcpy(backupName,autoFileName);
   strcat(backupName,".old");
   if (file_exists(backupName)==true) unlink(backupName);
   if (file_exists(autoFileName)==true) rename(autoFileName,backupName);
   FILE *fW=fopen(autoFileName,"w");
   FILE *fR=fopen(backupName,"r");
   if (fR != NULL)
   {
      char *buffer=malloc(2048);
      while (fgets(buffer, 2040, fR) != NULL)
      {
         int remove_line=0;
         if (strstr(buffer,"recovery_target_time")     == buffer) remove_line++;
         if (strstr(buffer,"restore_point_name")       == buffer) remove_line++;
         if (strstr(buffer,"recovery_target_timeline") == buffer) remove_line++;
         if (strstr(buffer,methode)                    == buffer) remove_line++;
         if (remove_line==0) fprintf(fW,"%s",buffer);
      }
      fclose(fR);
      free(buffer);
   };

   //fprintf(fW,"%s='%s'\n",methode,restore_date);
   char *query=malloc(1024);
   sprintf(query,"select rep_usr,rep_pwd,clu_addr,clu_port,coalesce(rep_opts,'(none)') from %s.clusters where CID=%d",
                       varGet(GVAR_DEPUSER),
                       cluster_id);
   int rows=DEPOquery(query,0);
   TRACE("[rc=%d]\n",rows);
   char *conn_opts=malloc(1024);
   if (strcmp(DEPOgetString(0,4),"(none)") == 0) { conn_opts[0]=0x00; }
                                            else { sprintf(conn_opts,"options=%s",asLiteral(DEPOgetString(0,4))); };
   fprintf(fW,"primary_conninfo = 'host=%s port=%s user=%s password=%s %s'\n",
              DEPOgetString(0,2),
              DEPOgetString(0,3),
              DEPOgetString(0,0),
              unscramble(DEPOgetString(0,1)),
              conn_opts);
   free(conn_opts);
   DEPOqueryEnd();

   char* RECM_RESTORE_COMMAND=getenv (ENVIRONMENT_RECM_RESTORE_COMMAND);
   if (RECM_RESTORE_COMMAND != NULL) 
   {
      fprintf(fW,"restore_command='%s'\n",RECM_RESTORE_COMMAND);
   }
   else
   {
      fprintf(fW,"restore_command='[ -r %s/%s.gz ] && gunzip -c %s/%s.gz > %s || cp -p %s/%s %s'\n",
                 WALdir,"%f",
                 WALdir,"%f","%p",
                 WALdir,"%f","%p");
   };
   fprintf(fW,"wal_level = hot_standby\n");
   fprintf(fW,"max_wal_senders = 10\n");
   fprintf(fW,"hot_standby = on\n");
   fclose(fW);
   free(query);
   free(backupName);
   free(autoFileName);
}


//********************************************************************************
// If engine is stopped, the socket will disappear
//********************************************************************************
int isUnixSocketPresent()
{
   int isPresent=false;
   char * unix_socket_file=malloc(1024);
   sprintf(unix_socket_file,"%s/.s.PGSQL.%ld",varGet(GVAR_SOCKPATH),varGetLong(GVAR_CLUPORT));
   TRACE("unix_socket_file=%s\n",unix_socket_file);
   isPresent=file_exists(unix_socket_file);
   TRACE("ispresent=%d\n",isPresent);
   free(unix_socket_file);
   TRACE("ARGS(none) --> RETURN (int) %d\n",isPresent);   
   return(isPresent);
}


//********************************************************************************
// initialize sequence
//********************************************************************************
void createSequence(int initvalue)
{
   sequence=initvalue;
   char *format=malloc(20);
   sprintf(format,"%ld",sequence);
   varAdd(RECM_BCKSEQ,format);
   free(format);
   TRACE("ARGS(initvalue=%d) [RECM_BCKSEQ='%s']\n",initvalue,varGet(RECM_BCKSEQ));   
}

//********************************************************************************
// increase sequence
//********************************************************************************
void nextSequence()
{
   sequence++;
   char *format=malloc(20);
   sprintf(format,"%ld",sequence);
   varAdd(RECM_BCKSEQ,format);
   free(format);
   TRACE("ARGS(none) [RECM_BCKSEQ='%s']\n",varGet(RECM_BCKSEQ));   
}


char *buildFileName(const char *fn,const char *defNam)
{
   static char *newf=NULL;
   if (newf == NULL) newf=malloc(1024);
   if (strlen(fn) == 0) 
   { 
      sprintf(newf,"%s/%s",varGet(GVAR_CLUPGDATA),defNam); 
   }
   else 
   { 
      if (fn[0] == '/') { strcpy(newf,fn); }
                   else { if (fn[0] == '.') fn++;
                          sprintf(newf,"%s/%s",varGet(GVAR_CLUPGDATA),fn);
                        };
   }
   TRACE("ARGS(fn='%s' defNam='%s') --> RETURN (char *) %s\n",fn,defNam,newf);   
   return(newf);
}

/* At abort exit */
void intHandler(int dummy) 
{
    struct termios info;
    tcgetattr(STDOUT_FILENO, &info);          /* get current terminal attirbutes; 0 is the file descriptor for stdin */
    info.c_lflag = info.c_lflag ^ICANON;                  /* disable canonical mode */
    info.c_cc[VMIN] = 0;                      /* wait until at least one keystroke available */
    info.c_cc[VTIME] = 0;                     /* no timeout */
    tcsetattr(STDOUT_FILENO, TCSANOW, &info); /* set immediately */
    tcgetattr(STDOUT_FILENO, &info);          /* get current terminal attirbutes; 0 is the file descriptor for stdin */
    info.c_lflag = info.c_lflag ^ ECHO;                    /* disable echo */
    info.c_cc[VMIN] = 0;                      /* wait until at least one keystroke available */
    info.c_cc[VTIME] = 0;                     /* no timeout */
    tcsetattr(STDOUT_FILENO, TCSANOW, &info); /* set immediately */
    exit(1);
}

/* at normal exit */
void exitHandler()
{
    struct termios info;
    tcgetattr(STDOUT_FILENO, &info);          /* get current terminal attirbutes; 0 is the file descriptor for stdin */
    info.c_lflag = info.c_lflag ^ICANON;                  /* disable canonical mode */
    info.c_cc[VMIN] = 0;                      /* wait until at least one keystroke available */
    info.c_cc[VTIME] = 0;                     /* no timeout */
    tcsetattr(STDOUT_FILENO, TCSANOW, &info); /* set immediately */
    tcgetattr(STDOUT_FILENO, &info);          /* get current terminal attirbutes; 0 is the file descriptor for stdin */
    info.c_lflag = info.c_lflag ^ ECHO;                    /* disable echo */
    info.c_cc[VMIN] = 0;                      /* wait until at least one keystroke available */
    info.c_cc[VTIME] = 0;                     /* no timeout */
    tcsetattr(STDOUT_FILENO, TCSANOW, &info); /* set immediately */
}

static Array *historic=NULL;

void hstCmd_Add(const char *cmd)
{
   ArrayItem *itm;
   itm=arrayLast(historic);
   if (itm != NULL && strcmp(cmd,itm->key) == 0) { return; };
   arrayAddKey(historic,cmd,NULL);
   if (arrayCount(historic) > varGetInt(SYSVAR_HISTDEEP))
   {
      itm=arrayFirst(historic);
      arrayDelete(historic,itm);
   };
   itm=arrayLast(historic);
}

char *hstCmd_getPrevious()
{
   ArrayItem *itm=arrayPrevious(historic);
   if (itm == NULL) return(NULL);
   return(itm->key);
}

char *hstCmd_getNext()
{
   ArrayItem *itm=arrayNext(historic);
   if (itm == NULL) return(NULL);
   return(itm->key);
}

char *hstCmd_getFirst()
{
   ArrayItem *itm=arrayFirst(historic);
   if (itm == NULL) return(NULL);
   return(itm->key);
}

char *hstCmd_getLast()
{
   ArrayItem *itm=arrayLast(historic);
   if (itm == NULL) return(NULL);
   return(itm->key);
}

void hstCmd_list()
{
   ArrayItem *keepitm=arrayCurrent(historic);
   ArrayItem *itm=arrayFirst(historic);
   int i=1;
   while (itm != NULL)
   {
      printf("%3d) %s\n",i,itm->key);
      itm=arrayNext(historic);
      i++;
   }
   arraySetCurrent(historic,keepitm);
}

void hstSave()
{
   char *hstFile=malloc(1024);
   char* pHome=getenv (ENVIRONMENT_HOME);
   strcpy(hstFile,pHome);
   strcat(hstFile,"/");
   strcat(hstFile,".recm");
   mkdir (hstFile,S_IRWXU|S_IRGRP|S_IROTH);
   strcat(hstFile,"/recm.history");   
   FILE *Hw=fopen(hstFile, "w");
   ArrayItem *itm=arrayFirst(historic);
   int i=1;
   while (itm != NULL)
   {
      fprintf(Hw,"%s\n",itm->key);
      itm=arrayNext(historic);
      i++;
   }
   fclose(Hw);
   TRACE("ARGS(none) --> RETURN (none)\n");
}

void hstLoad()
{
   char *hstFile=malloc(1024);
   char* pHome=getenv (ENVIRONMENT_HOME);
   strcpy(hstFile,pHome);
   strcat(hstFile,"/");
   strcat(hstFile,".recm");
   mkdir (hstFile,S_IRWXU|S_IRGRP|S_IROTH);
   strcat(hstFile,"/recm.history");   
   FILE *Hw=fopen(hstFile, "r");
   if (Hw == NULL) return;
   char* line=malloc(1024);
   while (fgets(line, 1024, Hw) != NULL)
   {
      if (strlen(line) == 0)  continue;
      if (line[0] == '#')    continue;
      char *cr=strchr(line,'\n');
      if (cr != NULL) cr[0]=0x00;
      hstCmd_Add(line);
   }
   fclose(Hw);
   free(line);
   free(hstFile);
   TRACE("ARGS(none) --> RETURN (none)\n");
}


char * readFromKeyboard(const char *pprompt)
{
    printf("\n");
    static char *buffer=NULL;
    if (buffer == NULL) { buffer=malloc(2048); };
    char *display=malloc(2049);
    char *spaces=malloc(2049);
    char *prompt=malloc(256);
    varTranslate(prompt,pprompt,0); // accept variable substitution
    char *pspaces;
    char *pbuf;
    char *tmpv;

    int max_cursor=0;
    int max_length=0;
    int cursor=0;
    int hst_after_add=0;
    int ch;
    int ESCAPE=0;
    int INSERTION=1;
    ch=0x20;
    memset(buffer,0,2048);
    memset(spaces,' ',2048);
    pspaces=spaces;
    pspaces+=(max_length + strlen(prompt))+3;
    pspaces[0]=0x00;
    buffer[0]=0x00;
    cursor=0;
    max_cursor=0;
    pbuf=buffer;
    pbuf[0]=0x00;
    memset(display,0,2048);
//    sprintf(prompt,"(%3d)%2d/%2d:'%c']",ch,cursor,max_cursor,pbuf[0]);
    printf("\r%c[?7h%c[B%c[A",(char)27,(char)27,(char)27);
    printf("\r%c[K\r%s %s %c[B%c[A\r%s %s",(char)27,prompt,buffer,(char)27,(char)27,prompt,display);
    while((ch = getc(stdin)) != 10) // UNIX => 10= enter
    {
       //sprintf(prompt,"(%3d)%2d/%2d:'%c']",ch,cursor,max_cursor,pbuf[0]);
       switch(ch)
       {
       case 27 : // ESCAPE SEQUENCE
                 ch=getc(stdin);
                 ch=getc(stdin);
       //sprintf(prompt,"(%3d)%2d/%2d:'%c']",ch,cursor,max_cursor,pbuf[0]);
                 switch(ch)
                 {
                 case 65 : /* UP */
                           if (hst_after_add == 0) { tmpv=hstCmd_getLast(); } else { tmpv=hstCmd_getPrevious(); };
                           hst_after_add++;
                           if (tmpv == NULL) { tmpv=hstCmd_getLast();};
                           if (tmpv == NULL) {  buffer[0]=0x00;
                                                cursor=0;
                                                max_cursor=0;
                                                pbuf=buffer;
                                             }
                                        else {    
                                                strcpy(buffer,tmpv);
                                                max_cursor=strlen(buffer);
                                                cursor=max_cursor;
                                                pbuf=buffer;pbuf+=cursor;
                                             }
                           break;
                 case 66 : /* DOWN */ 
                           if (hst_after_add == 0) { tmpv=hstCmd_getFirst(); } else { tmpv=hstCmd_getNext(); };
                           hst_after_add++;
                           if (tmpv == NULL) { tmpv=hstCmd_getFirst();};
                           if (tmpv == NULL) {  buffer[0]=0x00;
                                                cursor=0;
                                                max_cursor=0;
                                                pbuf=buffer;
                                             }
                                        else {    
                                                strcpy(buffer,tmpv);
                                                max_cursor=strlen(buffer);
                                                cursor=max_cursor;
                                                pbuf=buffer;pbuf+=cursor;
                                             }
                           break;
                 case 68 : /* LEFT */ 
                           if (cursor > 0) { pbuf--; cursor--; }
                           break;
                 case 67 : /* RIGHT */ 
                           if (cursor < max_cursor) { pbuf++; cursor++; }
                           break;
                 case 50 : if (INSERTION == 0) { INSERTION++; } else { INSERTION=0; };        
                           break;
                 case 51 : /* delete key */
                           if (cursor < max_cursor) 
                           { 
                              pbuf++; 
                              cursor++;
                              char *dst=pbuf;
                              char *src=dst;
                              dst--;
                              int j=cursor;
                              while (j <= max_cursor)
                              {
                                 dst[0]=src[0];
                                 dst++;
                                 src++;
                                 j++;
                              };
                              pbuf--;
                              cursor--;
                              max_cursor--;
                           }
                           break;
                  default: printf("\n\n\nKEY=%d\n\n\n\n",ch);
                                         
                 };
                 ch=' ';
                 break;
       case 10 : break;
       case 126: break;
       case 127: // backspace
                 if (cursor > 0) 
                 { 
//                    printf("\n\ncursor=%d maxcursor=%d char cursor='%c'\n",cursor,max_cursor,pbuf[0]);//
                    char *dst=pbuf;
                    char *src=dst;
                    dst--;
                    int j=cursor;
                    while (j <= max_cursor)
                    {
                       dst[0]=src[0];
                       dst++;
                       src++;
                       j++;
                    };
                    pbuf--;
                    cursor--;
                    max_cursor--;
                 }
                 break;                           
       default : if (ESCAPE > 0) { ESCAPE=0; }
                 else
                 { 
                    if (INSERTION == 0)
                    {
                       pbuf[0]=ch;
                       pbuf++;
                       cursor++;
                       pbuf[0]=0x00;
                       if (cursor > max_cursor) {  max_cursor=cursor; }; 
                       ESCAPE=0;
                     }
                     else
                     {
                        max_cursor++;
                        char *dst=buffer;dst+=max_cursor;
                        char *src=dst;src--;
                        int j=max_cursor;
                        while (j >= cursor)
                        {
                           dst[0]=src[0];
                           dst--;
                           src--;
                           j--;
                        };
                        pbuf[0]=ch;
                        pbuf++;
                        cursor++;
                        //if (cursor > max_cursor) max_cursor++;
                     };
                 }
       }
       if (cursor > max_cursor) max_cursor=cursor;
       if (max_length < max_cursor) { max_length=max_cursor; };
       memset(spaces,' ',2048);pspaces=spaces;pspaces+=(max_length + strlen(prompt))+3;pspaces[0]=0x00;
       memset(display,0,2048);

       strncpy(display,buffer,cursor);
       printf("\r%c[K\r%s %s %c[B%c[A\r%s %s",(char)27,prompt,buffer,(char)27,(char)27,prompt,display);
    };
    if (strlen(buffer) > 0) hstCmd_Add(buffer);
    hst_after_add++;
    free(display);
    free(spaces);
    printf("\n");
    strcat(buffer,";");
    return(buffer);
};

long prettySize(long value,char unit)
{
   switch(unit)
   {
   case 'G' :;
   case 'g' : return (value / 1024  / 1024  / 1024 );break;
   case 'M' :;
   case 'm' : return (value / 1024  / 1024);break;
   case 'K' :;
   case 'k' : return (value / 1024);break;
   };
   return(value);
}

char *DisplayprettySize(long siz)
{
   static char *buf=NULL;
   if (buf == NULL) buf=malloc(128);
   long pretty=siz;
   if (pretty < 1024) { sprintf(&buf[0],"%ld Bytes",pretty);return(&buf[0]);};
   pretty=(pretty/1024); // Kb
   if (pretty < 1024) { sprintf(&buf[0],"%ld KB",pretty);return(&buf[0]);};
   pretty=(pretty/1024); // Mb
   if (pretty < 1024) { sprintf(&buf[0],"%ld MB",pretty);return(&buf[0]);};
   pretty=(pretty/1024); // Gb
   sprintf(&buf[0],"%ld GB",pretty);
   return(&buf[0]);
}


#define QUAL_OPTIONAL   1
#define QUAL_MANDATORY  2
#define QUAL_STRING     4
#define QUAL_NUMBER     8
#define QUAL_DATE       16
#define QUAL_NOTDEFINED 32


int QualifierIsMandatory(int idcmd,const char *qualifier)
{
   char *s=strstr(recm_commands[idcmd].qal,qualifier);
   if (s == NULL) { return (QUAL_NOTDEFINED); };
   char *eq=strchr(s,'=');
   if (eq == NULL) { return(QUAL_NOTDEFINED); };
   while (eq[0] != ',')
   {
      if (eq[0] == 'M') { return (QUAL_MANDATORY); }
      eq++;
   }
   return(QUAL_OPTIONAL);
}

int QualifierValueType(int idcmd,const char *qualifier)
{
   char *s=strstr(recm_commands[idcmd].qal,qualifier);
   if (s == NULL) { return (QUAL_NOTDEFINED); };
   char *eq=strchr(s,'=');
   if (eq == NULL) { return(QUAL_NOTDEFINED); };
   while (eq[0] != ',')
   {
      if (eq[0] == 'X') { return (QUAL_STRING); }
      if (eq[0] == 'S') { return (QUAL_STRING); }
      if (eq[0] == 'N') { return (QUAL_NUMBER); }
      if (eq[0] == 'D') { return (QUAL_DATE);   }
      eq++;
   }
   return(QUAL_NOTDEFINED);
}

int checkAllQuotes(char * buffer)
{
    int quote=0;
    char *q=buffer;
    while (q[0] != 0x00)
    {
       if (q[0] == '"') quote++;
       q++;
    }
    if ((quote % 2) != 0)
    {
       ERROR(ERR_MISSQUOTE,"Unclose string quote.\n");
       return(false);
    }
    return(true);
}


char *splitLines(char *buffer,int linenumber)
{
   //TRACE("ARGS: buffer='%s' linenumber=%d\n",buffer,linenumber);
   static char *response=NULL;
   if (response == NULL) response=malloc(1024);
   int ln=0;
   int c;
   int pc;
   int quote=0;
   char *pbuf=buffer;
   char *res=response;
   res[0]=0x00;
   c=pbuf[0];
   pc=';';
   while (c != 0x00 && ln < 20)
   {
//      TRACE("ln=%d linenumber=%d response='%s'\n",ln,linenumber,response);
      switch (c)
      {
      case '"'  : quote++;
                  res[0]=c;res++;res[0]=0x00;
                  if (pc == c) { quote--; }
                  break;
      case ' '  : if ((quote % 2) != 0) { res[0]=c;res++;res[0]=0x00; }
                                    else { if (pc != c) {res[0]=c;res++;res[0]=0x00;};};
                  break;
//      case '/'  : if ((quote % 2) != 0) { res[0]=c;res++;res[0]=0x00; } 
//                                   else { return(response); };
//                  break;
      case ';'  : if ((quote % 2) != 0) { res[0]=c;res++;res[0]=0x00; } 
                                   else { ln++;
                                          if (ln == linenumber)
                                          { 
                                             // remove trail spaces 
                                             char *e=response;
                                             e+=strlen(response);
                                             e--;
                                             while (e[0] == ' ') { e[0]=0x00;e--;};
                                             return(response); 
                                          }
                                          if (ln > linenumber) { return(NULL); }
                                          res=response;
                                          res[0]=0x00;
                                        }
                  break;
      case 0x00 : ln++;
                  if (ln == linenumber) { return(response); }
                  return(NULL);
                  break;
      default   : res[0]=c;res++;res[0]=0x00;
      };
      pc=c;
      pbuf++;
      c=pbuf[0];
   }
   return(NULL);
}


char *getStringValue(char *buffer)
{
   TRACE("ARGS: buffer='%s'\n",buffer);
   static char *response=NULL; 
   if (response == NULL) response=malloc(1024);
   int c;
   int pc=' ';
   int quote=0;
   char *pbuf=buffer;
   char *res=response;
   res[0]=0x00;
   c=pbuf[0];
   while (c != 0x00)
   {
      //TRACE("response='%s' (c=%d '%c')\n",response,c,c);
      switch (c)
      {
      case '"'  : quote++;
                  if ((quote % 2) != 0) { if (pc == c) { res[0]=c;res++;res[0]=0x00; quote--; } } 
                                   else { if (quote > 2) {res[0]=c;res++;res[0]=0x00; }; };
                  break;
      case ' '  : if ((quote % 2) != 0) { res[0]=c;res++;res[0]=0x00; } 
                                   else { return(response); };
                  break;
      case '/'  : if ((quote % 2) != 0) { res[0]=c;res++;res[0]=0x00; } 
                                   else { return(response); };
                  break;                 
      case 0x00 : return(response);
                  break;
      default   : res[0]=c;res++;res[0]=0x00;
      };
      pc=c;
      pbuf++;
      c=pbuf[0];
   }
   return(response);
}


// A Qualifier is an option with a value.
char *getQualifier(const char * command_line,const char *qualifier)
{
   TRACE("ARGS: command_line='%s' qualifier='%s'\n",command_line,qualifier);
   static char *response=NULL;
   if (response == NULL) response=malloc(1024);
   char *fnd=strstr(command_line,qualifier);
   TRACE("Search '%s' in '%s' = '%s'\n",qualifier,command_line,fnd);
   if (fnd == NULL) return (NULL);
   char *equal=strchr(fnd,'=');
   if (equal == NULL) { ERROR(ERR_QUALNOVALUE,"Qualifier '%s' must have a value\n",qualifier); return(NULL); };
   equal++;
   char *q=getStringValue(equal);
   TRACE("getStringValue='%s'\n",q);
   if (q == NULL) { return(NULL); }
   strcpy(response,q);
   TRACE(" --> RETURN='%s'\n",response);
   return(response);
}

char *commandDisplay(int id)
{
   static char *result=NULL;
   if (result == NULL) result=(char *)malloc(256);
   strcpy(result,recm_commands[id].cmd);
   char *p=result;
   while (p[0] != 0x00)
   {
      if (p[0]=='-') p[0]=' ';
      p++;
   }
   return(result);
}

char *hlpKey(int idcmd,const char *keyword)
{
//   static char *empty=NULL;
//   if (empty == NULL) { empty=malloc(5);empty[0]=0x00; };
   int i=0;
   int fnd=0;
   char *k=(char *)keyword;
   if (k[0] == '/') k++;
   while (recm_help_options[i].id != 0 && fnd == 0)
   {
      if (recm_help_options[i].id == idcmd && strcmp(recm_help_options[i].opt,k) == 0) { fnd++; }
                                                                                  else { i++; };
   };
   if (fnd == 0) return(NULL);
   return(recm_help_options[i].hlp);
}

void showHelp(int id,int with_header)
{
   if (recm_commands[id].cmd[0] == '@') return;                                 // Hide internal debugging commands
   if (with_header!= 0)
   {
      printf("%s\n",commandDisplay(id));
      printf("   %s\n",recm_commands[id].hlp);
   };
   int n=0;
   char *msg=malloc(1024);
   msg[0]=0x00;
   if (recm_commands[id].opt != NULL)
   {
      char *optp=strdup(recm_commands[id].opt);
      char *opt=optp;
      opt++; // bypass first comma
      char *virgule=strchr(opt,',');
      while (virgule != NULL)
      {
         virgule[0]=0x00;
         if (n==0) { printf("   Available Option(s):\n"); }
              else { strcat(msg,", "); };
         char *hlptext=hlpKey(recm_commands[id].id,opt);
         if (hlptext != NULL) 
         { 
            // Check if we have multiple lines delimited by '\n'
            char *wrkhlp=strdup(hlptext);
            char *line=wrkhlp;
            char *cr=strchr(line,'\n');
            int lc=0;
            while (cr != NULL)
            {
               cr[0]=0x00;
               if (lc == 0) { printf("      %-25s %s\n",opt,line); }
                       else { printf("      %-25s %s\n","",line); }
               line=cr;line++;
               cr=strchr(line,'\n');
               lc++;
            };
            if (lc == 0) { printf("      %-25s %s\n",opt,line); }
                    else { printf("      %-25s %s\n","",line); }
            free(wrkhlp);
         }
         else 
         { printf("      %s\n",opt); };
         opt=virgule;
         opt++;
         virgule=strchr(opt,',');
         n++;
      };
      free(optp);
   }
   if (recm_commands[id].qal != NULL)
   {
      char *qalp=strdup(recm_commands[id].qal);
      char *qal=qalp;
      qal++; // Bypass first comma
      n=0;
      char *virgule=strchr(qal,',');
      while (virgule != NULL)
      {
         virgule[0]=0x00;
         if (n==0) printf("   Available Qualifier(s):\n");
         char *equal=strchr(qal,'=');
         if (equal == NULL) { printf("Invalid definition for : %s\n",qal); return; };
         equal[0]=0x00;
         equal++;
         sprintf(msg,"%s=",qal);
         if (strchr(equal,'D')!= NULL) { sprintf(msg,"%s=<date>",qal); };
         if (strchr(equal,'S')!= NULL) { sprintf(msg,"%s=<String>",qal); };
         if (strchr(equal,'X')!= NULL) { sprintf(msg,"%s=<yes|no>",qal); };
         if (strchr(equal,'N')!= NULL) { sprintf(msg,"%s=<Number>",qal); };
         if (strchr(equal,'M')!= NULL) { strcat (msg," (Mandatory)"); };
         char *hlptext=hlpKey(recm_commands[id].id,qal);
         if (hlptext != NULL) 
         { 
            // Check if we have multiple lines delimited by '\n'
            char *wrkhlp=strdup(hlptext);
            char *line=wrkhlp;
            char *cr=strchr(line,'\n');
            int lc=0;
            while (cr != NULL)
            {
               cr[0]=0x00;
               if (lc == 0) { printf("      %-25s %s\n",msg,line); }
                       else { printf("      %-25s %s\n","",line); }
               line=cr;line++;
               cr=strchr(line,'\n');
               lc++;
            };
            if (lc == 0) { printf("      %-25s %s\n",msg,line); }
                    else { printf("      %-25s %s\n","",line); }
            free(wrkhlp);
         }
         else 
         { printf("      %s\n",msg); };
         qal=virgule;
         qal++;
         virgule=strchr(qal,',');
         n++;
       };
       free(qalp);
   }
   free(msg);
}

char *extractCommandSegment(const char *segment,int number)
{
   static char *wrkstr=NULL;
   if (wrkstr == NULL) wrkstr=malloc(1024);
   strcpy(wrkstr,segment);strcat(wrkstr,"-");
   char *sign=strchr(wrkstr,'-');
   char *result=wrkstr;

   if (sign != NULL) { sign[0]=0x00;sign++; }
   
   int id=0;
   while (id < number && sign != NULL)
   {
//      TRACE("extractCommandSegment:%d='%s'\n",id,result);
      if (id < number)
      {
         result=sign;
         sign=strchr(result,'-');
         if (sign != NULL) { sign[0]=0x00;sign++; }
      }
      id++;
   }
   return(result);
}

int strcmpCommand(const char *response,const char *compareCommand)
{
//   TRACE("ARGS: response='%s' compareCommand='%s'\n",response,compareCommand);
   char wrkstr[128];
   char workingResponse[256];
   char finalResponse[256];
   finalResponse[0]=0x00;
   workingResponse[0]=0x00;
   int nn=0;
   while (nn < 4)
   {
      strcpy(wrkstr,extractCommandSegment(response,nn));
      int fndDup=0;
      int lastfnd=-1;
      int j=0;
      if (strlen(wrkstr) < 3)
      {
         ERROR(ERR_BADCOMMAND,"Too short. A command must contain at least 3 characters)\n");
         return(2);
      }
      if (nn > 0 && strlen(wrkstr) > 0) strcat(workingResponse,"-");
      strcat(workingResponse,wrkstr);
//      TRACE("(A)workingResponse='%s'\n",workingResponse);
      while (recm_commands[j].id != 0)
      {
         if (strncmp(workingResponse,recm_commands[j].cmd,strlen(workingResponse)) == 0) { fndDup++; lastfnd=j; };
         j++;
      }
      switch(fndDup)
      {
      case 0 : return(2);
               break;
      case 1 : if (nn > 0 ) strcat(finalResponse,"-");
               strcat(finalResponse, extractCommandSegment(recm_commands[lastfnd].cmd,nn));
               strcpy(workingResponse,finalResponse);
               if (strlen(extractCommandSegment(response,nn+1)) == 0 && strcmp(finalResponse,recm_commands[lastfnd].cmd) == 0)
               {
                  return( strncmp(finalResponse,compareCommand,strlen(finalResponse))==0);
               };
               break;
      default:
         if (strlen(extractCommandSegment(response,nn+1)) == 0)
         {
            ERROR(ERR_BADCOMMAND,"Ambiguous command '%s'\n",response);
            int jx=0;
            while (recm_commands[jx].id != 0)
            {
               if (strncmp(workingResponse,recm_commands[jx].cmd,strlen(workingResponse)) == 0) 
               { 
                  INFO("- '%s' (%s)\n",commandDisplay(jx),recm_commands[jx].hlp);
               } 
               jx++;
            }
            return(2);
         }
         else
         {
            strcat(finalResponse, extractCommandSegment(recm_commands[lastfnd].cmd,nn));
            strcpy(workingResponse,finalResponse);
         }
      };
      nn++;
   }
   return( strcmp(finalResponse,compareCommand));
}

int countKeywords(const char *keywords,const char *keyword)
{
//   TRACE("ARGS: keywords='%s' keyword='%s'\n",keywords,keyword);
   int result=0;
   if (keywords == NULL) return(result);
   char *itemlist=(char *)keywords;
   char *match=strstr(itemlist,keyword);
   while (match != NULL)
   {
//      TRACE("match='%s'\n",match);
      result++;
      itemlist=match;
      itemlist+=strlen(keyword);
      match=strstr(itemlist,keyword);
      
   };
//   TRACE("return('%d')\n",result);
   return(result);
}

char *getFullKeyword(const char *keywords,const char *keyword,char delimiter)
{
//   TRACE("ARGS: keywords='%s' keyword='%s' delimiter='%c'\n",keywords,keyword,delimiter);
   static char *result=NULL;
   if (result==NULL) result=malloc(1024);
   result[0]=0x00;
   if (keywords == NULL) return(result);
   char *match=strstr(keywords,keyword);
   if (match == NULL) return(result);
   match+=1; // skip ',/' chars
   strncpy(&result[0],match,30);
   match=strchr(&result[0],delimiter);
   match[0]=0x00;
//   TRACE("return('%s')\n",result);
   return(result);
}

void displayDuplicateKeywords(const char *keywords,const char *keyword,char delimiter)
{
   TRACE("ARGS: keywords='%s' keyword='%s' delimiter='%c'\n",keywords,keyword,delimiter);
   char tmpdata[128];
   char *itemlist=(char *)keywords;
   char *match=strstr(itemlist,keyword);
   while (match != NULL)
   {
      char *k=match;k++;
      strncpy(&tmpdata[0],k,30);
      k=strchr(&tmpdata[0],delimiter);
      if (k != NULL) k[0]=0x00;
      printf("%s ",tmpdata);
      itemlist=match;
      itemlist+=strlen(keyword);
      match=strstr(itemlist,keyword);
   };
}

int extract_command(const char *command_line)
{
   int is_help=0;
   char *s=(char *) command_line;
   static char *response=NULL;
   if (response == NULL) response=malloc(2048);
   char *r=response;
   int c=' ';
   int pc=' ';
   r[0]=0x00;
   while (s[0] != 0x00 && s[0] != '/')
   {
      pc=c;
      c=s[0];
      if (c == '\t') c=' ';
      if (c == ' ' && pc == ' ') { s++;continue; }; // Ignore space at the begining of the command and double spaces
      if (c == ' ' && pc != ' ') { if (strcmp(response,"help") == 0) { is_help++; };
                                   r[0]='-';
                                   r++;r[0]=0x00;
                                 }
                            else { r[0]=c;
                                   r++;r[0]=0x00;
                                 }
      s++;
   }
   if (is_help) { int id=0;
                  while (recm_commands[id].id != 0 && strcmp("help",recm_commands[id].cmd) != 0) { id++; };
                  return (id);
                 };
   
   char *lspace=r;lspace--;
   while (lspace[0] == ' ' || lspace[0] == '-') { lspace[0]=0x00;lspace--; };
   int id=0;
   int rccmp=strcmpCommand(response,recm_commands[id].cmd);
   while (recm_commands[id].id != 0 && rccmp != true)
   {
      if (rccmp != 2) rccmp=strcmpCommand(response,recm_commands[id].cmd);
      if (rccmp != true) { id++; };
   }
   if (recm_commands[id].id != 0)
   {
      // reset all options for this command ------------------------------------
      if (recm_commands[id].opt != NULL)
      {
         char *optp=strdup(recm_commands[id].opt);
         char *opt=optp;
         opt++;
         char *virgule=strchr(opt,',');
         while (virgule != NULL)
         {
            virgule[0]=0x00;
            char *opt_name=malloc(strlen(opt)+10);
            sprintf(opt_name,"opt_%s",++opt);
            strToLower(opt_name);
            UNSETOption(opt_name);
            free(opt_name);
            opt=virgule;
            opt++;
            virgule=strchr(opt,',');
         };
         free(optp);
      };
      if (recm_commands[id].qal != NULL)
      {
         char *qalp=strdup(recm_commands[id].qal);
         char *qal=qalp;
         qal++;           // Sikp first comma 
         char *virgule=strchr(qal,',');
         while (virgule != NULL)
         {
            virgule[0]=0x00;
            char *equal=strchr(qal,'=');
            if (equal == NULL) { printf("Invalid definition for : %s\n",qal); return(-1); };
            equal[0]=0x00;
         
            char *qal_name=malloc(strlen(qal)+10);
            sprintf(qal_name,"qal_%s",++qal);
            strToLower(qal_name);
            //varAdd(qal_name,QAL_UNSET);
            qualifierUNSET(qal_name);
            free(qal_name);
            qal=virgule;
            qal++;
            virgule=strchr(qal,',');
         };
         free(qalp);
      };
      // Now, we need to get all options and qualifiers
      char *arguments=strdup(command_line);
      char *arg=arguments;
      int inquote=0;
      char *slash=arg;
      while (slash[0] != 0x00 && slash[0] != '/') 
      {
         if (slash[0] == '"') inquote++;
         slash++;
         if (slash[0] == '/' && ((inquote % 2) != 0)) slash++;
      }
      while (slash != NULL && slash[0] != 0x00)
      {
         char *keyword=strdup(slash);
         char *kw=keyword;
         kw++;
         while (kw[0] != 0x00 && kw[0] != '=' && kw[0] != ' ' && kw[0] != '/') { kw++; };
         kw[0]=0x00;
         char *keyword_v=malloc(128);sprintf(keyword_v,",%s",keyword);
         char *keyword_e=malloc(128);sprintf(keyword_e,",%s",keyword);
         keyword++;
         char *in_option=NULL;
         int unique_opt=countKeywords(recm_commands[id].opt,keyword_v);
         unique_opt+=countKeywords(recm_commands[id].qal,keyword_v);
         if (recm_commands[id].opt != NULL) in_option=strstr(recm_commands[id].opt,keyword_v);
         char *in_qualifier=NULL;
         if (recm_commands[id].qal != NULL) in_qualifier=strstr(recm_commands[id].qal,keyword_e);
         if (unique_opt > 1)
         {
            ERROR(ERR_INVOPTION,"Ambiguous Option/Qualifier '/%s'\n",keyword);
            printf("Choose between ");
            displayDuplicateKeywords(recm_commands[id].opt,keyword_v,',');
            displayDuplicateKeywords(recm_commands[id].qal,keyword_v,'=');
            printf("\n");
            return(-2);
         }         
         if (in_option != NULL && unique_opt > 1)
         {
            ERROR(ERR_INVOPTION,"Ambiguous Option '/%s'\n",keyword); 
            printf("Choose between ");
            displayDuplicateKeywords(recm_commands[id].opt,keyword_v,',');
            displayDuplicateKeywords(recm_commands[id].qal,keyword_v,'=');
            printf("\n");
            return(-2);
         }
         if (in_qualifier != NULL && unique_opt > 1)
         {
            ERROR(ERR_INVOPTION,"Ambiguous Qualifier '/%s'\n",keyword); 
            printf("Choose between ");
            displayDuplicateKeywords(recm_commands[id].opt,keyword_v,',');
            displayDuplicateKeywords(recm_commands[id].qal,keyword_v,'=');
            printf("\n");
            return(-2);
         }
         if (in_option == NULL && in_qualifier == NULL)
         {
            ERROR(ERR_INVOPTION,"Invalid Option/Qualifier '/%s'\n",keyword); 
            showHelp(id,0);
            return(-1); 
         };
         if (in_option != NULL) 
         {
            char *opt_name=malloc(128);
            strcpy(keyword,getFullKeyword(recm_commands[id].opt,keyword_v,','));
            sprintf(opt_name,"opt_%s",++keyword);
            strToLower(opt_name);
            SETOption(opt_name);
            free(opt_name);
            char *ns=slash;ns+=strlen(keyword);
         };
         if (in_qualifier != NULL) 
         {
            char *workkey=malloc(128);
            sprintf(workkey,"/%s=",keyword);
            char *val=getQualifier(command_line,workkey);
            free(workkey);
            if (val == NULL) return (-1);
            char *qal_name=malloc(128);
            char *tmpnam=getFullKeyword(recm_commands[id].qal,keyword_v,'=');   // keyword start always by a '/'
            tmpnam++;
            sprintf(qal_name,"qal_%s",tmpnam);
            
            strToLower(qal_name);
            varAdd(qal_name,val);
            free(qal_name);
            
            char *ns=slash;ns+=strlen(keyword);
         }
         free(keyword_e);
         free(keyword_v);
         slash++;
         while (slash[0] != 0x00 && slash[0] != '/') 
         {
            if (slash[0] == '"') { inquote++;};
            slash++;
            if (slash[0] == '/' && ((inquote % 2) != 0)) { slash++; };
         }
      };
      free(arguments);
      if ((inquote % 2) != 0)
      {
         ERROR(ERR_MISSQUOTE,"Unclose string quote.\n");
         return(-1);
      }
      // Look now for missing Mandatory qualifier
      if ( recm_commands[id].qal != NULL)
      {
         char *qalp=strdup(recm_commands[id].qal);
         char *qal=qalp;
         qal++;           // Sikp first comma 
         char *virgule=strchr(qal,',');
         while (virgule != NULL)
         {
            virgule[0]=0x00;
            char *equal=strchr(qal,'=');
            if (equal == NULL) { printf("Invalid QAL definition for : %s\n",qal); return(-1); };
            equal[0]=0x00;
            equal++;
               
            char *qal_name=malloc(128);
            sprintf(qal_name,"qal_%s",++qal);
            strToLower(qal_name);
            if (strchr(equal,'M') != NULL && strcmp(varGet(qal_name),QAL_UNSET) == 0) 
            {
               ERROR(ERR_MISQUALVAL,"Mandatory Qualifier '/%s' require a value.\n",qal); 
               showHelp(id,0);
               return(-1);
            };
            free(qal_name);
            qal=virgule;
            qal++;
            virgule=strchr(qal,',');
         };
         free(qalp); 
      }
      return(id);      
   } 
   return(-1);
}

/********************************************************************************/
/* Initialize connection string with the given parameters                       */
/* connectString must be allocted.                                              */
/********************************************************************************/
void prepareConnectString(char *connectString,const char *pdb,const char *pusr,const char *ppwd,const char *phst,const char *pport )
{
   char *wrkstr=malloc(128);
   connectString[0]=0x00;
   if (pdb != NULL && strcmp(pdb,VAR_UNSET_VALUE) != 0)
   { sprintf(wrkstr," dbname=%s",pdb);  strcat(connectString,wrkstr); }
   if (pusr != NULL && strcmp(pusr,VAR_UNSET_VALUE) != 0)
   { sprintf(wrkstr," user=%s",pusr);  strcat(connectString,wrkstr); }
   
   if (ppwd != NULL && strcmp(ppwd,VAR_UNSET_VALUE) != 0)
   { sprintf(wrkstr," password=%s",ppwd);  strcat(connectString,wrkstr); }

   if (phst != NULL && strcmp(phst,VAR_UNSET_VALUE) != 0)
   { sprintf(wrkstr," host=%s",phst);  strcat(connectString,wrkstr); }

   if (pport != NULL && strcmp(pport,VAR_UNSET_VALUE) != 0)
   { sprintf(wrkstr," port=%s",pport);  strcat(connectString,wrkstr); }
   free(wrkstr); 
   TRACE("ARGS(connectString='%s' pdb='%s' pusr='%s' ppwd='%s' phst='%s' pport=%s) --> RETURN (string) '%s'\n",connectString,pdb,pusr,ppwd,phst,pport,connectString);
}

int DEPOloadData()
{
   if (varGetLong(GVAR_CLUCID) == 0) return(false);   

   char *query=malloc(1024);
   int rows;
   sprintf(query,"select clu_info from %s.clusters where cid=%s",
                 varGet(GVAR_DEPUSER),
                 varGet(GVAR_CLUCID));
   rows=DEPOquery(query,0);
   if (rows != 1)
   {
      DEPOqueryEnd();
      free(query);
      return(false);
   }
   varAdd(GVAR_CLUDESCR,DEPOgetString(0,0));
   DEPOqueryEnd();
   globalArgs.configChanged=1;
   SaveConfigFile("CLU",varGet(GVAR_CLUNAME),SECTION_CLUSTER);
//   SaveConfigFile(varGet(GVAR_DEPNAME),SECTION_DEPOSIT);
   free(query);
   return(true);
}


/********************************************************************************/
/* Check Deposit Version                                                        */
/********************************************************************************/
int DEPOCheckState()
{
   char *query=malloc(1024);
   sprintf(query,"select count(*) from %s.clusters where cid=%ld",
                 varGet(GVAR_DEPUSER),
                 varGetLong(GVAR_CLUCID));
   int rows=DEPOquery(query,0);
   if (rows == -1)
   {
      ERROR(ERR_NOTDEPOSIT,"Not connected to a DEPOSIT.\n");
      DEPOqueryEnd();
      free(query);
      return(false);
   }
   if (rows == 1 && DEPOgetLong(0,0) == 0)
   {
      ERROR(ERR_NOTREGISTERED,"Not registerd on this deposit. Use 'register cluster' command.\n");
      varAdd(GVAR_CLUCID,"0");
      DEPOqueryEnd();
      free(query);
      return(false);
   }
   if (DEPOloadData() == false)
   {
      ERROR(ERR_NOTREGISTERED,"Invalid CID %ld. Not registerd any more on this deposit. Use 'register cluster' command.\n",varGetLong(GVAR_CLUCID));
      varAdd(GVAR_CLUCID,"0");
      DEPOqueryEnd();
      free(query);
      return(false);
   };
   DEPOqueryEnd();
   INFO("Registered to '%s' (CID=%ld)\n",varGet(GVAR_REGDEPO),varGetLong(GVAR_CLUCID));
   
   /* We need to check if the cluster name is the same...The name of the DEPOSIT is the right one. */
   sprintf(query,"select clu_nam from %s.clusters where cid=%ld",
                 varGet(GVAR_DEPUSER),
                 varGetLong(GVAR_CLUCID));
   rows=DEPOquery(query,0);
   if (strcmp(DEPOgetString(0,0),varGet(GVAR_CLUNAME)) != 0)
   {
      WARN(WRN_CHANGEDNAME,"Your cluster is named '%s'.  It is registered under the name '%s'. \n",
                           varGet(GVAR_CLUNAME),
                           DEPOgetString(0,0));
   }
   DEPOqueryEnd();

   /* We need to validate the RECM version and the DEPOSIT one. */
   sprintf(query,"select version from %s.config",varGet(GVAR_DEPUSER));
   rows=DEPOquery(query,0);
   if (rows == 1 && strcmp(DEPOgetString(0,0),CURRENT_VERSION) != 0)
   {
      WARN(WRN_BADVERSION,"DEPOSIT is not at same version as RECM is.  Invoke 'upgrade' to upgrade your deposit\n");
   };
   DEPOqueryEnd();
   free(query);
   return(true);
}


/********************************************************************************/
/* Deposit Query routines                                                       */
/* open connection to the database                                              */
/********************************************************************************/
int DEPOconnect(const char *pdb,const char *pusr,const char *ppwd,const char *phst,const char *pport )
{
   char *connectString=malloc(1024);
   connectString[0]=0x00;
   prepareConnectString(connectString,pdb,pusr,ppwd,phst,pport);
   rep_cnx = PQconnectdb(connectString);
   if (PQstatus(rep_cnx) != CONNECTION_OK)
   {
       rep_cnx=NULL;
       varAdd(GVAR_DEPDB,  VAR_UNSET_VALUE);
       varAdd(GVAR_DEPUSER,VAR_UNSET_VALUE);
       varAdd(GVAR_DEPPWD, VAR_UNSET_VALUE);
       varAdd(GVAR_DEPHOST,VAR_UNSET_VALUE); 
       varAdd(GVAR_DEPPORT,VAR_UNSET_VALUE);
       ERROR(ERR_CNXFAILED,"Could not connect DEPOSIT using '%s'.\n",connectString);
       free(connectString);
       TRACE("ARGS: pdb='%s' pusr='%s' ppwd='%s' phst='%s' pport='%s') --> RETURN (int) false\n",pdb,pusr,ppwd,phst,pport);                                                   
       return (false);
   };
   free(connectString);
   if (pdb != NULL && strcmp(pdb,VAR_UNSET_VALUE) != 0)     { varAdd(GVAR_DEPDB,pdb); } 
                                                       else { varAdd(GVAR_DEPDB,VAR_UNSET_VALUE); };
                                                       
   if (pusr != NULL && strcmp(pusr,VAR_UNSET_VALUE) != 0)   { varAdd(GVAR_DEPUSER,pusr); } 
                                                       else { varAdd(GVAR_DEPUSER,VAR_UNSET_VALUE); };
   if (ppwd != NULL && strcmp(ppwd,VAR_UNSET_VALUE) != 0)   { varAdd(GVAR_DEPPWD,ppwd); } 
                                                       else { varAdd(GVAR_DEPPWD,VAR_UNSET_VALUE); };
   if (phst != NULL && strcmp(phst,VAR_UNSET_VALUE) != 0)   { varAdd(GVAR_DEPHOST,phst); } 
                                                       else { varAdd(GVAR_DEPHOST,VAR_UNSET_VALUE); };
   if (pport != NULL && strcmp(pport,VAR_UNSET_VALUE) != 0) { varAdd(GVAR_DEPPORT,pport); } 
                                                       else { varAdd(GVAR_DEPPORT,VAR_UNSET_VALUE); };
   TRACE("ARGS: pdb='%s' pusr='%s' ppwd='%s' phst='%s' pport='%s') --> RETURN (int) true\n",pdb,pusr,ppwd,phst,pport);                                                   
   return(true);
}

void DEPOdisconnect()
{
   TRACE("ARGS(none)\n");
   if (rep_cnx != NULL) { PQfinish(rep_cnx); };
   rep_cnx=NULL;
   varDel(GVAR_DEPNAME);
   varDel(GVAR_DEPDESCR);
   varDel(GVAR_DEPHOST);
   varDel(GVAR_DEPPORT);
   varDel(GVAR_DEPUSER);
   varDel(GVAR_DEPPWD);
   varDel(GVAR_DEPDB);
}

void DEPOqueryEnd()
{
   PQclear(DEPOresult[PGresID].res);
   DEPOresult[PGresID].res=NULL;
   DEPOresult[PGresID].rows=0;
   PGresID--;
   TRACE("ARGS(none) [ResID=%d] --> RETURN (none)\n",PGresID);
}

PGresult *DEPOgetRes()
{
   return (DEPOresult[PGresID].res);
}


int DEPOgetLength(int row,int col)
{ 
   int rc=PQgetlength(DEPOresult[PGresID].res,row,col);
   TRACE("ARGS(row=%d col=%d) --> RETURN (int) %d\n",row,col,rc);
   return (rc);
}

char *DEPOgetString(int row,int col)
{
   if (PQgetisnull(DEPOresult[PGresID].res,row,col) == 1)
   {
      TRACE("ARGS(row=%d col=%d) --> RETURN (char *) NULL\n",row,col);
      return(NULL);
   }
   else
   {
      TRACE("ARGS(row=%d col=%d) --> RETURN (char *) '%s'\n",row,col,PQgetvalue(DEPOresult[PGresID].res,row,col));
      return (PQgetvalue(DEPOresult[PGresID].res,row,col));
   }
}

long DEPOgetLong(int row,int col)
{
   if (PQgetisnull(DEPOresult[PGresID].res,row,col) == 1) 
   { 
      TRACE("ARGS(row=%d col=%d) --> RETURN (long) 0 [NULL]\n",row,col);
      return(0);
   }
   else 
   { 
      TRACE("ARGS(row=%d col=%d) --> RETURN (long) %ld\n",row,col,atol(PQgetvalue(DEPOresult[PGresID].res,row,col)));
      return (atol(PQgetvalue(DEPOresult[PGresID].res,row,col)));
   }
}
int DEPOgetInt(int row,int col)
{
   if (PQgetisnull(DEPOresult[PGresID].res,row,col) == 1)
   {
      TRACE("ARGS(row=%d col=%d) --> RETURN (int) 0 [NULL]\n",row,col);
      return(0);
   }
   else
   {
      TRACE("ARGS(row=%d col=%d) --> RETURN (int) %ld\n",row,col,atol(PQgetvalue(DEPOresult[PGresID].res,row,col)));
      return (atoi(PQgetvalue(DEPOresult[PGresID].res,row,col)));
   }
}

int DEPOisConnected(int showMessage)
{
   int res=( rep_cnx != NULL);
   if (res == false && showMessage==true) ERROR(ERR_NOTCONNECTED,"Not connected to any deposit.\n");
   return(res);
}

// Number of rows affected
long DEPOrowCount()
{
   TRACE("ARGS(none) RETURN (long) %ld\n",DEPOresult[PGresID].rows);
   return(DEPOresult[PGresID].rows);
}

char *DEPOgetErrorMessage()
{
   return( PQerrorMessage(rep_cnx));
}


// return the number of rows
int DEPOquery(const char *query,int with_retry)
{
   TRACE("ARGS(Query '%s' )\n",query);
   PGresID++;
   if (PGresID > PGresID_LIMIT) { printf("PGresID LIMIT reached(%d)\n",PGresID); return(-1); };
   TRACE("PGresID=%d\n",PGresID);
   int try_count=4;
   if (with_retry == 0) try_count=1;
   int finish=1;
   while (try_count > 0 && finish != 0)
   {
      DEPOresult[PGresID].res=PQexec(rep_cnx,query);
      if (DEPOresult[PGresID].res == NULL) { TRACE("QUERY ERROR ?\n"); return(-1); };
      ExecStatusType qry_rc=PQresultStatus(DEPOresult[PGresID].res);
      switch (qry_rc)
      {
         case PGRES_COMMAND_OK : TRACE("PGRES_COMMAND_OK\n");
                                 DEPOresult[PGresID].rows=atol(PQcmdTuples(DEPOresult[PGresID].res));
                                 TRACE(" --> RETURN (int) 0\n");
                                 return(0);                                     // Return 0 : query success, but do not return any row
                                 break;
         case PGRES_TUPLES_OK  : TRACE("PGRES_TUPLES_OK\n");
                                 DEPOresult[PGresID].rows=atol(PQcmdTuples(DEPOresult[PGresID].res));
                                 TRACE(" --> RETURN (int) %ld\n",DEPOresult[PGresID].rows);
                                 return(DEPOresult[PGresID].rows);                     // Return n : query success, return number of rows
                                 break;
         case PGRES_FATAL_ERROR: TRACE("PGRES_FATAL_ERROR\n");
                                 VERBOSE("Deposit connection error. Trying to reconnect[PGRES_FATAL_ERROR]...\n");
                                 try_count--;
                                 if (with_retry == 1)
                                 {
                                    sleep(10);
                                    DEPOconnect(varGet(GVAR_DEPDB),
                                                varGet(GVAR_DEPUSER),
                                                varGet(GVAR_DEPPWD),
                                                varGet(GVAR_DEPHOST),
                                                varGet(GVAR_DEPPORT)); 
                                 };
                                 break;
         case PGRES_BAD_RESPONSE :
            TRACE("PGRES_BAD_RESPONSE\n");
            TRACE("%s",PQresStatus(qry_rc));
            VERBOSE("Cluster connection error. Trying to reconnect[PGRES_BAD_RESPONSE]...\n");
            try_count--;
            if (with_retry == 1)
            {
               sleep(10);
               DEPOconnect(varGet(GVAR_DEPDB),
                           varGet(GVAR_DEPUSER),
                           varGet(GVAR_DEPPWD),
                           varGet(GVAR_DEPHOST),
                           varGet(GVAR_DEPPORT));
            }
            break;
         default:
            printf("unknown : %d : %s\n",qry_rc,PQresStatus(qry_rc));
      }
   }
   TRACE(" --> RETURN (int) -1\n");
   return(-1);                                                              // Return -1 : qquery failed
}


char *DisplayElapsed(long seconds)
{
   long phrs=0;
   long pmin=0;
   long psec=0;
   static char*res=NULL;
   if (res == NULL) res=malloc(60);
   if (seconds > 3600) phrs=(seconds / 3600);
   if (seconds > 60) pmin=((seconds - (phrs * 3600)) / 60);
   psec=seconds - ((phrs * 3600) + (pmin*60));
   if (phrs > 0) 
   { sprintf(res,"%2ldh%02ld:%02ld",phrs,pmin,psec); }
   else
   {
      if (pmin > 0)
      { sprintf(res,"0h%02ld:%02ld",pmin,psec);}
      else 
      { sprintf(res,"%02lds",psec); };
   }
   return(res);
}


/********************************************************************************/
/* Get the status of my running backup (return TRUE if my backup is 'RUNNING')  */
/********************************************************************************/

int myBackupIsInProgress(const char *bckid)
{
   char *query=malloc(1024);
   sprintf(query,"select count(*) from %s.backups where cid=%s and bcksts=%d and bck_id='%s'",
                 varGet(GVAR_DEPUSER),
                 varGet(GVAR_CLUCID),
                 RECM_BACKUP_STATE_RUNNING,
                 bckid);
   int rc=DEPOquery(query,0);
   if (rc != 1)
   {
      ERROR(ERR_INTERNALERROR,"(%s) Look for my running backup error.\n",IE_RUNNINGBCK);
      return(false);
   };
   rc=DEPOgetInt(0,0);
   DEPOqueryEnd();
   TRACE("ARGS(bckid='%s') return (int) %d\n",bckid,rc);
   return (rc != 0); // Return TRUE if my backup is running
}


/********************************************************************************/
/* Check if a backup is running                                                 */
/********************************************************************************/
int backupInProgress()
{
   char *query=malloc(1024);
   sprintf(query,"select count(*) from %s.backups where cid=%s and bcksts=%d",
                 varGet(GVAR_DEPUSER),
                 varGet(GVAR_CLUCID),
                 RECM_BACKUP_STATE_RUNNING);
   int rc=DEPOquery(query,0);
   if (rc != 1)
   {
      ERROR(ERR_INTERNALERROR,"(%s) Look for running backup error.\n",IE_RUNNINGBCK);
      return(false);
   };
   rc=DEPOgetInt(0,0);
   DEPOqueryEnd();
   TRACE("ARGS(none) return (int) %d\n",rc);
   return (rc != 0); // Return TRUE if a backup is running
}

/********************************************************************************/
/* Get the BCK_ID of the running backup.  Return NULL if no backup is running   */
/********************************************************************************/
char * IdOfBackupInProgress()
{
   static char result[30];
   char *query=malloc(1024);
   sprintf(query,"select bck_id from %s.backups where cid=%s and bcksts=%d limit 1",
                 varGet(GVAR_DEPUSER),
                 varGet(GVAR_CLUCID),
                 RECM_BACKUP_STATE_RUNNING);
   int rc=DEPOquery(query,0);
   if (rc != 1)
   {
      ERROR(ERR_INTERNALERROR,"(%s) Get backup UID of running backup error.\n",IE_RUNNINGBCK);
      return(NULL);
   };
   strcpy(result,DEPOgetString(0,0));
   DEPOqueryEnd();
   TRACE("ARGS(none) return (string) '%s'\n",result);
   return (result);
}



/********************************************************************************/
/* Cluster Query routines                                                       */
/* open connection to the database                                              */
/********************************************************************************/
void CLUdisconnect()
{
   TRACE("ARGS(none)\n");
   if (clu_cnx != NULL) { PQfinish(clu_cnx); };
   clu_cnx=NULL;
   varDel(GVAR_CLUNAME);
   varDel(GVAR_CLUDESCR);
   varDel(GVAR_CLUVERSION);
   varDel(GVAR_CLUPGDATA);
   varDel(GVAR_CLUUSER);
   varDel(GVAR_CLUPWD);
   varDel(GVAR_CLUDB);
   varDel(GVAR_CLUPORT);
   varDel(GVAR_WALDIR);
   varDel(GVAR_BACKUPDIR);
   varDel(GVAR_COMPLEVEL);
   varDel(GVAR_REGDEPO);
}

/********************************************************************************/
/* Verify a CLUSTER is connected.                                               */
/********************************************************************************/
int CLUisConnected(int showMessage)
{
   int rc=(clu_cnx != NULL);
   if (rc == false && showMessage==true) ERROR(ERR_NOTCONNECTED,"Not connected to any cluster.\n");
   TRACE("ARGS(none) return (int) %d\n",rc);
   return(rc);
}

char *CLUgetErrorMessage()
{
   return( PQerrorMessage(clu_cnx));
}

void CLUqueryEnd()
{
   PQclear(CLUresult[PGcluID]);
   CLUresult[PGcluID]=NULL;
   PGcluID--;
   TRACE("ARGS(none) ResID=%d return (none)\n",PGcluID);
}

int CLUgetLength(int row,int col)
{ 
   return (PQgetlength(CLUresult[PGcluID],row,col));
}

char *CLUgetString(int row,int col)
{
   if (PQgetisnull(CLUresult[PGcluID],row,col) == 1) 
   { 
      TRACE("ARGS(row=%d col=%d) --> RETURN (char *) NULL\n",row,col);
      return(NULL); 
   }
   else
   {
      TRACE("ARGS(row=%d col=%d) --> RETURN (char *) '%s'\n",row,col,PQgetvalue(CLUresult[PGcluID],row,col));
      return (PQgetvalue(CLUresult[PGcluID],row,col)); 
   }
}

long CLUgetLong(int row,int col)
{
   if (PQgetisnull(CLUresult[PGcluID],row,col) == 1)
   {
      TRACE("ARGS(row=%d col=%d) --> RETURN (long) 0 [NULL]\n",row,col);
      return(0);
   }
   else
   {
      TRACE("ARGS(row=%d col=%d) --> RETURN (long) %ld\n",row,col,atol(PQgetvalue(CLUresult[PGcluID],row,col)));
      return (atol(PQgetvalue(CLUresult[PGcluID],row,col))); 
   }
}

int CLUgetInt(int row,int col)
{
   if (PQgetisnull(CLUresult[PGcluID],row,col) == 1)
   {
      TRACE("ARGS(row=%d col=%d) --> RETURN (int) 0 [NULL]\n",row,col);
      return(0);
   }
   else
   {
      TRACE("ARGS(row=%d col=%d) --> RETURN (int) %d\n",row,col,atoi(PQgetvalue(CLUresult[PGcluID],row,col)));
      return (atoi(PQgetvalue(CLUresult[PGcluID],row,col))); 
   }
}

int CLUquery(const char *query,int with_retry);


int CLUconnect(const char *pdb,const char *pusr,const char *ppwd,const char *phst,const char *pport )
{
   TRACE("ARGS: pdb='%s' pusr:'%s' ppwd:'%s' phst:'%s' pport:'%s'\n",pdb,pusr,ppwd,phst,pport);
   //char *wrkstr=malloc(128);
   char *connectString=malloc(1024);
   prepareConnectString(connectString,pdb,pusr,ppwd,phst,pport);
   clu_cnx = PQconnectdb(connectString);
   if (PQstatus(clu_cnx) != CONNECTION_OK)
   {
       varAdd(GVAR_CLUDB  ,VAR_UNSET_VALUE);
       varAdd(GVAR_CLUUSER,VAR_UNSET_VALUE);
       varAdd(GVAR_CLUPWD ,VAR_UNSET_VALUE);
       varAdd(GVAR_CLUHOST,VAR_UNSET_VALUE); 
       varAdd(GVAR_CLUPORT,VAR_UNSET_VALUE);
       clu_cnx=NULL;
       ERROR(ERR_CNXFAILED,"Could not connect using '%s'.\n",connectString);
       free(connectString);
       return (false);
   };
   free(connectString);
   int rows=CLUquery("select split_part(split_part(version(),' ',2),'.',1),version()",0);
   varAdd(GVAR_CLUVERSION,CLUgetString(0,0));
   varAdd(GVAR_CLUFULLVER,CLUgetString(0,1));
   CLUqueryEnd();
   rows=CLUquery("select setting from pg_settings where name='data_directory'",0);
   varAdd(GVAR_CLUPGDATA,CLUgetString(0,0));
   CLUqueryEnd();
   rows=CLUquery("select setting from pg_settings where name='cluster_name'",0);
   TRACE("CLUNAME=%s\n",CLUgetString(0,0));
   varAdd(GVAR_CLUNAME,CLUgetString(0,0));
   CLUqueryEnd();
   rows=CLUquery("select setting from pg_settings where name='archive_mode'",0);
   if (strcmp(CLUgetString(0,0),"off") == 0)
   {
      WARN(WRN_NOARCHIVE,"Parameter 'archive_mode' must be enabled.\n");
      return(false);
   }
   CLUqueryEnd();
   INFO("Connected to cluster '%s'\n",varGet(GVAR_CLUNAME));
   if (pdb != NULL && strcmp(pdb,VAR_UNSET_VALUE) != 0)     { varAdd(GVAR_CLUDB,pdb); } 
                                                       else { varAdd(GVAR_CLUDB,VAR_UNSET_VALUE); };

   if (pusr != NULL && strcmp(pusr,VAR_UNSET_VALUE) != 0)   { varAdd(GVAR_CLUUSER,pusr); } 
                                                       else { varAdd(GVAR_CLUUSER,VAR_UNSET_VALUE); };

   if (ppwd != NULL && strcmp(ppwd,VAR_UNSET_VALUE) != 0)   { varAdd(GVAR_CLUPWD,ppwd); } 
                                                       else { varAdd(GVAR_CLUPWD,VAR_UNSET_VALUE); };

   if (phst != NULL && strcmp(phst,VAR_UNSET_VALUE) != 0)   { varAdd(GVAR_CLUHOST,phst); } 
                                                       else { varAdd(GVAR_CLUHOST,VAR_UNSET_VALUE); };

   if (pport != NULL && strcmp(pport,VAR_UNSET_VALUE) != 0) { varAdd(GVAR_CLUPORT,pport); } 
                                                       else { varAdd(GVAR_CLUPORT,VAR_UNSET_VALUE); };

   if (varGet(SYSVAR_TIMEZONE) == NULL || strcmp(varGet(SYSVAR_TIMEZONE),"(null)")==0) 
   {
      // By default, we will take the first timezone...
      rows=CLUquery("select setting from pg_settings where name='log_timezone'",0);
      varAdd(SYSVAR_TIMEZONE,CLUgetString(0,0));
      CLUqueryEnd();
      globalArgs.configChanged=1;
      SaveConfigFile(NULL,"default",SECTION_DEFAULT);
   }

   if (CLUcheckPrivilege( PGPRIV_SUPERUSER ) == false) { return (false); };
   return(true);
}

PGresult *CLUgetRes()
{
   return (CLUresult[PGcluID]);
}

// return the number of rows
int CLUquery(const char *query,int with_retry)
{
   TRACE("ARGS(Query '%s', with_retry=%d)\n",query,with_retry);
   PGcluID++;
   if (PGcluID > PGresID_LIMIT) { printf("PGcluID LIMIT reached(%d)\n",PGcluID); return(-1); };
   TRACE("PGcluID=%d\n",PGcluID);
   int try_count=4;
   if (with_retry == 0) try_count=1;
   int finish=1;
   while (try_count > 0 && finish != 0)
   {
      CLUresult[PGcluID]=PQexec(clu_cnx,query);
      if (CLUresult[PGcluID] == NULL) { TRACE("QUERY ERROR ?\n"); return(-1); };
      ExecStatusType qry_rc=PQresultStatus(CLUresult[PGcluID]);
      switch (qry_rc)
      {
         case PGRES_COMMAND_OK : TRACE("PGRES_COMMAND_OK --> RETURN (int) 0\n");
                                 return(0);                                     // Return 0 : query success, but do not return any row
                                 break;
         case PGRES_TUPLES_OK  : TRACE("PGRES_TUPLES_OK --> RETURN (int) %d\n",PQntuples(CLUresult[PGcluID]));
                                 return(PQntuples(CLUresult[PGcluID]));                     // Return n : query success, return number of rows
                                 break;
         case PGRES_FATAL_ERROR: TRACE("PGRES_FATAL_ERROR\n");
                                 VERBOSE("Cluster connection error. Trying to reconnect[PGRES_FATAL_ERROR]...\n");
                                 try_count--;
                                 if (with_retry == 1)
                                 {
                                    sleep(10);
                                    CLUconnect(varGet(GVAR_CLUDB),
                                               varGet(GVAR_CLUUSER),
                                               varGet(GVAR_CLUPWD),
                                               varGet(GVAR_CLUHOST),
                                               varGet(GVAR_CLUPORT));
                                 };
                                 break;
         case PGRES_BAD_RESPONSE :
            TRACE("PGRES_BAD_RESPONSE ( %s )",PQresStatus(qry_rc));
            VERBOSE("Cluster connection error. Trying to reconnect[PGRES_BAD_RESPONSE]...\n");
            try_count--;
            if (with_retry == 1)
            {
               sleep(10);
               CLUconnect(varGet(GVAR_CLUDB),
                          varGet(GVAR_CLUUSER),
                          varGet(GVAR_CLUPWD),
                          varGet(GVAR_CLUHOST),
                          varGet(GVAR_CLUPORT));
            }
            break;
         default:;
      }
   }
   TRACE(" --> RETURN (int) -1\n");
   return(-1);                                                                  // Return -1 : qquery failed
}


/********************************************************************************/
/* Auxiliary Query routines                                                       */
/* open connection to the database                                              */
/********************************************************************************/
void AUXdisconnect()
{
   TRACE("ARGS: none\n");
   if (aux_cnx != NULL) { PQfinish(aux_cnx); };
   aux_cnx=NULL;
}

int AUXisConnected()
{
   return( aux_cnx != NULL);
}


char *AUXgetErrorMessage()
{
   return( PQerrorMessage(aux_cnx));
}

void AUXqueryEnd()
{
   PQclear(AUXresult[PGauxID]);
   AUXresult[PGauxID]=NULL;
   PGauxID--;
   TRACE("AUXqueryEnd:id=%d\n",PGauxID);
}

int AUXgetLength(int row,int col)
{ 
   return (PQgetlength(AUXresult[PGauxID],row,col));
}

char *AUXgetString(int row,int col)
{
   if (PQgetisnull(AUXresult[PGauxID],row,col) == 1) { return(NULL); }
                                                else { return (PQgetvalue(AUXresult[PGauxID],row,col)); }
}

long AUXgetLong(int row,int col)
{
   if (PQgetisnull(AUXresult[PGauxID],row,col) == 1) { return(0); }
                                                       else { return (atol(PQgetvalue(AUXresult[PGauxID],row,col))); }
}
int AUXgetInt(int row,int col)
{
   if (PQgetisnull(AUXresult[PGauxID],row,col) == 1) { return(0); }
                                                else { return (atoi(PQgetvalue(AUXresult[PGauxID],row,col))); }
}

int AUXquery(const char *query,int with_retry);

int AUXconnect(const char *pdb,const char *pusr,const char *ppwd,const char *phst,const char *pport )
{
   TRACE("ARGS: pdb='%s' pusr='%s' ppwd='%s' phst='%s' pport='%s'\n",pdb,pusr,ppwd,phst,pport);
   char *connectString=malloc(1024);
   prepareConnectString(connectString,pdb,pusr,ppwd,phst,pport);
   aux_cnx = PQconnectdb(connectString);
   if (PQstatus(aux_cnx) != CONNECTION_OK)
   {
       aux_cnx=NULL;
       ERROR(ERR_CNXFAILED,"Could not connect using '%s'.\n",connectString);
       free(connectString);
       return (false);
   };
   free(connectString);
   int rows=AUXquery("select split_part(split_part(version(),' ',2),'.',1),version()",0);
   varAdd(GVAR_AUXVERSION,AUXgetString(0,0));
   varAdd(GVAR_AUXFULLVER,AUXgetString(0,1));
   AUXqueryEnd();
   rows=AUXquery("select setting from pg_settings where name='data_directory'",0);
   varAdd(GVAR_AUXPGDATA,AUXgetString(0,0));
   AUXqueryEnd();
   rows=AUXquery("select setting from pg_settings where name='cluster_name'",0);
   TRACE("GVAR_AUXNAME=%s\n",AUXgetString(0,0));
   varAdd(GVAR_AUXNAME,AUXgetString(0,0));
   AUXqueryEnd();
   TRACE("AUXILIARY Connected.\n");
   if (pdb != NULL && strcmp(pdb,VAR_UNSET_VALUE) != 0)     { varAdd(GVAR_AUXDB,pdb); } 
                                                       else { varAdd(GVAR_AUXDB,VAR_UNSET_VALUE); };

   if (pusr != NULL && strcmp(pusr,VAR_UNSET_VALUE) != 0)   { varAdd(GVAR_AUXUSER,pusr); } 
                                                       else { varAdd(GVAR_AUXUSER,VAR_UNSET_VALUE); };
                                                                          
   if (ppwd != NULL && strcmp(ppwd,VAR_UNSET_VALUE) != 0)   { varAdd(GVAR_AUXPWD,ppwd); } 
                                                       else { varAdd(GVAR_AUXPWD,VAR_UNSET_VALUE); };
                                                                          
   if (phst != NULL && strcmp(phst,VAR_UNSET_VALUE) != 0)   { varAdd(GVAR_AUXHOST,phst); } 
                                                       else { varAdd(GVAR_AUXHOST,VAR_UNSET_VALUE); };
                                                                          
   if (pport != NULL && strcmp(pport,VAR_UNSET_VALUE) != 0) { varAdd(GVAR_AUXPORT,pport); } 
                                                       else { varAdd(GVAR_AUXPORT,VAR_UNSET_VALUE); };
   return(true);
}

// return the number of rows
int AUXquery(const char *query,int with_retry)
{
   TRACE("ARGS: query='%s' with_retry=%d\n",query,with_retry);

   PGauxID++;
   if (PGauxID > PGresID_LIMIT) { printf("PGcauxID LIMIT reached(%d)\n",PGauxID); return(-1); };
   
   TRACE("curid=%d\n",PGauxID);

   int try_count=4;
   if (with_retry == 0) try_count=1;
   int finish=1;
   while (try_count > 0 && finish != 0)
   {
      AUXresult[PGauxID]=PQexec(aux_cnx,query);
      if (AUXresult[PGauxID] == NULL) { TRACE("QUERY ERROR ?\n"); return(-1); };
      ExecStatusType qry_rc=PQresultStatus(AUXresult[PGauxID]);
      switch (qry_rc)
      {
         case PGRES_COMMAND_OK : TRACE("PGRES_COMMAND_OK --> RETURN (int) 0\n");
                                 return(0);                                     // Return 0 : query success, but do not return any row
                                 break;
         case PGRES_TUPLES_OK  : TRACE("PGRES_TUPLES_OK --> RETURN (int) %d\n",PQntuples(AUXresult[PGauxID]));
                                 return(PQntuples(AUXresult[PGauxID]));                     // Return n : query success, return number of rows
                                 break;
         case PGRES_FATAL_ERROR: TRACE("PGRES_FATAL_ERROR\n");
                                 TRACE("Cluster connection error. Trying to reconnect[PGRES_FATAL_ERROR]...\n");
                                 try_count--;
                                 if (with_retry == 1)
                                 {
                                    sleep(10);
                                    AUXconnect(varGet(GVAR_AUXDB),
                                               varGet(GVAR_AUXUSER),
                                               varGet(GVAR_AUXPWD),
                                               varGet(GVAR_AUXHOST),
                                               varGet(GVAR_AUXPORT));
                                 };
                                 break;
         case PGRES_BAD_RESPONSE :
            TRACE("PGRES_BAD_RESPONSE\n");
            TRACE("%s",PQresStatus(qry_rc));
            TRACE("Cluster connection error. Trying to reconnect[PGRES_BAD_RESPONSE]...\n");
            try_count--;
            if (with_retry == 1)
            {
               sleep(10);
               AUXconnect(varGet(GVAR_AUXDB),
                          varGet(GVAR_AUXUSER),
                          varGet(GVAR_AUXPWD),
                          varGet(GVAR_AUXHOST),
                          varGet(GVAR_AUXPORT));
            }
            break;
         default:;
      }
   }
   TRACE(" --> RETURN (int) -1\n");
   return(-1);                                                                  // Return -1 : qquery failed
}




int pgStopAuxiliaryEngine(const char *AUXPGDATA,long AUXPORT)
{
   TRACE("ARGS: AUXPGDATA='%s' AUXPORT=%ld\n",AUXPGDATA,AUXPORT); 
   char *pgm_pgctl=malloc(1024);
   if (varIsSET(SYSVAR_PGBIN) == true) 
   { sprintf(pgm_pgctl,"%s/pg_ctl",varGet(SYSVAR_PGBIN)); }
   else 
   { strcpy(pgm_pgctl,pg_ctl_locate());
     char *px=strstr(pgm_pgctl,"/pg_ctl");
     if (px != NULL) { px[0]=0x00;                                              // For the first time, we update the PGBIN folder
                       varAdd(SYSVAR_PGBIN,pgm_pgctl);
                       globalArgs.configChanged=1;
                       SaveConfigFile(NULL,"default",SECTION_DEFAULT);
                     }
   };
   
   char *saved_PGDATA=strdup(varGet(GVAR_CLUPGDATA));                           // Save Current PGDATA variable and PORT
   long saved_PORT=varGetLong(GVAR_CLUPORT);
   varAdd    (GVAR_CLUPGDATA,AUXPGDATA);
   varAddLong(GVAR_CLUPORT,AUXPORT);
   TRACE("Set temporarily PGDATA to '%s'\n",varGet(GVAR_CLUPGDATA));
   TRACE("Set temporarily PORT to %s\n",    varGet(GVAR_CLUPORT));
   
   int pgctl_rc=pg_ctl(PGCTL_ACTION_STOP ,pgm_pgctl,PGCTL_OUTPUT);
   if (pgctl_rc == ERROR_PGCTL_FATAL) 
   {
      varAdd    (GVAR_CLUPGDATA,saved_PGDATA);                                  // Restore PGDATA variable and PORT
      varAddLong(GVAR_CLUPORT,saved_PORT);
      free(saved_PGDATA);
      free(pgm_pgctl);      
      TRACE("RETURN (int) False\n");
      return(false);
   }

   // Wait the server been stopped...
   int max_wait=20;
   while (max_wait > 0 && isUnixSocketPresent() == true) { printf(".");fflush(stdout);sleep(3);max_wait--; };
   if (max_wait < 20) printf("\n");

   varAdd    (GVAR_CLUPGDATA,saved_PGDATA);                                     // Restore PGDATA variable and PORT
   varAddLong(GVAR_CLUPORT,saved_PORT);
   free(saved_PGDATA);
   free(pgm_pgctl);
   TRACE("RETURN (int) True\n");
   return(true);
}

int pgStartAuxiliaryEngine(const char *AUXPGDATA,long AUXPORT)
{
   TRACE("ARGS: AUXPGDATA='%s' AUXPORT=%ld\n",AUXPGDATA,AUXPORT); 
   char *pgm_pgctl=malloc(1024);
   if (varIsSET(SYSVAR_PGBIN) == true) 
   { sprintf(pgm_pgctl,"%s/pg_ctl",varGet(SYSVAR_PGBIN)); }
   else 
   { strcpy(pgm_pgctl,pg_ctl_locate());
     char *px=strstr(pgm_pgctl,"/pg_ctl");
     if (px != NULL) { px[0]=0x00;                                              // For the first time, we update the PGBIN folder
                       varAdd(SYSVAR_PGBIN,pgm_pgctl);
                       globalArgs.configChanged=1;
                       SaveConfigFile(NULL,"default",SECTION_DEFAULT);
                     }
   };
   
   char *saved_PGDATA=NULL;
   long saved_PORT=0;
   if (clu_cnx != NULL)
   {
      saved_PGDATA=strdup(varGet(GVAR_CLUPGDATA));                           // Save Current PGDATA variable and PORT
      saved_PORT=varGetLong(GVAR_CLUPORT);
   }
   varAdd    (GVAR_CLUPGDATA,AUXPGDATA);
   varAddLong(GVAR_CLUPORT,AUXPORT);
   
   TRACE("Set temporarily PGDATA to '%s'\n",varGet(GVAR_CLUPGDATA));
   TRACE("Set temporarily PORT to %s\n",    varGet(GVAR_CLUPORT));
   
   int pgctl_rc=pg_ctl(PGCTL_ACTION_START ,pgm_pgctl,PGCTL_NO_OUTPUT);
   if (pgctl_rc == ERROR_PGCTL_FATAL) 
   {
      if (clu_cnx != NULL)
      {
         varAdd    (GVAR_CLUPGDATA,saved_PGDATA);                                  // Restore PGDATA variable and PORT
         varAddLong(GVAR_CLUPORT,saved_PORT);
         free(saved_PGDATA);
      }
      free(pgm_pgctl);      
      return(false);
   }

   // Wait the server been stopped...
   int max_wait=5;
   while (max_wait > 0 && isUnixSocketPresent() == false) { printf(".");fflush(stdout);sleep(3);max_wait--; };
   if (max_wait < 5) printf("\n");

   if (clu_cnx != NULL)
   {
      varAdd    (GVAR_CLUPGDATA,saved_PGDATA);                                     // Restore PGDATA variable and PORT
      varAddLong(GVAR_CLUPORT,saved_PORT);
      free(saved_PGDATA);
   };
   free(pgm_pgctl);
   return(true);
}


int pgStopEngine()
{
   CLUdisconnect();
   char *pgm_pgctl=malloc(1024);
   if (varIsSET(SYSVAR_PGBIN) == true)
   { sprintf(pgm_pgctl,"%s/pg_ctl",varGet(SYSVAR_PGBIN)); }
   else 
   { strcpy(pgm_pgctl,pg_ctl_locate());
     char *px=strstr(pgm_pgctl,"/pg_ctl");
     if (px != NULL) { px[0]=0x00;                                              // For the first time, we update the PGBIN folder
                       varAdd(SYSVAR_PGBIN,pgm_pgctl);
                       globalArgs.configChanged=1;
                       SaveConfigFile(NULL,"default",SECTION_DEFAULT);
                     }
   };
   TRACE("PGDATA is '%s'\n",varGet(GVAR_CLUPGDATA));
   TRACE("Stopping engine using '%s'\n",pgm_pgctl);
   int pgctl_rc=pg_ctl(PGCTL_ACTION_STOP ,pgm_pgctl,PGCTL_OUTPUT);
   if (pgctl_rc == ERROR_PGCTL_FATAL)
   {
      ERROR(ERR_STOPFAILED,"PostgreSql shutdown failed: %s\n",last_pg_ctl_message);
      return(false);
   }

   // Wait the server been stopped...
   int max_wait=5;
   while (max_wait > 0 && isUnixSocketPresent() == true) { printf(".");fflush(stdout);sleep(3);max_wait--; };
   if (max_wait < 5) printf("\n");
   free(pgm_pgctl);
   TRACE("Stopped successfully\n");
   return(true);
}

int pgStartEngine()
{
   char *pgm_pgctl=malloc(1024);
   if (varIsSET(SYSVAR_PGBIN) == true) 
   { sprintf(pgm_pgctl,"%s/pg_ctl",varGet(SYSVAR_PGBIN)); }
   else 
   { strcpy(pgm_pgctl,pg_ctl_locate());
     char *px=strstr(pgm_pgctl,"/pg_ctl");
     if (px != NULL) { px[0]=0x00;                                              // For the first time, we update the PGBIN folder
                       varAdd(SYSVAR_PGBIN,pgm_pgctl);
                       globalArgs.configChanged=1;
                       SaveConfigFile(NULL,"default",SECTION_DEFAULT);
                     }
   };
   TRACE("PGDATA is '%s'\n",varGet(GVAR_CLUPGDATA));
   TRACE("Starting engine using '%s'\n",pgm_pgctl);
   int pgctl_rc=pg_ctl(PGCTL_ACTION_START,pgm_pgctl,PGCTL_OUTPUT);
   if (pgctl_rc == ERROR_PGCTL_FATAL)
   {
      ERROR(ERR_STOPFAILED,"PostgreSql startup failed: %s\n",last_pg_ctl_message);
      return(false);
   }

   // Wait the server been stopped...
   int max_wait=5;
   while (max_wait > 0 && isUnixSocketPresent() == false) { printf(".");fflush(stdout);sleep(3);max_wait--; };
   if (max_wait < 5) printf("\n");
   free(pgm_pgctl);
   TRACE("Started successfully\n");
   return(true);
}

//********************************************************************************
// Change a configuration parameter
//********************************************************************************
int CLUsetParameter(const char *prmnam,const char *newvalue,int reload_conf)
{
   char *query=malloc(1024);
   sprintf(query,"select setting from pg_settings where name='%s'",prmnam);
   int rc=CLUquery(query,0);
   if (rc != 1)
   {
      ERROR(ERR_INVPRMNAME,"Bad parameter '%s'\n",prmnam);
      free(query);
      CLUqueryEnd();
      return(false);
   }
   if (strcmp(CLUgetString(0,0),newvalue) == 0)
   {
      INFO("New value is not different than the current one. Value not changed.\n");
      free(query);
      CLUqueryEnd();
      return(false);
   };
   TRACE("ARGS(prmnam='%s' newvalue='%s' reload_conf=%d)\n",prmnam,newvalue,reload_conf); 
   VERBOSE ("Changing parameter '%s' : '%s'->'%s'\n",prmnam,CLUgetString(0,0),newvalue);
   CLUqueryEnd();
   
   sprintf(query,"alter system set %s='%s'",
                 prmnam,
                 newvalue);
   rc=CLUquery(query,0);
   if (rc != 0)
   { 
      ERROR(ERR_INTERNALERROR,"(%s) Could not set parameter '%s'\n",IE_SETPRM,prmnam);
      free(query);
      CLUqueryEnd();
      return(false);
   }
   CLUqueryEnd();
   if (reload_conf == true)
   {
      rc=CLUquery("select pg_reload_conf()",0);   
      if (rc != 1)
      {
         ERROR(ERR_INTERNALERROR,"(%s) Reload conf failed.\n",IE_RELOADCONF);
      }
      CLUqueryEnd();
      VERBOSE("configuration reloaded.\n");
   }
   free(query);
   return(true);
}


// accepted values : 'full/days:n'
int validRetention(char *pbuffer)
{
   char *buffer=strdup(pbuffer);
   char *mode=buffer;
   char *count=strchr(buffer,':');
   if (count == NULL) return(false);
   count[0]=0x00;
   count++;
   int val=atoi(count);
   TRACE("ARGS(pbuffer '%s') : mode='%s' count=%d",pbuffer,mode,val);
   if (val <= 0)  {TRACENP(" -- RETURN (int) false [0]\n");return(false); }
   if (val > 365) {TRACENP(" -- RETURN (int) false [365]\n");return(false); }
   int isOK=false;
   strToLower(mode);
   if (strcmp(mode,"count")==0 && isOK==false) isOK=true; 
   if (strcmp(mode,"days")==0 && isOK==false) isOK=true; 
   TRACENP(" -- RETURN (int) %d\n",isOK);
   return(isOK);
}


void cluster_doSwitchWAL(int chkpt)
{
   TRACE("ARGS(chkpt %d)\n",chkpt);
   char *LSN=malloc(128);
   int rows;
   if (chkpt == true)
   {
      rows=CLUquery("CHECKPOINT",0);  // CHECKPOINT must be in upper case to work
      if (rows != 0)
      {
         ERROR(ERR_DBQRYERROR,"Checkpoint failed.\nError:%s",CLUgetErrorMessage()); return;
      };
   };
   rows=CLUquery("select pg_switch_wal()",0);
   if (rows != 1)
   {
      ERROR(ERR_DBQRYERROR,"Switch WAL failed.\nError:%s",CLUgetErrorMessage()); return;
   }; 
   strcpy(LSN,CLUgetString(0,0));
   VERBOSE("Invoking switch WAL (%s)\n",LSN);
   CLUqueryEnd();
   varAdd(SYSVAR_LASTLSN,LSN);
}

void cluster_getCurrentWAL()
{
   TRACE("ARGS: none\n");
   char *LSN=malloc(128);
   int rows=CLUquery("select pg_current_wal_lsn()",0);
   if (rows != 1)
   {
      ERROR(ERR_DBQRYERROR,"Switch WAL failed.\nError:%s",CLUgetErrorMessage()); return;
   }; 
   strcpy(LSN,CLUgetString(0,0));
   CLUqueryEnd();
   varAdd(SYSVAR_LASTLSN,LSN);
}

int IsClusterEnabled(long cluster_id)
{
   if (varExist(GVAR_DEPUSER) == false) return (false);

   char *query=malloc(1024);
   sprintf(query,"select clu_sts from %s.clusters where cid=%ld",
                 varGet(GVAR_DEPUSER),
                 cluster_id);
   int rows=DEPOquery(query,0);
   if (rows != 1)
   {
      DEPOqueryEnd();
      free(query);
      return(false);
   }
   char *state=strdup(DEPOgetString(0,0));
   DEPOqueryEnd();
   int rc=strcmp(state,CLUSTER_STATE_ENABLED);
   free(state);
   free(query);
   TRACE("ARGS(cluster_id %ld) -- RETURN (int) %d\n",cluster_id,rc); 
   return ( rc == 0);
} 

// Load all variables stored within DEPOSIT database 
void CLUloadDepositOptions(long cluster_id)
{
   char *query=malloc(1024);
   sprintf(query,"select opt_delwal,   "
                       " opt_blksize,  " 
                       " opt_compress, " 
                       " opt_retfull,  " 
                       " opt_retmeta,  " 
                       " opt_retcfg,   " 
                       " opt_maxsize,  "
                       " opt_maxfiles, "
                       " opt_datfmt,   "
                       " opt_waldir,   "
                       " opt_bkpdir,   "
                       " opt_concurindex,"
                       " clu_addr,"
                       " opt_maxwsize,  "
                       " opt_maxwfiles"
                 " from %s.clusters where cid=%ld",
                 varGet(GVAR_DEPUSER),cluster_id);
   int rows=DEPOquery(query,0);
   //printf("query=%s\nrc=%d\n",query,rows);
   if (rows == 1)
   {
      varAdd(GVAR_CLUAUTODELWAL,        DEPOgetString(0, 0));    // opt_delwal
      varAdd(GVAR_CLUREADBLOCKSIZE,     DEPOgetString(0, 1));  // opt_blksize
      varAdd(GVAR_COMPLEVEL,            DEPOgetString(0, 2));  // opt_compress
      varAdd(GVAR_CLURETENTION_FULL,    DEPOgetString(0, 3));  // opt_full
      varAdd(GVAR_CLURETENTION_META,    DEPOgetString(0, 4));  // opt_meta
      varAdd(GVAR_CLURETENTION_CONFIG,  DEPOgetString(0, 5));  // opt_cfg
      varAdd(GVAR_CLUBACKUP_MAXSIZE,    DEPOgetString(0, 6));  // opt_maxsize
      varAdd(GVAR_CLUBACKUP_MAXFILES,   DEPOgetString(0, 7));  // opt_maxfiles
      varAdd(GVAR_CLUDATE_FORMAT,       DEPOgetString(0, 8));  // opt_datfmt
      varAdd(GVAR_WALDIR,               DEPOgetString(0, 9));  // opt_waldir
      varAdd(GVAR_BACKUPDIR,            DEPOgetString(0,10));  // opt_bckdir
      varAdd(GVAR_REINDEXCONCURRENTLY,  DEPOgetString(0,11));  // opt_reindex
      varAdd(GVAR_CLUIP,                DEPOgetString(0,12));  // clu_addr
      varAdd(GVAR_CLUBACKUP_MAXWALSIZE, DEPOgetString(0,13));  // maxwalsize
      varAdd(GVAR_CLUBACKUP_MAXWALFILES,DEPOgetString(0,14));  // maxwalfiles
   }
   DEPOqueryEnd();
}

// archive_mode=on
// archive_command='??'
// wal_level=replica
int isClusterComplianceForRECM()
{
   int isCompliance=true;
   int rows=DEPOquery("select lower(setting) from pg_settings where name='wal_level'",0);
   if (strcmp(DEPOgetString(0,0),"replica") != 0) 
   {  isCompliance=false; 
      ERROR(ERR_BADLEVEL,"Invalid 'wal_level'. Must be 'replica'.\n");
   };
   DEPOqueryEnd();
   rows=DEPOquery("select lower(setting) from pg_settings where name='archive_mode'",0);
   if (strcmp(DEPOgetString(0,0),"on") != 0) 
   {  isCompliance=false; 
      ERROR(ERR_BADARCHMODE,"Parameter 'archive_mode' is 'off'.\n");
   };
   DEPOqueryEnd();

   rows=DEPOquery("select setting from pg_settings where name='archive_command'",0);
   char *archive_command=DEPOgetString(0,0);
   if (archive_command == NULL || strlen(archive_command) == 0 || strcmp(archive_command,"(disabled)") == 0) 
   {
      isCompliance=false; 
      ERROR(ERR_BADARCHCMD,"Parameter 'archive_command' is not set.\n");
   };
   DEPOqueryEnd();
   TRACE("ARGS(none) -- RETURN (int) %d\n",isCompliance);
   return(isCompliance);
}

int isConnectedAndRegistered()
{
   
   if (clu_cnx == NULL)
   {
      ERROR(ERR_NOTCONNECTED,"Not connected to any cluster.\n");
      return(false);
   };
   if (rep_cnx == NULL)
   {  
      ERROR(ERR_NOTCONNECTED,"Not connected to any deposit.\n");
      return(false);
   };
   if (varGetLong(GVAR_CLUCID) == 0 && isClusterComplianceForRECM() == false) 
   { 
      TRACE("ARGS(none) -- RETURN (int) false\n");
      return(false); 
   };
   if (varGetLong(GVAR_CLUCID) == 0)
   {
      ERROR(ERR_NOTREGISTERED,"Not registered to any deposit.\n");
      return(false);
   };
   TRACE("ARGS(none) -- RETURN (int) true\n");
   return(true);
}

//********************************************************************************
// Read current TIMELINE if engine is running, otherwise return -1
//********************************************************************************
long pgGetCurrentTimeline()
{
   TRACE("ARGS: none\n");
   long TL=-1;
   if (clu_cnx != NULL)
   {
      int rc_tl=CLUquery("SELECT timeline_id FROM pg_control_checkpoint()",0);
      if (rc_tl == 1) { TL=CLUgetLong(0,0); }
                 else { TL=-1; };
      CLUqueryEnd();
   }
   TRACE(" --> RETURN (long) %ld\n",TL);
   return(TL);
}


//********************************************************************************
// Extract Postgresql Version from pg_ctl program
//********************************************************************************
int pg_ctl_version(const char *pgctl)
{
   static char buf[1024];
   FILE *fp;
   char *cmd=malloc(1024);
   sprintf(cmd,"%s --version|awk '{print $3}'|cut -d. -f1",pgctl);
   if ((fp = popen(cmd, "r")) == NULL)
   {
      TRACE("ARGS(pgctl '%s') --> RETURN (int) -1\n",pgctl);
      return -1;
   }
   buf[0]=0x00;
   int stop=0;
   while (stop == 0 && fgets(buf, 1024, fp) != NULL) 
   {
      TRACE("pg_ctl_version='%s'\n",buf);
   }
   if (pclose(fp))
   {
      TRACE("ARGS(pgctl '%s') --> RETURN (int) -1 [PIPE]\n",pgctl);
      return -1;
   }
   int version=atoi(buf);
   TRACE("ARGS(pgctl '%s') --> RETURN (int) %d\n",pgctl,version);
   return version;
}


// help list backup
void COMMAND_HELP(int idcmd,char *command_line)
{
   char *subcommand=malloc(512);
   subcommand[0]=0x00;
   char *f=subcommand;
   char *p=(char *)command_line;
   int c=p[0];
   int pc=' ';
   while (c != 0x00)
   {
      if (c == ' ')
      {
         if (pc != ' ') { f[0]='-';f++;};
      }
      else { f[0]=c;f++; };
      p++;
      pc=c;
      c=p[0];
   }
   f[0]=0x00;
   f=strchr(subcommand,'-');
   if (f != NULL)
   {
      f++;
      int id=0;
      while (recm_commands[id].id != 0 )
      {
         if (strncmp(recm_commands[id].cmd,f,strlen(f)) == 0) showHelp(id,1);
         id++;
      }
   }
   else
   {
      int id=0;
      while (recm_commands[id].id != 0 )
      {
         showHelp(id,1);
         id++;
      }
   }
   free(subcommand);
}

// @show variables
void COMMAND_SHOWVARIABLE(int idcmd,char *command_line)
{
   if (qualifierIsUNSET("qal_name") == true)
   {
      varDump(1);
   }
   else
   {
      char *lsearch=malloc(128);      
      strcpy(lsearch,varGet("qal_name"));
      strToLower(lsearch);
      char *lvar=malloc(128);
      
      Variable *wrk=varFirst;
      while (wrk != NULL)
      {
         strcpy(lvar,wrk->nam);
         strToLower(lvar);
         if (strstr(lvar,lsearch) != NULL)
         {
            printf("\t %-20s='%s'\n",wrk->nam,wrk->val);
         };
         wrk=wrk->nxt;
      }
      free(lvar);
      free(lsearch);
   }
}

// SYS_DEBUG : @debug /on /off
void COMMAND_DEBUG(int idcmd,char *command_line)
{
   int done=0;
   
   if (optionIsSET("opt_on"))  { globalArgs.debug=true;  globalArgs.verbosity_bd=globalArgs.verbosity; done++; };                  // Keep verbosity state
   if (optionIsSET("opt_off")) { globalArgs.debug=false; globalArgs.verbosity=globalArgs.verbosity_bd; done++; };                  // Restore verbosity state
   if (done == 0) { printf ("debug is '%d'\n",globalArgs.debug); };
   if (qualifierIsUNSET("qal_file") == false && globalArgs.debug == true)
   {
      
      globalArgs.htrace=fopen(varGet("qal_file"), "a");
      if (globalArgs.htrace != NULL) 
      {
         
         struct timeval log_start;
         gettimeofday(&log_start, NULL);
         globalArgs.tracefile=strdup(varGet("qal_file"));
         fprintf(globalArgs.htrace,"*** RECM trace file **************************************************************************\n");
         fprintf(globalArgs.htrace,"File : %-40s            Date: %s\n",globalArgs.tracefile,displayTime(&log_start));
         fprintf(globalArgs.htrace,"host : %-40s PostgreSQL Name: %s (Deposit:%s)\n",varGet(SYSVAR_HOSTNAME),varGet(GVAR_CLUNAME),varGet(GVAR_DEPNAME));
         fprintf(globalArgs.htrace,"**********************************************************************************************\n");
      }
      else
      {
         INFO("Coud not create file '%s' (rc=%d)\n",varGet("qal_file"),errno);
      }
   }
   else
   {
      if (globalArgs.htrace != NULL) fclose(globalArgs.htrace);
      globalArgs.htrace=NULL;
      if (globalArgs.tracefile != NULL) free(globalArgs.tracefile);
      globalArgs.tracefile=NULL;
   }
}

//Command CMD_DISCONAUX
void COMMAND_DISCONNECT_AUXILIARY(int idcmd,char *command_line)
{
   AUXdisconnect();
}

//Command CMD_DISCONCLU
void COMMAND_DISCONNECT_CLUSTER(int idcmd,char *command_line)
{
   CLUdisconnect();
}

//Command CMD_DISCONDEPO
void COMMAND_DISCONNECT_DEPOSIT(int idcmd,char *command_line)
{
   DEPOdisconnect();
}

//Command CMD_CLRSCR
void COMMAND_CLRSCR(int idcmd,char *command_line)
{
   system("clear");
}

#include "recm_zip.h"

// Command CMD_SQL
#include "recm_sql.h"

//Command CMD_SHOWBACKUP
#include "recm_showbackup.h"

#include "recm_config.h"

// Command CMD_LISTRP
#include "recm_listrp.h"

//Command CMD_LISTWAL
#include "recm_listwal.h"

// Command CMD_RESTOREWAL
#include "recm_restorewal.h"

// Command CMD_RESTORECFG
#include "recm_restorecfg.h"

// Command CMD_RESTOREMETA
#include "recm_restoremeta.h"

// Command CMD_RESTOREFULL
//#include "recm_restorefull.h"

// Command CMD_DUPLICATEPARTIAL
#include "recm_duplicatepartial.h"
      
//Command CMD_BACKUPCFG
#include "recm_listbackup.h"

//Command CMD_DELETECLUSTER
#include "recm_deletecluster.h"

//Command CMD_LISTCLUSTER
#include "recm_listcluster.h"

//Command CMD_MODIFYCLUSTER
#include "recm_modifycluster.h"

//Command CMD_MODIFYBACKUP
#include "recm_modifybackup.h"

//Command CMD_DELETEBACKUP
#include "recm_deletebackup.h"

//Command CMD_BACKUPCFG
#include "recm_backupcfg.h"

//Command CMD_BACKUPWAL
#include "recm_backupwal.h"

//Command COMMAND_CANCELBACKUP
#include "recm_cancelbackup.h"

//Command CMD_STATCLUSTER
#include "recm_statcluster.h"

//Command CMD_STATDEPOSIT
#include "recm_statdeposit.h"

//Command CMD_SHOWCONFIG,CMD_SETCONFIG
#include "recm_showconfig.h"

// Command CMD_CREATERP
#include "recm_createrp.h"

// Command CMD_DELETERP
#include "recm_deleterp.h"

// Comman CMD_DUPLICATEFULL
//#include "recm_duplicate.h"

#include "recm_cnxclu.h"

#include "recm_cnxdepo.h"

#include "recm_usage.h"

//Command CMD_SHOWDEPOSIT,
#include "recm_showdepo.h"

// Command CMD_CREATEDEPOSIT
#include "recm_credeposit.h"

// Command CMD_UPGRADEDEPOSIT
#include "recm_upgdeposit.h"

// Command CMD_SHOWCLUSTER
#include "recm_showcluster.h"

//Command COMMAND_VERIFYBACKUP
#include "recm_verifybackup.h"

//Command CMD_BACKUPFULL
#include "recm_backupfull.h"

//Command CMD_BACKUPMETA
#include "recm_backupmeta.h"

//Command CMD_SWITCHWAL
#include "recm_switchwal.h"

//Command CMD_LISTMAPPING
#include "recm_listmapping.h"

//Command CMD_DELMAPPING
#include "recm_delmapping.h"

//Command CMD_REGFILE
#include "recm_regfiles.h"
#include "recm_expcfg.h"
#include "recm_regclu.h"
#include "recm_reload.h"
#include "recm_save.h"
#include "recm_setsource.h"
#include "recm_setmapping.h"

void CLUloadData()
{
   TRACE("ARGS: none\n"); 
   char *res=malloc(1024);
   int rows;
   if (varIsSET(SYSVAR_PGBIN) == false)
   {
      rows=CLUquery("select setting from pg_config where name='BINDIR'",0);
      varAdd(SYSVAR_PGBIN,CLUgetString(0,0));
      globalArgs.configChanged=1;
      SaveConfigFile(NULL,"default",SECTION_DEFAULT);
      CLUqueryEnd();
      //globalArgs.configChanged=1;
   };
   if (varExist(GVAR_SOCKPATH) == false) varAdd(GVAR_SOCKPATH,VAR_UNSET_VALUE);
   if (strcmp(varGet(GVAR_SOCKPATH),VAR_UNSET_VALUE)==0)
   {
      rows=CLUquery("select setting from pg_settings where name='unix_socket_directories'",0);
      varAdd(GVAR_SOCKPATH,CLUgetString(0,0));
      CLUqueryEnd();
      globalArgs.configChanged=1;
   };
   if (varExist(GVAR_WALDIR) == false) varAdd(GVAR_WALDIR,VAR_UNSET_VALUE);
   if (varExist(GVAR_WALDIR) == false || strcmp(varGet(GVAR_WALDIR),VAR_UNSET_VALUE)==0)
   {
      rows=CLUquery("select setting from pg_settings where name='archive_command'",0);
      char *archive_command=DEPOgetString(0,0);
      CLUqueryEnd();
      if (archive_command != NULL && strlen(archive_command)>= 0 && strcmp(archive_command,"(disabled)") != 0) { varAdd(GVAR_WALDIR,pgExtractWalDestination(archive_command)); }
                                                                                                          else { varAdd(GVAR_WALDIR,""); };
      globalArgs.configChanged=1;
   };
   
   free(res);
   SaveConfigFile("CLU",varGet(GVAR_CLUNAME),SECTION_CLUSTER);
}

//CMD_STATUS
void COMMAND_STATUS(int idcmd,const char *command_line)
{
   char DEPO_version[20];
   memset(DEPO_version,0x00,20);
   char flg[10];
   if (IsClusterEnabled(varGetLong(GVAR_CLUCID)) == false) { strcpy(flg,"DISABLED");} 
                                                      else { strcpy(flg,"ENABLED"); };
   if (CLUisConnected(false))  
   {
       printf ("Cluster: CONNECTED  Name='%s'  CID=%ld  state=%s\n",varGet(GVAR_CLUNAME),varGetLong(GVAR_CLUCID),flg); 
       printf ("\tVersion : %s\n",varGet(RECM_VERSION));
   
   }
   else 
   {
      printf ("Cluster: NOT CONNECTED.\n");
   };
   if (DEPOisConnected(false))
   { 
      printf ("Deposit: CONNECTED  Name='%s'\n",varGet(GVAR_REGDEPO));
      char *query=malloc(1024);
      sprintf(query,"select version,description,to_char(created,'%s'),to_char(updated,'%s') from %s.config",
                    varGet(GVAR_CLUDATE_FORMAT),
                    varGet(GVAR_CLUDATE_FORMAT),
                    varGet(GVAR_DEPUSER));
      int rows=DEPOquery(query,0);
      TRACE("[rc=%d]\n",rows);
      
      strcpy(DEPO_version,DEPOgetString(0,0));
      printf("\tVersion : %s\t( %s )\n",DEPO_version,DEPOgetString(0,1));
      printf("\tLast update '%s' ( created '%s')\n",DEPOgetString(0,3),DEPOgetString(0,2));
      DEPOqueryEnd();
      free(query);      
   }
   else
   {
      printf ("Deposit: NOT CONNECTED.\n");
   };
   if (strcmp(DEPO_version,CURRENT_VERSION) != 0)
   {
      ERROR(ERR_BADDEPOVER,"DEPOSIT is not at same version as RECM is.  Invoke 'upgrade' to upgrade your deposit\n");
   }
/*
   char *wrkstr;
   int first=0;
   wrkstr=getenv(ENVIRONMENT_RECM_META_DIR);
   if (wrkstr != NULL) { if (first==0) {printf ("Environment variables:\n"); first++;};printf ("\t- RECM_METADIR='%s'\n",wrkstr);};
   wrkstr=getenv(ENVIRONMENT_RECM_CONFIG    );
   if (wrkstr != NULL) { if (first==0) {printf ("Environment variables:\n"); first++;};printf ("\t- RECM_CONFIG='%s'\n",wrkstr);};
   wrkstr=getenv(ENVIRONMENT_RECM_PGBIN                 );
   if (wrkstr != NULL) { if (first==0) {printf ("Environment variables:\n"); first++;};printf ("\t- RECM_PGBIN='%s'\n",wrkstr);};
   wrkstr=getenv(ENVIRONMENT_RECM_PGBIN           );
   if (wrkstr != NULL) { if (first==0) {printf ("Environment variables:\n"); first++;};printf ("\t- RECM_METADIR='%s'\n",wrkstr);};
   wrkstr=getenv(ENVIRONMENT_RECM_WALCOMP_EXT     );
   if (wrkstr != NULL) { if (first==0) {printf ("Environment variables:\n"); first++;};printf ("\t- RECM_WALC_EXT='%s'\n",wrkstr);};
   wrkstr=getenv(ENVIRONMENT_RECM_RESTORE_COMMAND );
   if (wrkstr != NULL) { if (first==0) {printf ("Environment variables:\n"); first++;};printf ("\t- RECM_RESTORE_COMMAND='%s'\n",wrkstr);};
   wrkstr=getenv(ENVIRONMENT_RECM_COMPRESS_METHOD );
   if (wrkstr != NULL) { if (first==0) {printf ("Environment variables:\n"); first++;};printf ("\t- RECM_COMPRESS_METHOD='%s'\n",wrkstr);};
*/   
}

void COMMAND_CONNECTTARGET(int idcmd,const char *command_line)
{
       // Option '-t' is set. Try to connect to saved cluster name 
   if (LoadConfigFile("CLU",varGet("qal_name")) == ERR_SECNOTFND)
   {
      ERROR(ERR_SECNOTFND,"Cluster '%s' not known.\n",varGet("qal_name"));
      DisplaySavedClusters("CLU");
   }
   else
   {
      if (CLUconnect(varGet(GVAR_CLUDB),
                     varGet(GVAR_CLUUSER),
                     varGet(GVAR_CLUPWD),
                     varGet(GVAR_CLUHOST),
                     varGet(GVAR_CLUPORT)) == true)
      {
         CLUloadData();
         if (qualifierIsUNSET(GVAR_REGDEPO)== true && varGetLong(GVAR_CLUCID) > 0)
         {
            ERROR(ERR_NOTREGISTERED,"Unset deposit. Use 'register cluster/force=cid=%ld' command to restore registration configuration.\n",varGetLong(GVAR_CLUCID));
            varAdd(GVAR_CLUCID,"0");
            return;
         }
         if (varGetLong(GVAR_CLUCID) != 0)
         {
            TRACE("CID=%s\n",varGet(GVAR_CLUCID));
            LoadConfigFile("DEPO",varGet(GVAR_REGDEPO));
            if (DEPOconnect(varGet(GVAR_DEPDB),
                            varGet(GVAR_DEPUSER),
                            varGet(GVAR_DEPPWD),
                            varGet(GVAR_DEPHOST),
                            varGet(GVAR_DEPPORT)) == true)
            {
               DEPOCheckState();
               CLUloadDepositOptions(varGetLong(GVAR_CLUCID));
            }
         }
         else
         {
            ERROR(ERR_INVALIDCID,"Invalid cluster ID (CID) %ld\n",varGetLong(GVAR_CLUCID));
            CLUdisconnect();
         }
      }
   }
}

void processCommandLine(char *command_line)
{
//    TRACE("** SART Args: command_line='%s'\n",command_line); 
    if (checkAllQuotes(command_line) == false) { return; };
    int nr=1;
    char *cmdl=splitLines(command_line,nr);
    while (cmdl != NULL && nr < 80 && globalArgs.exitRequest == false)
    {
//       TRACE("splitline %d : '%s'\nglobalArgs.exitRequest=%d",nr,cmdl,globalArgs.exitRequest);
       if (strlen(cmdl) > 0)
       {
          int idcmd=extract_command(cmdl);
          if (idcmd > -1)
          {
             int saved_verbose=globalArgs.verbosity;                                                                               // Save verbosity before calling command
             switch(recm_commands[idcmd].id)
             {
                case CMD_QUIT            : ;
                case CMD_EXIT            : globalArgs.exitRequest=true;
                                           hstSave();
                                           break;
                case CMD_SETSOURCE       : COMMAND_SETSOURCE            (idcmd,command_line);break;
                case CMD_SETMAPPING      : COMMAND_SETMAPPING           (idcmd,command_line);break;
                case CMD_LISTMAPPING     : COMMAND_LISTMAPPING          (idcmd,command_line);break;
                case CMD_DELMAPPING      : COMMAND_DELMAPPING           (idcmd,command_line);break;
                case CMD_HELP            : COMMAND_HELP                 (idcmd,cmdl);break;
                case CMD_LISTBACKUP      : COMMAND_LISTBACKUP           (idcmd,command_line);break;
                case CMD_LISTCLUSTER     : COMMAND_LISTCLUSTER          (idcmd,command_line);break;
                case CMD_MODIFYCLUSTER   : COMMAND_MODIFYCLUSTER        (idcmd,command_line);break;
                case CMD_MODIFYBACKUP    : COMMAND_MODIFYBACKUP         (idcmd,command_line);break;
                case CMD_DELETECLUSTER   : COMMAND_DELETECLUSTER        (idcmd,command_line);break;
                case CMD_LISTRP          : COMMAND_LISTRP               (idcmd,command_line);break;   
                case CMD_LISTWAL         : COMMAND_LISTWAL              (idcmd,command_line);break;   
                case CMD_BACKUPWAL       : COMMAND_BACKUPWAL            (idcmd,command_line);break;
                case CMD_BACKUPCFG       : COMMAND_BACKUPCFG            (idcmd,command_line);break;
                case CMD_BACKUPFULL      : COMMAND_BACKUPFULL           (idcmd,command_line);break;
                case CMD_BACKUPMETA      : COMMAND_BACKUPMETA           (idcmd,command_line);break;
                case CMD_DELETEBACKUP    : COMMAND_DELETEBACKUP         (idcmd,command_line);break;
                case CMD_DELETERP        : COMMAND_DELETERP             (idcmd,command_line);break;
                case CMD_DISCONAUX       : COMMAND_DISCONNECT_AUXILIARY (idcmd,command_line);break;
                case CMD_DISCONCLU       : COMMAND_DISCONNECT_CLUSTER   (idcmd,command_line);break;
                case CMD_DISCONDEPO      : COMMAND_DISCONNECT_DEPOSIT   (idcmd,command_line);break;
                case CMD_DUPLICATEFULL   : COMMAND_DUPLICATEPARTIAL     (3,idcmd,command_line);break;
                case CMD_DUPLICATEPARTIAL: COMMAND_DUPLICATEPARTIAL     (2,idcmd,command_line);break;
                case CMD_CANCELBACKUP    : COMMAND_CANCELBACKUP         (idcmd,command_line);break;
                case CMD_CLRSCR          : COMMAND_CLRSCR               (idcmd,command_line);break;
                case CMD_SQL             : COMMAND_SQL                  (idcmd,command_line);break;   
                case CMD_CREATEDEPOSIT   : COMMAND_CREATEDEPOSIT        (idcmd,command_line);break;
                case CMD_UPGRADEDEPOSIT  : COMMAND_UPGRADEDEPOSIT       (idcmd,command_line);break;
                case CMD_CREATERP        : COMMAND_CREATERP             (idcmd,command_line);break;
                case CMD_CNXCLUSTER      : COMMAND_CNXCLUSTER           (idcmd,command_line);break;
                case CMD_CNXDEPOSIT      : COMMAND_CNXDEPOSIT           (idcmd,command_line);break;
                case CMD_EXPCFG          : COMMAND_EXPORTCONFIG         (idcmd,command_line);break;
                case CMD_REGCLUSTER      : COMMAND_REGCLUSTER           (idcmd,command_line);break;
                case CMD_HISTORY         : hstCmd_list();break;
                case CMD_REGFILES        : COMMAND_REGFILES             (idcmd,command_line);break;
                case CMD_RELOAD          : COMMAND_RELOAD               (idcmd,command_line);break;
                case CMD_RESTOREFULL     : COMMAND_DUPLICATEPARTIAL     (1,idcmd,command_line);break;
                case CMD_RESTOREPARTIAL  : COMMAND_DUPLICATEPARTIAL     (1,idcmd,command_line);break;
                case CMD_RESTOREWAL      : COMMAND_RESTOREWAL           (idcmd,command_line);break;
                case CMD_RESTORECFG      : COMMAND_RESTORECFG           (idcmd,command_line);break;
                case CMD_RESTOREMETA     : COMMAND_RESTOREMETA          (idcmd,command_line);break;
                case CMD_SAVE            : COMMAND_SAVE                 (idcmd,command_line);break;
                case CMD_SETCONFIG       : COMMAND_SETCONFIG            (idcmd,command_line);break;
                case CMD_MODIFYDEPOSIT   : COMMAND_MODIFYDEPOSIT        (idcmd,command_line);break;
                case CMD_SHOWBACKUP      : COMMAND_SHOWBACKUP           (idcmd,command_line);break;
                case CMD_SHOWCLUSTER     : COMMAND_SHOWCLUSTER          (idcmd,command_line);break;
                case CMD_SHOWCONFIG      : COMMAND_SHOWCONFIG           (idcmd,command_line);break;
                case CMD_SHOWDEPOSIT     : COMMAND_SHOWDEPOSIT          (idcmd,command_line);break;
                case SYS_SHOWVARIABLE    : COMMAND_SHOWVARIABLE         (idcmd,command_line);break;
                case SYS_DEBUG           : COMMAND_DEBUG                (idcmd,command_line);break;
                case CMD_STATCLUSTER     : COMMAND_STATCLUSTER          (idcmd,command_line);break;
                case CMD_STATDEPOSIT     : COMMAND_STATDEPOSIT          (idcmd,command_line);break;
                case CMD_SWITCHWAL       : COMMAND_SWITCHWAL            (idcmd,command_line);break;
                case CMD_VERIFYBACKUP    : COMMAND_VERIFYBACKUP         (idcmd,command_line);break;
                case CMD_CNXTARGET       : COMMAND_CONNECTTARGET        (idcmd,command_line);break;
                case CMD_STATUS          : COMMAND_STATUS               (idcmd,command_line);break;
                default:
                   ERROR(ERR_BADCOMMAND,"Invalid/Incomplete command '%s'\n",command_line);
             }
             if (recm_commands[idcmd].id != SYS_DEBUG)
             {
                globalArgs.verbosity=saved_verbose;                                                                                // Restore verbosity as it was before command
             };
          }
          else
          {
             if (strlen(cmdl) > 0 && idcmd > -2)
             {
                ERROR(ERR_BADCOMMAND,"Invalid/Incomplete command '%s'\n",cmdl);
             };
          }
       }
       nr++;
       cmdl=splitLines(command_line,nr);
    }
}
  
static struct option long_options[] = {
        {"help",      no_argument,       0,  'h' },
        {"version",   no_argument,       0,  'V' },
        {"verbose",   no_argument,       0,  'v' },
        {"command",   required_argument, 0,  'c' },
        {"target",    required_argument, 0,  't' },
        {"deposit",   required_argument, 0,  'd' },
        {0,           0,                 0,    0 }
    };


int main( int argc, char *argv[] )
{
   // Main initialization
   memInit();
#ifdef use_libzip_18
   char* RECM_COMPRESS_METHOD=getenv (ENVIRONMENT_RECM_COMPRESS_METHOD);
   if (RECM_COMPRESS_METHOD != NULL)
   {
      if (strstr(RECM_COMPRESS_METHOD,"STORE")   != NULL) zip_method=ZIP_CM_STORE;   // Store the file uncompressed
      if (strstr(RECM_COMPRESS_METHOD,"BZIP2")   != NULL) zip_method=ZIP_CM_BZIP2;   // Compress the file using the bzip2
      if (strstr(RECM_COMPRESS_METHOD,"DEFLATE") != NULL) zip_method=ZIP_CM_DEFLATE; // Deflate the file with the zlib(3) algorithm
      if (strstr(RECM_COMPRESS_METHOD,"XZ")      != NULL) zip_method=ZIP_CM_XZ;      // Use the xz(1) algorithm
      if (strstr(RECM_COMPRESS_METHOD,"LZMA")      != NULL) zip_method=ZIP_CM_LZMA;
      int rc=zip_compression_method_supported(zip_method,1);
      if (rc == 1) INFO("Method '%s' supported (rc=%d).\n",RECM_COMPRESS_METHOD,rc); 
   };
#endif
    varFirst=NULL;
    varLast=NULL;
    globalArgs.exitRequest = false;
    globalArgs.exitOnError = false;
    globalArgs.debug=false;
    globalArgs.htrace=NULL;
    globalArgs.tracefile=NULL;
    
    historic=arrayCreate(ARRAY_NO_ORDER);

    // Predefined/Initialized variables
    varAdd(RECM_VERSION,CURRENT_VERSION);
    varAdd(SYSVAR_DATE_FORMAT,"YYYY-MM-DD HH24:MI:SS");
    varAdd(GVAR_CLUDATE_FORMAT,"YYYY-MM-DD HH24:MI:SS");
    varAdd(SYSVAR_HISTDEEP,"100");
    varAdd(SYSVAR_PROMPT,"RECM");
    varAdd(SYSVAR_TARGET,     VAR_UNSET_VALUE);
    varAdd(SYSVAR_DEPOSIT,    VAR_UNSET_VALUE);
    varAdd("@command",VAR_UNSET_VALUE);
    varAdd(GVAR_COMPLEVEL,"-1");
    varAdd(GVAR_CLUMASK_BACKUP,"rwx");
    varAdd(GVAR_CLUMASK_RESTORE,"rwx");
    varAdd(SYSVAR_READBLOCKSIZE,"16384000");
    varAdd(SYSVAR_AUTODELWAL,   "no");
    varAdd(SYSVAR_DELAYDELWAL,  "1440");          // 24h by default
    varAdd(SYSVAR_RETENTION_FULL,  "count:2");    // Keep 2 backups of FULL by default   
    varAdd(SYSVAR_RETENTION_CONFIG,"count:7");    // Keep 7 backups of CONFIG by default
    varAdd(GVAR_SOCKPATH,VAR_UNSET_VALUE);        // Unix socket Path
    varAdd(GVAR_PGCTL,VAR_UNSET_VALUE);           // pg_ctl location
    varAdd(SYSVAR_PGBIN,VAR_UNSET_VALUE);         // pg_ctl location
    varAdd(SYSVAR_BACKUP_MAXFILES,"200");
    varAdd(SYSVAR_BACKUP_CFGFILES,"");
    varAdd(SYSVAR_BACKUP_EXCLDIR,"");
    TRACE("LoadConfigFile\n");
    LoadConfigFile(NULL,"default");
    TRACE("hstLoad\n");
    hstLoad();
    
    char *hostname=malloc(256); 
    gethostname(hostname, 256); 
    varAdd(SYSVAR_HOSTNAME,hostname);
    free(hostname);

//    char *pgdata=getenv("PGDATA");if (pgdata != NULL) { varAdd("@PGDATA",pgdata);

    signal(SIGINT, intHandler);
    atexit(exitHandler);

    struct termios info;
    tcgetattr(STDOUT_FILENO, &info);          /* get current terminal attirbutes; 0 is the file descriptor for stdin */
    info.c_lflag &= ~ICANON;                  /* disable canonical mode */
    info.c_cc[VMIN] = 1;                      /* wait until at least one keystroke available */
    info.c_cc[VTIME] = 0;                     /* no timeout */
    tcsetattr(STDOUT_FILENO, TCSANOW, &info); /* set immediately */
    tcgetattr(STDOUT_FILENO, &info);          /* get current terminal attirbutes; 0 is the file descriptor for stdin */
    info.c_lflag &= ~ECHO;                    /* disable echo */
    info.c_cc[VMIN] = 1;                      /* wait until at least one keystroke available */
    info.c_cc[VTIME] = 0;                     /* no timeout */
    tcsetattr(STDOUT_FILENO, TCSANOW, &info); /* set immediately */
    int opt = 0;
    int index=0;
    TRACE("ARGC=%d\n",argc);
    while ((opt = getopt_long(argc, argv,"Vvwt:c:hd:",long_options, &index )) != -1)
    {
        TRACE("opt=%d(%c) value='%s'\n",opt,opt,optarg);
        switch( opt )
        { 
            case 'V': display_version();                                        // Display version and exit
                      exit(0);
                      break;
            case 'v': globalArgs.verbosity = true;                              // Enable verbose mode
                      break;
            case 't': varAdd(SYSVAR_TARGET,optarg);                             // connect target (Cluster and it's deposit)
                      break;
            case 'd': varAdd(SYSVAR_DEPOSIT,optarg);                            // Connect to a deposit only
                      break;
            case 'c': varAdd("@command",optarg);                                // Execute a command from the command line          
                      break;
            case 'h':
                display_usage();
                break;
            default:
                /* You won't actually get here. */
                break;
        }
    };
    
    // Option '-t' is set. Try to connect to saved cluster name 
    if (strcmp(varGet(SYSVAR_TARGET),VAR_UNSET_VALUE) != 0)
    {
       if (LoadConfigFile("CLU",varGet(SYSVAR_TARGET)) == ERR_SECNOTFND)
       {
          ERROR(ERR_SECNOTFND,"Target cluster '%s' not defined. You cannot connect to a cluster remotely.\n",varGet(SYSVAR_TARGET));
       }
       else
       {
          if (CLUconnect(varGet(GVAR_CLUDB),
                         varGet(GVAR_CLUUSER),
                         varGet(GVAR_CLUPWD),
                         varGet(GVAR_CLUHOST),
                         varGet(GVAR_CLUPORT)) == true)
          {
             CLUloadData();
             if (varGetLong(GVAR_CLUCID) != 0)
             {
                LoadConfigFile("DEPO",varGet(GVAR_REGDEPO));
                if (DEPOconnect(varGet(GVAR_DEPDB),
                                varGet(GVAR_DEPUSER),
                                varGet(GVAR_DEPPWD),
                                varGet(GVAR_DEPHOST),
                                varGet(GVAR_DEPPORT)) == true)
                {
                   DEPOCheckState();
                   CLUloadDepositOptions(varGetLong(GVAR_CLUCID));
               }
             }
          }
       }
    }
    else
    {
       if (strcmp(varGet(SYSVAR_DEPOSIT),VAR_UNSET_VALUE) != 0)
       {
          LoadConfigFile("DEPO",varGet(SYSVAR_DEPOSIT));
          if (DEPOconnect(varGet(GVAR_DEPDB),
                          varGet(GVAR_DEPUSER),
                          varGet(GVAR_DEPPWD),
                          varGet(GVAR_DEPHOST),
                          varGet(GVAR_DEPPORT)) == true)
          {
             INFO("Connected to deposit '%s'\n",varGet(SYSVAR_DEPOSIT));
             varAdd(GVAR_CLUCID,"0"); // deposit only mode (for duplicate )
             DEPOCheckState();
          }
       }
    }
    
    if (strcmp(varGet("@command"),VAR_UNSET_VALUE) != 0)
    {
       globalArgs.exitOnError = true;                                           // Exit on any ERROR
       char * line = malloc(1024);
       sprintf(line,"%s;",varGet("@command"));
       processCommandLine(line);
       free(line);
       exit(0);
    }
    index=optind;
    if (index < argc) 
    { 
       globalArgs.exitOnError = true;                                           // Exit on any ERROR
       varAdd(SYSVAR_CMDFILE,argv[index]);
       VERBOSE("Executing file '%s'\n",varGet(SYSVAR_CMDFILE));
       FILE *fh=fopen(varGet(SYSVAR_CMDFILE), "r");
       if (fh == NULL) { ERROR(ERR_FILNOTFND,"File '%s' not found.\n",varGet(SYSVAR_CMDFILE));
                         exit(1);
                       };
       char * line = malloc(1024);
       while (fgets(line, 1023, fh) != NULL && globalArgs.exitRequest == false)
       {
          if (strcmp(line,"\n") == 0) continue;
          if (strchr(line,'#') == line) continue;
          processCommandLine(line);
       }
       fclose(fh);
       free(line);
    }
    else
    {
       globalArgs.exitOnError = false;                                          // In interactive mode disable 'Exit on ERROR'
       char *command_line=NULL;
       char *command=malloc(2048);
       while (globalArgs.exitRequest == false)
       {
          command_line=readFromKeyboard(varGet(SYSVAR_PROMPT));
          TRACE("COMMAND:%s\n",command_line);
          if (strlen(command_line) > 0) 
          {
             strcpy(command,command_line);
             processCommandLine(command);
          }
       }
    }
    exit(0);

}
