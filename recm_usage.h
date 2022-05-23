/********************************************************************************/
/* Display the Header                                                           */
/********************************************************************************/
void display_version( void )
{
   printf("recm (PostgreSQL) %s\n",CURRENT_VERSION);
}

void display_header( void )
{
printf("Recover Manager utility for PostgreSQL\n");
}

/********************************************************************************/
/* Display Usage                                                                */
/********************************************************************************/
void display_usage( void )
{
   display_header();
   display_version();
   printf("\n");
   printf("Usage:\n");
   printf("  recm [OPTIONS]... [@filename]\n");
   printf("\n");
   printf("Options:\n");
   printf("  -V, --version            output version information, then exit.\n");
   printf("  -v, --verbose            increase the output for tracing.\n");
   printf("  -c, --command            execute the command and exit.\n");
   printf("  -h, --help               show this help, then exit.\n");
   printf("  -t, --target=name        Name of the server name.\n");
   printf("  -d, --deposit=name       Name of the deposit.\n");
   printf("Parameters:\n");
   printf("  @filename                script to execute.\n");
   printf("\n");
   printf("For more information, type \"\?\" (for internal commands), or consult the recm documentation.\n");
   printf("Report bugs to <recm-bugs@postgresql.org>.\n");
   exit( ERR_SUCCESS );
}
