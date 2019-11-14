#!/bin/bash

#-------------------------------------------------------------------------------
#  Filename:    aster_maint_sessions_users_history.sh
#
#  Description: This script captures all new rows in the
#               nc_system.nc_all_sessions and nc_system.nc_all_users tables and
#               merges them into the tables:
#                   dba_maint.aster_sessions_history
#                   dba_maint.aster_users_history
#
#               These 2 tables need to be captured at the same time because of
#               dependencies.
#
#               NOTE:  The the aster_users_history table has additional columns
#                      for security tracking:
#
#                      first_captured_time   - The timestamp that the username
#                                              was first captured from the
#                                              nc_all_users table.
#                      last_activity_time    - The timestamp that the username
#                                              was last involved in a session.
#                      delete_time           - The timestamp that this user was
#                                              was dropped from the database.
#                      connect_revoked_time  - The timestamp that connect to
#                                              the database was revoked.
#                      exempt_activity_flag  - A T/F flag to determine if this
#                                              userid is exempt from inactivity
#                                              processing.
#                      security_unique_id    - An id assigned by the security
#                                              team to a user.
#                      email_address         - The email address for the user.
#                      full_username         - The full user's name.
#
#               This is done using the following steps:
#
#               1)  Get the latest startime in the aster_sessions_history table
#                     as the max_starttime.
#               2)  Extract all sessions from the nc_all_sessions table
#                     where the starttime is greater than the max_starttime and
#                     less than now() which is the current time.  This uses
#                     the SQL-MR sysextract function.
#               3)  Perform a merge statement to merge the data in the
#                     aster_sessions_new table into the aster_sessions_history
#                     table.
#               4)  Extract all users from the nc_all_users table into
#                     a aster_users_new table using the sysextract SQL-MR
#                     function.
#               5)  Perform a merge statement to merge the data in the
#                     aster_users_new table into the aster_users_history
#                     table.
#               6)  Set the last_activity_time, delete_time, and
#                     connect_revoked_time according to rules.
#               7)  Vacuum the aster_users_history table.
#
#               NOTE: This script is assumed to be executed using an Aster DB
#                     account with the db_admin role.  A script that is sourced
#                     before this script, could be called to set environment
#                     variables as needed.
#
#               NOTE: It is presumed that this script resides on the Aster
#                     cluster queen node.
#
#  Usage:       ./aster_maint_sessions_users_history.sh [-v]
#
#  Arguments:   -v   - Run script in verbose mode.  Show actions and execute
#                      them.
#
#  Outputs:     Insert rows into the following tables:
#                  dba_maint.aster_sessions_history
#                  dba_maint.aster_users_history
#
#  To Install:  1) Optionally install td_wallet and setup password strings
#               2) Modify varible values below such as ASTER_DB_HOSTNAME,
#                     ASTER_DB_DATABASE, ASTER_DB_LOGON, ASTER_DB_PASSWORD,
#                     ASTER_DBA_SCHEMA, and FULL_ASTER_CLIENT_DIR as
#                     needed for your environment.
#               3) Create schema specified by variable ASTER_DBA_SCHEMA.
#               4) Create tables aster_sessions_new, aster_sessions_history,
#                     aster_users_new, and aster_users_history and
#                     aster_statements_history using SQL below in
#                     the ASTER_DBA_SCHEMA.
#               5) Install the sysExtract SQL-MR function, if not already
#                     installed.  Copy the sysextract.zip file onto the Aster
#                     Queen node.  Change to that directory and run act and 
#                     from act, run the following command.
#                        beehive>  \install sysextract.zip
#               6) Copy script onto Queen.
#               7) If installing on Aster prior to 6.20, comment out lines
#                     of SQL marked "for 6.20" in steps 2 and 3 below.
#               8) Add the following comands to crontab to run nightly.  The
#                     output is appended to a log file you can review.
#
#                  DBASCRIPTDIR=/home/beehive/dba/scripts
#
#                  0 1 * * * $DBASCRIPTDIR/aster_maint_sessions_users_history.sh >> $DBASCRIPTDIR/aster_maint_sessions_users_history.log
#
#  To Upgrade these tables when upgrading from Aster 6.10 to Aster 6.20
#               1) Run the alter table statement listed below in table
#                    definitions
#               2) Implement current version of this script
#
#               NOTE: In Aster 6.20 and above 4 new columns were added to 
#                     nc_all_sessions table including parentsessionid,
#                     parentstatementid, parenttransactionid, and
#                     parentretrynumber
#
#  To Do:
#
#  1) When put into production the DBA script variables below should be
#     commented out and the password set to "".
#
#  Change History:
#
#  Date        Who              Description
#  ----------  ---------------  ------------------------------------------------
#  07/31/2012  Bruno Coon       Initial creation.
#  08/08/2012  Bruno Coon       Modified to run as sysdba.
#  08/22/2012  Bruno Coon       Added column full_username to aster_users_table
#                                 to trap full user name.
#  08/30/2012  Bruno Coon       Added cleanup of /tmp files.
#  02/02/2014  Bruno Coon       Adapted to bash shell
#  03/04/2014  Bruno Coon       Added status to sessions table to show 'PASSED'
#                                 or FAILED so that failed attempts can also
#                                 be tracked.
#  07/23/2014  Bruno Coon       Added use of sysextract SQL-MR to eliminate
#                                 generation of data from system tables to a
#                                 flat file and then re-loading.
#  01/10/2016  Bruno Coon       Added boolean type casting to columns returned
#                                  by sysextract which returns a boolean as
#                                  a varchar value.
#  05/15/2016  Bruno Coon       Added sourcing of asterenv.sh file for 6.*
#                                 databases.
#  07/07/2016  Bruno Coon       Added the following new columns from the 
#                                 nc_all_sessions table for support of 6.2
#                                 parentsessionid, parentstatementid, 
#                                 parenttransactionid, parentretrynumber.
#-------------------------------------------------------------------------------

##------------------------------------------------------------------------------
##------------------------------------------------------------------------------
##  The table create statements to keep a history all sql statements executed.
##
##  NOTE:  The script to create these tables should be located in
##         /home/beehive/ddl/cr_aster_system_history_tables.sql
##------------------------------------------------------------------------------
##
##------------------------------------------------------------------------------
##  The table create statements to keep a history of all rows in the
##  nc_all_sessions system table.
##------------------------------------------------------------------------------
##
##  create table dba_maint.aster_sessions_new
##    ( sessionid           bigint
##    , username            varchar
##    , clientip            character(16)
##    , dbname              varchar
##    , starttime           timestamp without time zone
##    , endtime             timestamp without time zone
##    , parentsessionid     bigint
##    , parentstatementid   bigint
##    , parenttransactionid bigint
##    , parentretrynumber   bigint
##    , status              varchar(20))
##    distribute by replication;
##
##  create table dba_maint.aster_sessions_history
##    ( sessionid           bigint
##    , username            varchar
##    , clientip            character(16)
##    , dbname              varchar
##    , starttime           timestamp without time zone
##    , endtime             timestamp without time zone
##    , parentsessionid     bigint
##    , parentstatementid   bigint
##    , parenttransactionid bigint
##    , parentretrynumber   bigint
##    , status              varchar(20))
##    distribute by hash (sessionid);
##
##------------------------------------------------------------------------------
##  To upgrade from 6.10,x and prior to 6.20.x and above add columns to
##  tables using the commands:
##------------------------------------------------------------------------------
##  
##  alter table dba_maint.aster_sessions_new
##      add column parentsessionid     bigint
##    , add column parentstatementid   bigint
##    , add column parenttransactionid bigint
##    , add column parentretrynumber   bigint;
##    
##  alter table dba_maint.aster_sessions_history
##      add column parentsessionid     bigint
##    , add column parentstatementid   bigint
##    , add column parenttransactionid bigint
##    , add column parentretrynumber   bigint;
##    
##------------------------------------------------------------------------------
##  The table create statements to keep a history of all rows in the
##  nc_all_users system table.
##------------------------------------------------------------------------------
##
##  create table dba_maint.aster_users_new
##    ( userid                int
##    , username              varchar
##    , schemapath            varchar
##    , cancreaterole         boolean
##    , cancreatedb           boolean
##    , autoinheritgrouppriv  boolean
##    , connlimit             int )
##    distribute by replication;
##
##  create table dba_maint.aster_users_history
##    ( userid                int
##    , username              varchar
##    , schemapath            varchar
##    , cancreaterole         boolean
##    , cancreatedb           boolean
##    , autoinheritgrouppriv  boolean
##    , connlimit             int
##    , first_captured_time   timestamp without time zone
##    , last_activity_time    timestamp without time zone
##    , delete_time           timestamp without time zone
##    , connect_revoked_time  timestamp without time zone
##    , exempt_activity_flag  boolean
##    , security_unique_id    bigint
##    , email_address         varchar(200)
##    , full_username         varchar(50)  )
##    distribute by replication;
##
##------------------------------------------------------------------------------
##------------------------------------------------------------------------------

if [[ -f /home/beehive/config/asterenv.sh ]]; then
  source /home/beehive/config/asterenv.sh
fi

#-------------------------------------------------------------------------------
#  The following variables can be set by sourcing a shell script before this
#  script is called and the ASTER_DB* variable can be commented out of this
#  script.  For example:
#  
#  if [[ -f <path>/act_env.sh ]]; then
#    source <path>/act_env.sh
#  else
#    echo "act_env.sh does not exist"
#    exit 1
#  fi
#
#  Modify the following lines depending on how you log into Aster.
#-------------------------------------------------------------------------------

ASTER_DB_HOSTNAME="10.XXX.XXX.100"
ASTER_DB_DATABASE="xxxxxx"
ASTER_DB_LOGON="db_superuser"
##-- ASTER_DB_PASSWORD="xxxxx"                          # password for account
ASTER_DB_PASSWORD="\$tdwallet(db_superuser_passwd)"     # tdwallet style
ASTER_DBA_SCHEMA="dba_maint"                            # working DBA schema

FULL_ASTER_CLIENT_DIR="/home/beehive/clients"

#-------------------------------------------------------------------------------
#  Initialize local shell variables.
#-------------------------------------------------------------------------------

VERBOSE="N"                             # Y/N - Set to Y for verbose messaging

ACT_ERROR=""                            # To capture act error msg
MAX_STARTTIME=""                        # Max date/time in sql_sessions_history
NOW_DATETIME=""                         # Curr date time in Aster DB
RESULT=""                               # Capture messages from ACT
SQL=""                                  # SQL commands to execute
ROWS_EXTRACTED=""                       # Rows extracted from nc_ table(s)
ROWS_INSERTED=""                        # Rows inserted in aster_*_history table(s)
ROWS_UPDATED=""                         # Rows updated  in aster_*_history table(s)

#-------------------------------------------------------------------------------
#  A command line argument is passed naming giving the name of the table to
#  be replicated.  This name is used to source the appropriate parameter
#  file containing variables that determine how the data is to be moved.
#-------------------------------------------------------------------------------

while getopts ":v" opt; do
  case $opt in
    v )   VERBOSE="Y";;
    \? )  echo "Usage: aster_maint_sessions_users_history.sh  [-v]"
               return 1 ;;
  esac
done

if [ ${VERBOSE} = "Y" ]; then
  echo "VERBOSE mode set.: ${VERBOSE}"
fi

#-------------------------------------------------------------------------------
#  Echo variables that should be set.
#-------------------------------------------------------------------------------

if [ ${VERBOSE} = "Y" ]; then
  echo "ASTER_DB_HOSTNAME .............: ${ASTER_DB_HOSTNAME}"
  echo "ASTER_DB_DATABASE .............: ${ASTER_DB_DATABASE}"
  echo "ASTER_DB_LOGON ................: ${ASTER_DB_LOGON}"
  echo "FULL_ASTER_CLIENT_DIR .........: ${FULL_ASTER_CLIENT_DIR}"
  echo "ASTER_DBA_SCHEMA ..............: ${ASTER_DBA_SCHEMA}"
  echo ""
fi

#-------------------------------------------------------------------------------
#  Function:  exec_act_sql
#
#  Purpose: Execute a SQL statement using aster ACT interface.  If there
#               is an ACT error detected, display the error and exit the
#               script.
#
#  Assumptions: RESULT    - This variable is already defined and will contain
#                           output from call to ACT.
#               SQL       - This variable is already defined and contains a SQL
#                           statement
#               VERBOSE   - This variable is already set.
#               ASTER_DB* - All login variables are already set
#
#  Returns:     None.
#-------------------------------------------------------------------------------

exec_act_sql()
{

  #-------------------------------------------------------------------------------
  #  Run the needed SQL command and put the maxvalue returned from the TGT
  #  tables into an array.  If VERBOSE mode then display SQL before executing it
  #  and display the ACT results as well.
  #-------------------------------------------------------------------------------

  if [ ${VERBOSE} = "Y" ]; then
    echo "SQL: ${SQL}"
    echo ""
  fi

  RESULT=`${FULL_ASTER_CLIENT_DIR}/act -U ${ASTER_DB_LOGON} -w ${ASTER_DB_PASSWORD} -h ${ASTER_DB_HOSTNAME} -d ${ASTER_DB_DATABASE} -A -t -f /dev/stdin <<EOF 2>&1
  ${SQL}
EOF`

  if [ ${VERBOSE} = "Y" ]; then
    echo "RESULT==>\"${RESULT}\""
    echo ""
  fi

  #-------------------------------------------------------------------------------
  #  Check for ACT errors in retreiving a max value.
  #-------------------------------------------------------------------------------

  ACT_ERROR="`echo ${RESULT} | grep 'ERROR: '`"

  if [ "${ACT_ERROR}X" != "X" ]; then
    echo "ERROR 2: ACT error - ${ACT_ERROR}"
    exit 2
  fi

  return

}

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#  STEP 1 - Get the maximum endtime from the aster_sessions_history table.
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------

SQL="
  select coalesce( to_char( max( starttime ), 'MM/DD/YYYY HH24:MI:SS' )
                 , '01/01/2000 00:00:00')
       , to_char( now(), 'MM/DD/YYYY HH24:MI:SS' )
    from ${ASTER_DBA_SCHEMA}.aster_sessions_history;
  \q"

exec_act_sql

#-------------------------------------------------------------------------------
#  Make sure a value is returned from the MAX() select statement.
#-------------------------------------------------------------------------------

MAX_STARTTIME=""
MAX_STARTTIME=`echo ${RESULT} | awk -F '|' '{print $1}'`
NOW_DATETIME=`echo ${RESULT}  | awk -F '|' '{print $2}'`

if [ ${VERBOSE} = "Y" ]; then
  echo "MAX_STARTTIME==>${MAX_STARTTIME}<"
  echo "NOW_DATETIME===>${NOW_DATETIME}<"
  echo ""
fi

echo "Using max endtime value ...: ${MAX_STARTTIME}."
echo "Using now timestamp value..: ${NOW_DATETIME}."

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#  STEP 2 - Get the count of rows that will be moved from the nc_all_sessions
#           table.  Use the values to retrieve data from the nc_all_sessions
#           table into a tmp flat file.
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------

SQL="
  BEGIN;
    select count(1)
      from nc_system.nc_all_sessions
     where starttime < '${NOW_DATETIME}'::datetime
       and starttime > '${MAX_STARTTIME}'::datetime;

    truncate ${ASTER_DBA_SCHEMA}.aster_sessions_new;

    insert into ${ASTER_DBA_SCHEMA}.aster_sessions_new
    select *
      from sysextract(on (SELECT 1)
                      database('${ASTER_DB_DATABASE}')
                      username('${ASTER_DB_LOGON}')
                      password('${ASTER_DB_PASSWORD}')
                      query('select sessionid '
                            '     , username '
                            '     , clientip '
                            '     , dbname '
                            '     , starttime '
                            '     , endtime '
                            '     , parentsessionid '         -- for 6.20 
                            '     , parentstatementid '       -- for 6.20
                            '     , parenttransactionid '     -- for 6.20
                            '     , parentretrynumber '       -- for 6.20
                            '     , \'PASSED\' as status '
                            '  from nc_system.nc_all_sessions '
                            ' where starttime < \'${NOW_DATETIME}\'::datetime '
                            '   and starttime > \'${MAX_STARTTIME}\'::datetime ')
                     );
  END;
  \q"

exec_act_sql

#-------------------------------------------------------------------------------
#  Give standard message of rows extracted from nc_all_sessions.
#-------------------------------------------------------------------------------

ROWS_EXTRACTED=`echo "$RESULT" | awk 'NR == 2 {print $1}'`

printf "%12d row(s) extracted from nc_all_sessions.\n" ${ROWS_EXTRACTED}

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#  STEP 3 - Merge statement to merge data from the aster_sessions_new table
#           into the aster_sessions_history table.
#
#           Note: Comment out the line below that are marked for "for 6.20"
#                 if installing for a Aster DB earlier than 6.20.
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------

SQL="
  merge into ${ASTER_DBA_SCHEMA}.aster_sessions_history
       using ${ASTER_DBA_SCHEMA}.aster_sessions_new
          on aster_sessions_new.sessionid = aster_sessions_history.sessionid
        when matched then
      update set aster_sessions_history.username            = aster_sessions_new.username
               , aster_sessions_history.clientip            = aster_sessions_new.clientip
               , aster_sessions_history.dbname              = aster_sessions_new.dbname
               , aster_sessions_history.starttime           = aster_sessions_new.starttime
               , aster_sessions_history.endtime             = aster_sessions_new.endtime
               , aster_sessions_history.parentsessionid     = aster_sessions_new.parentsessionid       -- for 6.20
               , aster_sessions_history.parentstatementid   = aster_sessions_new.parentstatementid     -- for 6.20
               , aster_sessions_history.parenttransactionid = aster_sessions_new.parenttransactionid   -- for 6.20
               , aster_sessions_history.parentretrynumber   = aster_sessions_new.parentretrynumber     -- for 6.20
               , aster_sessions_history.status              = aster_sessions_new.status
        when not matched then
      insert values ( aster_sessions_new.sessionid
                    , aster_sessions_new.username
                    , aster_sessions_new.clientip
                    , aster_sessions_new.dbname
                    , aster_sessions_new.starttime
                    , aster_sessions_new.endtime
                    , aster_sessions_new.parentsessionid         -- for 6.20
                    , aster_sessions_new.parentstatementid       -- for 6.20
                    , aster_sessions_new.parenttransactionid     -- for 6.20
                    , aster_sessions_new.parentretrynumber       -- for 6.20
                    , aster_sessions_new.status);
  \q"

exec_act_sql

#-------------------------------------------------------------------------------
#  Give standard message of rows updated and inserted into
#  aster_users_history.
#-------------------------------------------------------------------------------

ROWS_INSERTED=`echo "$RESULT" | grep MERGE | awk -F ' ' '{print $2}'`
ROWS_UPDATED=`echo "$RESULT"  | grep MERGE | awk -F ' ' '{print $3}'`

printf "%12d row(s) inserted in table ${ASTER_DBA_SCHEMA}.aster_sessions_history\n" ${ROWS_INSERTED}
printf "                    capturing new user sessions since last update.\n"
printf "%12d row(s) updated in table ${ASTER_DBA_SCHEMA}.aster_sessions_history\n" ${ROWS_UPDATED}
printf "                    updating existing user sessions since last update.\n"

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#  STEP 4 - Extract all rows in the nc_all_users table into aster_users_new
#           table.
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------

SQL="
  BEGIN;
    select count(1)
      from nc_system.nc_all_users;

    truncate ${ASTER_DBA_SCHEMA}.aster_users_new;

    insert into ${ASTER_DBA_SCHEMA}.aster_users_new
    select userid                         as userid
         , username                       as username
         , schemapath                     as schemapath
         , cancreaterole::boolean         as cancreaterole
         , cancreatedb::boolean           as cancreatedb
         , autoinheritgrouppriv::boolean  as autoinheritgrouppriv
         , connlimit                      as connlimit
      from sysextract(on (SELECT 1)
                      database('${ASTER_DB_DATABASE}')
                      username('${ASTER_DB_LOGON}')
                      password('${ASTER_DB_PASSWORD}')
                      query('select * '
                            '  from nc_system.nc_all_users ')
                     );

  END;
  \q"

exec_act_sql

#-------------------------------------------------------------------------------
#  Give standard message of rows extracted from nc_all_users.
#-------------------------------------------------------------------------------

ROWS_EXTRACTED=`echo "$RESULT" | awk 'NR == 2 {print $1}'`

printf "%12d row(s) rows extracted from nc_all_users.\n" ${ROWS_EXTRACTED}

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#  STEP 5 - Merge statement to merge data from the aster_users_new table
#           into the aster_users_history table.
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------

SQL="
  begin;
    update ${ASTER_DBA_SCHEMA}.aster_users_history
       set aster_users_history.userid               = newusr.userid
         , aster_users_history.schemapath           = newusr.schemapath
         , aster_users_history.cancreaterole        = newusr.cancreaterole
         , aster_users_history.cancreatedb          = newusr.cancreatedb
         , aster_users_history.autoinheritgrouppriv = newusr.autoinheritgrouppriv
         , aster_users_history.connlimit            = newusr.connlimit
         , aster_users_history.delete_time          = null
      from ${ASTER_DBA_SCHEMA}.aster_users_new newusr
     where ${ASTER_DBA_SCHEMA}.aster_users_history.username = newusr.username
       and (${ASTER_DBA_SCHEMA}.aster_users_history.userid               !=  newusr.userid        or
            ${ASTER_DBA_SCHEMA}.aster_users_history.schemapath           !=  newusr.schemapath    or
            ${ASTER_DBA_SCHEMA}.aster_users_history.cancreaterole        !=  newusr.cancreaterole or
            ${ASTER_DBA_SCHEMA}.aster_users_history.cancreatedb          !=  newusr.cancreatedb   or
            ${ASTER_DBA_SCHEMA}.aster_users_history.autoinheritgrouppriv !=
                                                                     newusr.autoinheritgrouppriv or
            ${ASTER_DBA_SCHEMA}.aster_users_history.connlimit            !=  newusr.connlimit     or
            ${ASTER_DBA_SCHEMA}.aster_users_history.delete_time          is not null);

    insert into ${ASTER_DBA_SCHEMA}.aster_users_history
    select newusr.userid
         , newusr.username
         , newusr.schemapath
         , newusr.cancreaterole
         , newusr.cancreatedb
         , newusr.autoinheritgrouppriv
         , newusr.connlimit
         , now()        -- first_captured_time
         , now()        -- last_activity_time
         , null         -- delete_time
         , null         -- connect_revoked_time
         , false        -- exempt_activity_flag
         , null         -- security_unique_id
         , null         -- email_address
         , null         -- full_username
      from            ${ASTER_DBA_SCHEMA}.aster_users_new     newusr
      left outer join ${ASTER_DBA_SCHEMA}.aster_users_history hstusr
                              on (hstusr.username = newusr.username)
     where hstusr.username is null;
  end;
  \q"

exec_act_sql

#-------------------------------------------------------------------------------
#  Give standard message of rows updated and inserted into
#  aster_users_history.
#-------------------------------------------------------------------------------

ROWS_INSERTED=`echo "$RESULT" | grep INSERT | awk -F ' ' '{print $3}'`
ROWS_UPDATED=`echo "$RESULT"  | grep UPDATE | awk -F ' ' '{print $2}'`

printf "%12d row(s) inserted in table ${ASTER_DBA_SCHEMA}.aster_users_history\n" ${ROWS_INSERTED}
printf "                    to capture new users.\n"

printf "%12d row(s) updated in table ${ASTER_DBA_SCHEMA}.aster_users_history\n" ${ROWS_UPDATED}
printf "                    capturing current settings for current users.\n"

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#  STEP 6.1 - Update aster_users_history columns.
#
#  Rule 1)  Set the last_activity_time for a user in aster_users_history to the
#           latest datetime for an activity.  Check in the aster_sessions_new
#           to see the latest session endtime, or if the session is started,
#           but not ended, then use now() as the latest_activity_time.
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------

SQL="
  update ${ASTER_DBA_SCHEMA}.aster_users_history
     set aster_users_history.last_activity_time = v1.last_activity_time
    from (select hstusr.userid
               , hstusr.username
               , max( coalesce( newses.endtime, now() )) last_activity_time
            from ${ASTER_DBA_SCHEMA}.aster_users_history   hstusr
               , ${ASTER_DBA_SCHEMA}.aster_sessions_new    newses
           where newses.username = hstusr.username
           group
              by hstusr.userid
               , hstusr.username) v1
   where ${ASTER_DBA_SCHEMA}.aster_users_history.username = v1.username;
  \q"

exec_act_sql

#-------------------------------------------------------------------------------
#  Give standard message of rows updated aster_users_history.
#-------------------------------------------------------------------------------

ROWS_UPDATED=`echo "$RESULT" | grep UPDATE | awk -F ' ' '{print $2}'`

printf "%12d row(s) updated in table ${ASTER_DBA_SCHEMA}.aster_users_history\n" ${ROWS_UPDATED}
printf "                    setting last_activity_time for active users.\n"

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#  STEP 6.2 - Update aster_users_history columns.
#
#  Rule 2)  If the user does not show up in the aster_users_new table, then
#           user must have been deleted, so set the delete_time to now().
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------

SQL="
  update ${ASTER_DBA_SCHEMA}.aster_users_history
     set aster_users_history.delete_time = now()
    from (select hstusr.userid
            from            ${ASTER_DBA_SCHEMA}.aster_users_history  hstusr
            left outer join ${ASTER_DBA_SCHEMA}.aster_users_new      newusr
                                          on (hstusr.userid = newusr.userid)
           where newusr.userid is null) v1
   where ${ASTER_DBA_SCHEMA}.aster_users_history.userid = v1.userid
     and aster_users_history.delete_time is null;
  \q"

exec_act_sql

#-------------------------------------------------------------------------------
#  Give standard message of rows updated aster_users_history.
#-------------------------------------------------------------------------------

ROWS_UPDATED=`echo "$RESULT" | grep UPDATE | awk -F ' ' '{print $2}'`

printf "%12d row(s) updated in table ${ASTER_DBA_SCHEMA}.aster_users_history\n" ${ROWS_UPDATED}
printf "                    setting delete_time for deleted users.\n"

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#  STEP 6.3 - Update aster_users_history columns.
#
#  Rule 3)  If a user had a connect_revoked_time set, and they have a session
#           in aster_sessions_new that has a later starttime that, then it
#           is presumed that that user once again had privileges, so set the
#           connect_revoked_time to null.
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------

SQL="
  update ${ASTER_DBA_SCHEMA}.aster_users_history
     set aster_users_history.connect_revoked_time = null
    from (select hstusr.userid
            from ${ASTER_DBA_SCHEMA}.aster_users_history  hstusr
               , ${ASTER_DBA_SCHEMA}.aster_sessions_new   newses
           where hstusr.username = newses.username
             and hstusr.connect_revoked_time < newses.starttime) v1
   where ${ASTER_DBA_SCHEMA}.aster_users_history.userid = v1.userid;
  \q"

exec_act_sql

#-------------------------------------------------------------------------------
#  Give standard message of rows updated aster_users_history.
#-------------------------------------------------------------------------------

ROWS_UPDATED=`echo "$RESULT" | grep UPDATE | awk -F ' ' '{print $2}'`

printf "%12d row(s) updated in table ${ASTER_DBA_SCHEMA}.aster_users_history\n" ${ROWS_UPDATED}
printf "                    setting connect_revoke_time for inactive users.\n"

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#  STEP 7 - Vaccum the aster_users_history table to keep dead tuples cleaned
#           up.
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------

SQL="
  vacuum full analyze ${ASTER_DBA_SCHEMA}.aster_users_history;
  \q"

exec_act_sql

#-------------------------------------------------------------------------------
#  Give standard message for vacuum.
#-------------------------------------------------------------------------------

echo "Vacuumed table ${ASTER_DBA_SCHEMA}.aster_users_history."

#-------------------------------------------------------------------------------
#  Default SUCCESS exit
#-------------------------------------------------------------------------------

exit 0


