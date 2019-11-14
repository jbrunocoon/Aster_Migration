#!/bin/bash

#-------------------------------------------------------------------------------
#  Filename:    aster_maint_statements_history.sh
#
#  Description: This script captures all new rows in the Aster DB
#               nc_system.nc_all_statements table and merges them into the
#               dba_maint.aster_statements_history table.  This is done
#               using the following steps:
#
#               1)  Get the latest endtime in the aster_statements_history table
#                     as the max_endtime.
#               2)  Extract all statements from the nc_all_statements table
#                     where the endtime is greater than the max_enddate and
#                     less than now() which is the current time.  This uses
#                     the SQL-MR sysextract function.
#               3)  Use ncluster_loader to load the data in the table
#                     aster_statements_new after truncating  the table.
#               4)  Perform a merge statement to merge the date in the
#                     aster_statements_new table into the aster_statements_history
#                     table.
#
#               NOTE: This script is assumed to be executed using an Aster DB
#                     account with the db_admin role.  A script that is sourced
#                     before this script, could be called to set environment
#                     variables as needed.
#
#               NOTE: It is presumed that this script resides on the Aster
#                     cluster queen node.
#
#  Usage:       ./aster_maint_statements_history.ksh [-v]
#
#  Arguments:   -v   - Run script in verbose mode.  Show actions and execute
#                      them.
#
#  Outputs:     Insert rows into the following tables:
#                  dba_maint.aster_statements_history
#
#  To Install:  1) Optionally install td_wallet and setup password strings
#               2) Modify varible values below such as ASTER_DB_HOSTNAME,
#                     ASTER_DB_DATABASE, ASTER_DB_LOGON, ASTER_DB_PASSWORD,
#                     ASTER_DBA_SCHEMA, and FULL_ASTER_CLIENT_DIR as
#                     needed for your environment.
#               3) Create schema specified by variable ASTER_DBA_SCHEMA.
#               4) Create tables aster_statements_new and
#                     aster_statements_history using SQL below in
#                     the ASTER_DBA_SCHEMA.
#               5) Install the sysExtract SQL-MR function, if not already
#                     installed.  Copy the sysextract.zip file onto the Aster
#                     Queen node.  Change to that directory and run act and 
#                     from act, run the following command.
#                        beehive>  \install sysextract.zip
#               6) Copy script onto Queen.
#               7) Add the following commands to crontab to run nightly.  The
#                     output is appended to a log file you can review.
#
#                  DBASCRIPTDIR=/home/beehive/dba/scripts
#
#                  2 1 * * * $DBASCRIPTDIR/aster_maint_statements_history.sh >> $DBASCRIPTDIR/aster_maint_statements_history.log
#
# To Do:
#
#  1) When put into production the DBA script variables below should be
#     commented out and the password set to "".
#
#  Change History:
#
#  Date        Who               Description
#  ----------  ----------------  -----------------------------------------------
#  07/30/2012  Bruno Coon        Initial creation.
#  08/08/2012  Bruno Coon        Modified to run as sysdba.
#  08/30/2012  Bruno Coon        Added cleanup of /tmp files.
#  07/23/2014  Bruno Coon        Added use of sysextract SQL-MR to eliminate
#                                  generation of data from system tables to a
#                                  flat file and then re-loading.
#  04/18/2016  Bruno Coon       Added boolean type cast as sysextract function
#                                  returns boolean column iscancelable as a
#                                  character data type.
#  05/15/2016  Bruno Coon       Added sourcing of asterenv.sh file for 6.*
#                                 databases.
#  07/07/2016  Bruno Coon       Reviewed for 6.20 compatability.  Added comments. 
#-------------------------------------------------------------------------------

##------------------------------------------------------------------------------
##------------------------------------------------------------------------------
##  The table create statements to keep a history all sql statements executed.
##
##  NOTE:  These scripts should be located in /home/beehive/ddl
##------------------------------------------------------------------------------
##
##  create schema dba_maint;
##
##  create table dba_maint.aster_statements_new
##    ( statementid       bigint
##    , xactionid         bigint
##    , sessionid         bigint
##    , retrynum          int
##    , statement         varchar
##    , starttime         timestamp without time zone
##    , endtime           timestamp without time zone
##    , iscancelable      boolean )
##    distribute by hash (statementid);
##
##  create table dba_maint.aster_statements_history
##    ( statementid       bigint
##    , xactionid         bigint
##    , sessionid         bigint
##    , retrynum          int
##    , statement         varchar
##    , starttime         timestamp without time zone
##    , endtime           timestamp without time zone
##    , iscancelable      boolean )
##    distribute by hash (statementid);
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
ASTER_DB_DATABASE="aster_xxx"
ASTER_DB_LOGON="db_superuser"
##-- ASTER_DB_PASSWORD="XXXXX"                          # password for account
ASTER_DB_PASSWORD="\$tdwallet(db_superuser_passwd)"     # tdwallet style
ASTER_DBA_SCHEMA="dba_maint"                            # working DBA schema

FULL_ASTER_CLIENT_DIR="/home/beehive/clients"

#-------------------------------------------------------------------------------
#  Initialize local shell variables.
#-------------------------------------------------------------------------------

VERBOSE="N"                             # Y/N - Set to Y for verbose messaging

ACT_ERROR=""                            # To capture act error msg
MAX_ENDTIME=""                          # Max datetime in sql_statement_history
NOW_DATETIME=""                         # Curr date time in Aster DB
RESULT=""                               # Capture messages from ACT
ROWS_EXTRACTED=""                       # Rows extracted from nc_all_statements
ROWS_INSERTED=""                        # Rows inserted in sql_statement_history
ROWS_UPDATED=""                         # Rows updated  in sql_statement_history

#-------------------------------------------------------------------------------
#  A command line argument is passed giving the name of the table to
#  be replicated.  This name is used to source the appropriate parameter
#  file containing variables that determine how the data is to be moved.
#-------------------------------------------------------------------------------

while getopts ":v?" opt; do
  case $opt in
    v )   VERBOSE="Y";;
    ? )   echo "Usage: aster_maint_statements_history.sh  [-v]"
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
#  Function:    exec_act_sql
#
#  Purpose:     Execute a SQL statement using aster ACT interface.  If there
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
#  STEP 1 - Get the maximum endtime from the aster_statements_history table.
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------

SQL="
     select coalesce( to_char( max( endtime ), 'MM/DD/YYYY HH24:MI:SS' )
                    , '01/01/2000 00:00:00')
          , to_char( now(), 'MM/DD/YYYY HH24:MI:SS' )
       from ${ASTER_DBA_SCHEMA}.aster_statements_history;
     \q"

exec_act_sql

#-------------------------------------------------------------------------------
#  Make sure a value is returned from the MAX() select statement.
#-------------------------------------------------------------------------------

MAX_ENDTIME=""
MAX_ENDTIME=`echo ${RESULT}  | awk -F '|' '{print $1}'`
NOW_DATETIME=`echo ${RESULT} | awk -F '|' '{print $2}'`

if [ ${VERBOSE} = "Y" ]; then
  echo "MAX_ENDTIME===>${MAX_ENDTIME}<"
  echo "NOW_DATETIME==>${NOW_DATETIME}<"
fi

echo "Using max endtime value ...: ${MAX_ENDTIME}."
echo "Using now timestamp value..: ${NOW_DATETIME}."


#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#   STEP 2 - Get the maximum endtime from the aster_statements_history table.
#
#   NOTE: The 1st query below returns a rowcount to the RESULT variable.  The
#         second select statement pipes to output using the \o option and does
#         not return anything to act RESULT variable.
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------

SQL="
  BEGIN;
    select coalesce(count(1), 0)
      from nc_system.nc_all_statements
     where endtime < '${NOW_DATETIME}'::datetime
       and endtime > '${MAX_ENDTIME}'::datetime;

    truncate ${ASTER_DBA_SCHEMA}.aster_statements_new;

    insert into ${ASTER_DBA_SCHEMA}.aster_statements_new
    select statementid
         , xactionid
         , sessionid
         , retrynum
         , statement
         , starttime
         , endtime
         , iscancelable::boolean
      from sysextract(on (SELECT 1)
                      database('${ASTER_DB_DATABASE}')
                      username('${ASTER_DB_LOGON}')
                      password('${ASTER_DB_PASSWORD}')
                      query('select * '
                            '  from nc_system.nc_all_statements '
                            ' where endtime < \'${NOW_DATETIME}\'::datetime '
                            '   and endtime > \'${MAX_ENDTIME}\'::datetime ')
                     );

  END;
  \q"

exec_act_sql

#-------------------------------------------------------------------------------
#  Give standard message of rows extracted from nc_all_statements.
#-------------------------------------------------------------------------------

ROWS_EXTRACTED=`echo "$RESULT" | awk 'NR == 2 {print $1}'`

##  echo "ROWS_EXTRACTED==>$ROWS_EXTRACTED."

printf "%12d row(s) extracted from nc_all_statements.\n" ${ROWS_EXTRACTED}

## exit

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#  STEP 3 - Merge statement to merge data from the _sterstatements_new table
#           into the aster_statements_history table.
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------

SQL="
  merge into ${ASTER_DBA_SCHEMA}.aster_statements_history
       using ${ASTER_DBA_SCHEMA}.aster_statements_new
          on aster_statements_new.statementid = aster_statements_history.statementid
        when matched then
      update set aster_statements_history.xactionid    = aster_statements_new.xactionid
               , aster_statements_history.sessionid    = aster_statements_new.sessionid
               , aster_statements_history.retrynum     = aster_statements_new.retrynum
               , aster_statements_history.statement    = aster_statements_new.statement
               , aster_statements_history.starttime    = aster_statements_new.starttime
               , aster_statements_history.endtime      = aster_statements_new.endtime
               , aster_statements_history.iscancelable = aster_statements_new.iscancelable
        when not matched then
      insert values ( aster_statements_new.statementid
                    , aster_statements_new.xactionid
                    , aster_statements_new.sessionid
                    , aster_statements_new.retrynum
                    , aster_statements_new.statement
                    , aster_statements_new.starttime
                    , aster_statements_new.endtime
                    , aster_statements_new.iscancelable );
  \q"

exec_act_sql

#-------------------------------------------------------------------------------
#  Give standard message of rows updated and inserted into
#  aster_statements_history.
#-------------------------------------------------------------------------------

ROWS_INSERTED=`echo "$RESULT" | grep MERGE | awk -F ' ' '{print $2}'`
ROWS_UPDATED=`echo "$RESULT"  | grep MERGE | awk -F ' ' '{print $3}'`

printf "%12d row(s) inserted in table ${ASTER_DBA_SCHEMA}.aster_statements_history.\n" ${ROWS_INSERTED}
printf "%12d row(s) updated  in table ${ASTER_DBA_SCHEMA}.aster_statements_history.\n" ${ROWS_UPDATED}

#-------------------------------------------------------------------------------
#  Default SUCCESS exit
#-------------------------------------------------------------------------------

exit 0

