-- -------------------------------------------------------------------------------
--  Filename:     aster_users_history_lst.sql
-- 
--  Description:  This script SQL is used to capture all the collected  
--                history information for sessions captured in the
--                dba_maint.aster_users_history table.
--
--                NOTE: This script was deisgned to be executed from an act
--                      command line and will generate an output file to capture
--                      results.  To using Teradata Studio UI, simply cut and
--                      paste the SQL statement, and then save the results using
--                      Teradata Studio commands.
--
--                There are some optional columns, that would be nice to have.
--                When a user is dropped from the database, the row in the 
--                nc_users_all table is purged.  The Aster collection script will
--                keep the data on the user and capture the datetime when the
--                was dropped.
-- 
--  Change History:
--            
-- 
--  Date        Who               Description
--  ----------  ----------------  ------------------------------------------------_
--  11/11/2019  Bruno Coon        Initial creation.
-- -------------------------------------------------------------------------------

\o aster_users_history_lst.txt

select userid 
     , username 
     , schemapath 
     , cancreaterole 
     , cancreatedb 
     , autoinheritgrouppriv
     , connlimit 
     , first_captured_time        -- optional
     , last_activity_time         -- optional
     , delete_time                -- optional
  from dba_maint.aster_users_history;
   
\o


