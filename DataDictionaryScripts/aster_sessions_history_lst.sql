-- -------------------------------------------------------------------------------
--  Filename:     aster_sessions_history_lst.sql
-- 
--  Description:  This script SQL is used to capture all the collected  
--                history information for sessions captured in the
--                dba_maint.aster_sessions_history table.
--
--                NOTE: This script was deisgned to be executed from an act
--                      command line and will generate an output file to capture
--                      results.  To using Teradata Studio UI, simply cut and
--                      paste the SQL statement, and then save the results using
--                      Teradata Studio commands.
--
--                The deault is to pull all rows in the history table, but
--                by using the where clause the amount of history can be limited.
-- 
--  Change History:
-- 
--  Date        Who               Description
--  ----------  ----------------  ------------------------------------------------_
--  11/11/2019  Bruno Coon        Initial creation.
-- -------------------------------------------------------------------------------

\o aster_sessions_history_lst.txt

select sessionid 
     , username 
     , clientip 
     , dbname 
     , starttime 
     , endtime
     , parentsessionid          -- for 6.20 
     , parentstatementid        -- for 6.20
     , parenttransactionid      -- for 6.20
     , parentretrynumber        -- for 6.20
     , status 
  from dba_maint.aster_sessions_history;
-- where starttime > '01/01/2000 00:00:00'::datetime);  -- how far to reach back
   
\o

