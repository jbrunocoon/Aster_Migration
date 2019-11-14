-- -------------------------------------------------------------------------------
--  Filename:     aster_view_lst.sh
-- 
--  Description:  This script SQL is used to capture all the views in the Aster
--                Database except for views in the nc_temp schema.
--
--                NOTE: This script was deisgned to be executed from an act
--                      command line and will generate an output file to capture
--                      results.  To using Teradata Studio UI, simply cut and
--                      paste the SQL statement, and then save the results using
--                      Teradata Studio commands.
-- 
--  Change History:
-- 
--  Date        Who               Description
--  ----------  ----------------  ------------------------------------------------_
--  11/11/2019  Bruno Coon        Initial creation.
-- -------------------------------------------------------------------------------


\o aster_view_lst.txt

select v.viewid 
     , s.dbname 
     , v.schemaid 
     , s.schemaname 
     , v.viewname 
     , v.viewowner 
     , v.viewquery 
     , v.createtime 
     , v.lastmodtime 
  from nc_system.nc_all_views   v 
  join nc_system.nc_all_schemas s on (s.schemaid = v.schemaid) 
 where s.schemaname not like 'nc_temp%' 
   and s.schemaname <>       'nc_system';
   
\o

