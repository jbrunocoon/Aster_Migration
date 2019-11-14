-- -------------------------------------------------------------------------------
--  Filename:     aster_schema_lst.sql
-- 
--  Description:  This script SQL is used to capture all the schemas in the Aster
--                Database except for nc_temp.
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


\o aster_schema_lst.txt

select *
  from nc_system.nc_all_schemas
 where schemaname not like 'nc_temp%';
 
\o 
