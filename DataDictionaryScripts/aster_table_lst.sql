-- -------------------------------------------------------------------------------
--  Filename:     aster_table_lst.sh
-- 
--  Description:  This script SQL is used to capture all the tables in the Aster
--                Database except for tables in the nc_temp schema.
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


\o aster_table_lst.txt

select t.tableid 
     , s.dbname 
     , t.schemaid 
     , s.schemaname 
     , t.tablename 
     , t.tableowner 
     , t.tabletype 
     , t.compresslevel 
     , t.storagetype 
     , t.partitionkey 
     , t.permissions 
     , t.persistence 
     , t.valid 
 from nc_system.nc_all_tables  t 
 join nc_system.nc_all_schemas s on (s.schemaid = t.schemaid) 
where s.schemaname not like 'nc_temp%';

\o

