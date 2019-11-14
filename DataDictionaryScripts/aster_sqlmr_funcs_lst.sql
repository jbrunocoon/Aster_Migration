-- -------------------------------------------------------------------------------
--  Filename:     aster_sqlmr_funcs_lst.sh
-- 
--  Description:  This script SQL is used to capture all the sql-mr functions in
--                the Aster Database except for functions in the nc_temp schema.
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


\o aster_sqlmr_funcs_lst.txt

select f.funcid   
     , s.dbname 
     , f.schemaid 
     , s.schemaname 
     , f.fileid 
     , f.funcname 
     , f.funcowner 
     , f.creationtime 
     , f.funcrunnerkind 
     , f.funchelp_usagesyntax 
     , f.funchelp_shortdesc 
     , f.funchelp_longdesc 
     , f.funchelp_inputcols 
     , f.funchelp_outputcols 
     , f.funchelp_author 
     , f.funcversion 
     , f.apiversion 
     , f.funcsupports_row 
     , f.funcsupports_partition 
     , f.funcsupports_multiple_input 
     , f.funcsupports_graph 
     , f.funcsupports_collaborative_planning 
 from nc_system.nc_all_sqlmr_funcs f 
 join nc_system.nc_all_schemas     s on (s.schemaid = f.schemaid) 
where s.schemaname not like 'nc_temp%';

\o

