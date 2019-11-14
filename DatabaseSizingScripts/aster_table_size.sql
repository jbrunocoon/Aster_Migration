-- -------------------------------------------------------------------------------
--  Filename:     aster_table_size.sql
-- 
--  Description:  This script SQL is used to capture all table objects in
--                all databases and their size in uncompressed GB.
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
--  11/13/2019  Bruno Coon        Initial creation.
-- -------------------------------------------------------------------------------

\o aster_table_size.txt

select owner_name                             as "owner"
     , case when object_type = 'relation'
            then 'table' else object_type end as "tbl/idx"
     , database
     , schema
     , relation                               as "name"
     , compression_level                      as "cmp lvl"
     , (sum(uncompressed_size) / 
                (1024 * 1024))::decimal(14,6) as "MB Uncompressed"
  from nc_relationstats(on (select 1)
                        partition by 1
                        databases ('*')       -- database name
                        report_size ('uncompressed')
                        report_stats('all')
                        report_stats_mode('estimated')                       )
group by 1, 2, 3, 4, 5, 6
order by 1, 3, 7 desc;

\o


