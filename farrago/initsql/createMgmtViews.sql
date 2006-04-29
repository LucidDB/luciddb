-- This script creates a view schema used for database management 
                                                                                
!set verbose true
                                                                                
-- create views in system-owned schema sys_boot.mgmt
create schema sys_boot.mgmt;
set schema 'sys_boot.mgmt';
set path 'sys_boot.mgmt';

create function statements()
returns table(id int, session_id int, sql_stmt varchar(1024), create_time timestamp, parameters varchar(1024))
language java
parameter style system defined java
no sql
external name 'class net.sf.farrago.syslib.FarragoManagementUDR.statements';

create view statements_view as
  select * from table(statements());

-- todo:  grant this only to a privileged user
grant select on statements_view to public;

create function sessions()
returns table(id int, url varchar(128), current_user_name varchar(128), current_role_name varchar(128), session_user_name varchar(128), system_user_name varchar(128), system_user_fullname varchar(128), session_name varchar(128), program_name varchar(128), process_id int, catalog_name varchar(128), schema_name varchar(128), is_closed boolean, is_auto_commit boolean, is_txn_in_progress boolean)

language java
parameter style system defined java
no sql
external name 'class net.sf.farrago.syslib.FarragoManagementUDR.sessions';

create view sessions_view as
  select * from table(sessions());

-- todo:  grant this only to a privileged user
grant select on sessions_view to public;

create function objectsInUse()
returns table(stmt_id int, mof_id varchar(32))

language java
parameter style system defined java
no sql
external name 'class net.sf.farrago.syslib.FarragoManagementUDR.objectsInUse';

create view objects_in_use_view as
  select * from table(objectsInUse());

-- TODO: grant this only to a privileged user
grant select on objects_in_use_view to public;

-- lets an administrator kill a running session
-- TODO: grant this only to a privileged user
create procedure kill_session(in id bigint)
  language java
  parameter style java
  no sql
  external name 'class net.sf.farrago.syslib.FarragoKillUDR.killSession';

-- lets an administrator kill an executing statement
-- (like unix "kill -KILL")
-- param ID: globally-unique statement id
-- TODO: grant this only to a privileged user
create procedure kill_statement(in id bigint)
  language java
  parameter style java
  no sql
  external name 'class net.sf.farrago.syslib.FarragoKillUDR.killStatement';

-- kills all statements with SQL matching a given string
-- (like unix pkill)
-- Works around lack of scalar subqueries, whuch makes kill_statement(id) hard to use
-- param SQL: a string
-- TODO: grant this only to a privileged user
create procedure kill_statement_match(in s varchar(256))
  language java
  parameter style java
  no sql
  external name 'class net.sf.farrago.syslib.FarragoKillUDR.killStatementMatch';
            
---------------------------------------------------------------------------
-- Statistics generation                                                 --
---------------------------------------------------------------------------

--
-- Set the row count of a table
--
create procedure stat_set_row_count(
    in catalog_name varchar(2000),
    in schema_name varchar(2000),
    in table_name varchar(2000),
    in row_count bigint)
language java
contains sql
external name 'class net.sf.farrago.syslib.FarragoStatsUDR.set_row_count';

--
-- Set the page count of an index
--
create procedure stat_set_page_count(
    in catalog_name varchar(2000),
    in schema_name varchar(2000),
    in index_name varchar(2000),
    in page_count bigint)
language java
contains sql
external name 'class net.sf.farrago.syslib.FarragoStatsUDR.set_page_count';

--
-- Generate a histogram for a column
--
-- distribution_type must be 0 for now
-- value_digits are characters to use for fake column values
--
create procedure stat_set_column_histogram(
    in catalog_name varchar(2000),
    in schema_name varchar(2000),
    in table_name varchar(2000),
    in column_name varchar(2000),
    in distict_values bigint,
    in sample_percent int,
    in sample_distinct_values bigint,
    in distribution_type int,
    in value_digits varchar(2000))
language java
contains sql
external name 'class net.sf.farrago.syslib.FarragoStatsUDR.set_column_histogram';

--
-- Page counts, qualified by 
--
create view page_counts_view as
select 
  n."name" as "schema", t."name" as "table", 
  i."name" as "index", "pageCount" 
from 
  sys_cwm."Core"."Namespace" n,
  sys_fem.sql2003."AbstractColumnSet" t,
  sys_fem.med."LocalIndex" i
where 
  t."namespace" = n."mofId"
  and i."spannedClass" = t."mofId"
  and "pageCount" is not null;

--
-- Select row counts, qualified by schema and table
--
create view row_counts_view as
select n."name" as "schema", t."name" as "table", "rowCount" 
from 
  sys_cwm."Core"."Namespace" n,
  sys_fem.sql2003."AbstractColumnSet" t
where 
  t."namespace" = n."mofId"
  and "rowCount" is not null;

--
-- Selects histograms, qualified by table name (missing schema name)
--
create view histograms_view as
select 
    t."name" as "table", c."name" as "column",
    "distinctValueCount" "values","percentageSampled" "percent",
    "barCount","rowsPerBar","rowsLastBar"
from 
    sys_fem.med."ColumnHistogram" h,
    sys_fem.sql2003."AbstractColumn" c,
    sys_fem.sql2003."AbstractColumnSet" t
where 
    c."Histogram" = h."mofId"
    and c."owner" = t."mofId";

--
-- Selects histogram bars, qualified by table and column
--
create view histogram_bars_a_view as
select 
  h."mofId",b."ordinal","startingValue","valueCount"
from 
  sys_fem.med."ColumnHistogramBar" b
inner join
  sys_fem.med."ColumnHistogram" h
on
  b."Histogram" = h."mofId";

create view histogram_bars_b_view as
select 
  c."owner" as "table", c."name" as "column", 
  a."ordinal","startingValue","valueCount"
from 
  sys_fem.sql2003."AbstractColumn" c
inner join
  histogram_bars_a_view a
on
  c."Histogram" = a."mofId";

create view histogram_bars_view as
select 
  t."name" as "table", b."column", 
  b."ordinal","startingValue","valueCount"
from 
  sys_fem.sql2003."AbstractColumnSet" t
inner join
  histogram_bars_b_view b
on
  b."table" = t."mofId";
