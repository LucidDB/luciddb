--
-- Setup
--

create schema ct;
set schema 'ct';
set path 'ct';

create table t1(aa int, bb varchar(20));
insert into t1 values (1, 'one'), (3, 'three'), (10, 'ten');

create table t2(a int, b varchar(20));
insert into t2 values (2, 'dos'), (3, 'tres');

--
-- UDX with multiple output columns
--
create function get_column_types(c cursor)
returns table( colname varchar(65535), coltype int, coltypename varchar(65535))
language java
parameter style system defined java
no sql
external name 'class com.lucidera.luciddb.test.udr.GetColumnTypesUdx.execute';

select * from table(
  get_column_types(
    cursor(select * from sys_fem."Config"."FarragoConfig")))
order by 1;

select * from table(
  get_column_types(
    cursor(select * from sys_fem."Config"."FennelConfig")))
order by 1;

select * from table(
  get_column_types(
    cursor(values(1,'dsfs', cast ('sdfsd' as varchar(10)), TIME'12:12:12', 1.2, cast(12.2 as float)))))
order by 1;

--
-- UDX in view
--
create function pivot_columns_to_rows(c cursor)
returns table(param_name varchar(65535), param_value varchar(65535))
language java
parameter style system defined java
no sql
external name 'applib.applibJar:com.lucidera.luciddb.applib.cursor.PivotColumnsToRowsUdx.execute';

create view farrago_params as
select * from table(
  pivot_columns_to_rows(
    cursor(select "name","calcVirtualMachine", "checkpointInterval",
                  "codeCacheMaxBytes", "fennelDisabled", 
                  "javaCompilerClassName",
                  "serverRmiRegistryPort", "serverSingleListenerPort", 
                  "userCatalogEnabled" 
           from sys_fem."Config"."FarragoConfig")
));

create view fennel_params as
select * from table(
  pivot_columns_to_rows(
    cursor(select "cachePageSize", "cachePagesInit", "cachePagesMax",
                  "databaseIncrementSize", "databaseInitSize",
                  "databaseMaxSize", "databaseShadowLogIncrementSize",
                  "databaseShadowLogInitSize", "databaseTxnLogIncrementSize",
                  "databaseTxnLogInitSize", "groupCommitInterval",
                  "jniHandleTraceFile", "resourceDir", "tempIncrementSize",
                  "tempInitSize", "tempMaxSize"
           from sys_fem."Config"."FennelConfig")
));

select * from farrago_params
union 
select * from fennel_params
order by 1;

--
-- UDX with two cursors
--
create function two_cursor_test(c1 cursor, c2 cursor)
returns table(col1 int, col2 varchar(20))
language java
parameter style system defined java
no sql
external name 'class com.lucidera.luciddb.test.udr.TestTwoCursorUdx.execute';

select * from table(
  two_cursor_test(
    cursor(select * from t1),
    cursor(select * from t2)))
order by 1;

--
-- UDX with another UDX as input cursor
--
select * from table(
  get_column_types(
    cursor(select * from
           table(applib.time_dimension(2101, 4, 25, 2101, 5, 2)
))));

select * from table(
  get_column_types(
    cursor(select * from table(
             pivot_columns_to_rows(
               cursor(select * from table(
                        applib.time_dimension(2000, 12, 29, 2000, 12, 29)
)))))));

--
-- Join two UDX
--
select 
  pctr.param_name, pctr.param_value, gct.coltype, gct.coltypename
from 
  table(get_column_types(
    cursor(select * from sys_fem."Config"."FarragoConfig"))) gct,
  table(pivot_columns_to_rows(
    cursor(select "name","calcVirtualMachine", "checkpointInterval",
                  "codeCacheMaxBytes", "fennelDisabled", 
                  "javaCompilerClassName", "serverRmiRegistryPort", 
                  "serverSingleListenerPort", "userCatalogEnabled" 
           from sys_fem."Config"."FarragoConfig"))) pctr
where gct.colname = pctr.param_name
order by 1
;

--
-- UDX throws exception after a few records
--
create function test_throws_exception(c cursor)
returns table(junk varchar(256))
language java
parameter style system defined java
no sql
external name 'class com.lucidera.luciddb.test.udr.ThrowsExceptionUdx.execute';
select * from table(
  test_throws_exception(
    cursor(select * from t1 order by 1)
));

-- 
-- UDX using internal query against jdbc:default:connection
--
create function create_table_as_select(schema_name varchar(65535), table_name varchar(65535), c cursor)
returns table(errors varchar(65535))
language java
parameter style system defined java
modifies sql data
external name 'class com.lucidera.luciddb.test.udr.CreateTableAsSelectUdx.execute';

-- TODO: FRG-141, commented out so we don't have lock problems
-- select * from table(
--   create_table_as_select(
--     'CT', 
--     'MYTABLE', 
--     cursor(select * from farrago_params)
-- ));

-- select * from ct.mytable order by 1;
