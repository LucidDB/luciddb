-- Test management views

select count(url) from sys_boot.mgmt.sessions_view;

select sql_stmt from sys_boot.mgmt.statements_view;

select count(mof_id) from sys_boot.mgmt.objects_in_use_view;

select sys_boot.mgmt.sleep(1500) from (values(0));

select count("mofId") from sys_boot.mgmt.dba_foreign_wrappers_internal;

select count(*) from sys_boot.mgmt.dba_views_internal2
where catalog_name='SYS_BOOT' and schema_name='MGMT';

select parameter_name from sys_boot.mgmt.dba_routine_parameters_internal1
where schema_name='MGMT' and routine_specific_name='SLEEP'
order by 1;

-- SYS_CWM/FEM use different wrappers depending on repository configuration
select foreign_wrapper_name, foreign_server_name
from sys_boot.mgmt.dba_foreign_servers_internal2
where foreign_server_name not in ('SYS_CWM', 'SYS_FEM')
order by 1,2;

select foreign_server_name
from sys_boot.mgmt.dba_foreign_servers_internal2
order by 1;

create schema mtest;
create table mtest.t(col int primary key);
insert into mtest.t values (1), (2), (3);

select count(*) from sys_boot.mgmt.dba_schemas_internal2
where schema_name='MTEST';

select table_name, table_type from sys_boot.mgmt.dba_tables_internal2
where schema_name='MTEST';

analyze table mtest.t estimate statistics for all columns sample 66 percent;

select table_name, last_analyze_row_count 
from sys_boot.mgmt.dba_stored_tables_internal2
where schema_name='MTEST';

drop schema mtest cascade;

select * from table(sys_boot.mgmt.browse_foreign_schemas('HSQLDB_DEMO'))
order by schema_name;

-- specialize the JDBC foreign wrapper for a particular DBMS;
-- note that the url setting is intended to be interpreted as a template
create foreign data wrapper hsqldb_wrapper
library '${FARRAGO_HOME}/plugin/FarragoMedJdbc.jar'
language java
options(
    browse_connect_description 'Hypersonic',
    driver_class 'org.hsqldb.jdbcDriver',
    url 'jdbc:hsqldb:path/to/data'
);

-- note that none of the system wrappers should have 
-- browse_connect_description set, so don't need ORDER BY here
select * from sys_boot.mgmt.browse_connect_foreign_wrappers;

-- query for available options with no proposed settings (empty_view)
select * from table(
  sys_boot.mgmt.browse_connect_foreign_server(
    'HSQLDB_WRAPPER',
    cursor(table sys_boot.mgmt.browse_connect_empty_options)))
order by option_ordinal, option_choice_ordinal;

-- query again with a real URL, asking for extended options:  
-- should get back details direct from driver
select option_name, option_choice_ordinal, option_choice_value from table(
  sys_boot.mgmt.browse_connect_foreign_server(
    'HSQLDB_WRAPPER',
    cursor(
        values ('URL', 'jdbc:hsqldb:testcases/hsqldb/scott'),
               ('EXTENDED_OPTIONS', 'TRUE'))))
order by option_ordinal, option_choice_ordinal;

-- NOTE jvs 17-Sept-2006:  Next ones return too much noise to make
-- it possible to check their results; just verify that they can execute

select * from table(sys_boot.mgmt.threads()) where false;

select * from table(sys_boot.mgmt.thread_stack_entries()) where false;

select * from table(sys_boot.mgmt.system_info()) where false;

select * from table(sys_boot.mgmt.performance_counters()) where false;

select source_name, counter_name 
from table(sys_boot.mgmt.performance_counters())
order by source_name, counter_name;

call sys_boot.mgmt.create_directory('testgen/mgmt_files');

create server test_server
foreign data wrapper sys_file_wrapper
options (
    directory 'testgen/mgmt_files/',
    file_extension 'csv',
    with_header 'yes', 
    lenient 'no');
create server test_server2
foreign data wrapper sys_file_wrapper
options (
    directory 'testgen/mgmt_files/',
    file_extension 'csv',
    with_header 'yes', 
    lenient 'no');

call sys_boot.mgmt.flush_code_cache();

-- should pass
call sys_boot.mgmt.test_data_server('TEST_SERVER');

call sys_boot.mgmt.flush_code_cache();

-- should pass
call sys_boot.mgmt.test_all_servers_for_wrapper('SYS_FILE_WRAPPER');

call sys_boot.mgmt.delete_file_or_directory('testgen/mgmt_files');

call sys_boot.mgmt.flush_code_cache();

-- should fail now that directory is gone
call sys_boot.mgmt.test_data_server('TEST_SERVER');

call sys_boot.mgmt.flush_code_cache();

-- should fail now that directory is gone
call sys_boot.mgmt.test_all_servers_for_wrapper('SYS_FILE_WRAPPER');

-- set code cache size to some arbitrary number
alter system set "codeCacheMaxBytes" = 42000;

call sys_boot.mgmt.flush_code_cache();

-- verify that flush did not modify code cache size
select "codeCacheMaxBytes" from sys_fem."Config"."FarragoConfig";

-- test generate ddl functions, should all pass

-- test local catalog
select statement from table(sys_boot.mgmt.generate_ddl_for_catalog());
-- test sys_fem catalog
select statement from table(sys_boot.mgmt.generate_ddl_for_catalog('SYS_FEM'));
-- should fail since it doesn't exist
select statement from table(sys_boot.mgmt.generate_ddl_for_catalog(
    '02FAC378-F888-11DF-B152-E56DDFD72085'));

-- test schema, table, view, functions, procedures, jars
-- also runs catalog-specific functions on sys_fem, which should all be empty.
create schema reznor;

create table reznor.rhino (
  a int primary key,
  b int
);

create view reznor.r_view as
  select a, b from reznor.rhino;

create function reznor.eat(plumber varchar(32))
returns varchar(20)
contains sql
return case
  when plumber = 'Mario' then 'Luigi'
  else 'Mario' end;

create jar reznor.rez_jar
library 'file:${FARRAGO_HOME}/plugin/FarragoMedJdbc.jar'
options(0);

create procedure reznor.r_kill(in id bigint)
  language java
  parameter style java
  no sql
  external name 'class net.sf.farrago.syslib.FarragoKillUDR.killSession';

select statement from table(sys_boot.mgmt.generate_ddl_for_schema('REZNOR'));
select statement from table(sys_boot.mgmt.generate_ddl_for_schema(
    'SYS_FEM', 'REZNOR'));
select statement from
  table(sys_boot.mgmt.generate_ddl_for_table('REZNOR', 'RHINO'));
select statement from
  table(sys_boot.mgmt.generate_ddl_for_table('SYS_FEM', 'REZNOR', 'RHINO'));
select statement from
  table(sys_boot.mgmt.generate_ddl_for_table('REZNOR', 'R_VIEW'));
select statement from
  table(sys_boot.mgmt.generate_ddl_for_table('REZNOR', 'not_exist'));
select statement from
  table(sys_boot.mgmt.generate_ddl_for_routine('REZNOR', 'EAT'));
select statement from
  table(sys_boot.mgmt.generate_ddl_for_routine('SYS_FEM', 'REZNOR', 'EAT'));
select statement from
  table(sys_boot.mgmt.generate_ddl_for_routine('REZNOR', 'R_KILL'));
select statement from
  table(sys_boot.mgmt.generate_ddl_for_jar('REZNOR', 'REZ_JAR'));
select statement from
  table(sys_boot.mgmt.generate_ddl_for_jar('SYS_FEM', 'REZNOR', 'REZ_JAR'));

-- UNIQUE constraints
create table reznor.rezzy (a int primary key, b int unique);
create table reznor.rezzy2 (
  a int primary key,
  b int not null,
  c int not null,
  d int not null,
  e int not null,
  CONSTRAINT b_and_c UNIQUE(b, c),
  CONSTRAINT d_and_e UNIQUE(d, e));
select statement from
  table(sys_boot.mgmt.generate_ddl_for_table('REZNOR', 'REZZY'));
select statement from
  table(sys_boot.mgmt.generate_ddl_for_table('REZNOR', 'REZZY2'));

-- test overloaded routine
create function reznor.eat(xy varchar(32), uv varchar(32))
returns varchar(20)
contains sql
specific eat2
return xy;

-- should show both
select statement from
  table(sys_boot.mgmt.generate_ddl_for_routine('REZNOR', 'EAT'));
-- should show just second one
select statement from
  table(sys_boot.mgmt.generate_ddl_for_specific_routine('REZNOR', 'EAT2'));

-- test index
create index rhino_idx on reznor.rhino(a);
select statement from table(sys_boot.mgmt.generate_ddl_for_index(
    'REZNOR', 'RHINO_IDX'));
select statement from table(sys_boot.mgmt.generate_ddl_for_index(
    'SYS_FEM', 'REZNOR', 'RHINO_IDX'));
drop index reznor.rhino_idx;
drop schema reznor cascade;

-- test server created earlier
select statement from
  table(sys_boot.mgmt.generate_ddl_for_server('TEST_SERVER2'));
drop server test_server2;

-- test wrapper
create foreign data wrapper hsqldb_wrapper2
library '${FARRAGO_HOME}/plugin/FarragoMedJdbc.jar'
language java
options(
    browse_connect_description 'Hypersonic',
    driver_class 'org.hsqldb.jdbcDriver',
    url 'jdbc:hsqldb:path/to/data'
);
select statement from
  table(sys_boot.mgmt.generate_ddl_for_wrapper('HSQLDB_WRAPPER2'));
drop foreign data wrapper hsqldb_wrapper2;

-- test user
create user peachy identified by 'toadstool';
select statement from table(sys_boot.mgmt.generate_ddl_for_user('PEACHY'));

-- test role
create role xyz with admin peachy;
select statement from table(sys_boot.mgmt.generate_ddl_for_role('XYZ'));
drop role xyz;
drop user peachy;

-- test label: (can only be created and dropped in the LucidDB personality)
alter session implementation set jar sys_boot.sys_boot.luciddb_plugin;
create label alabel description 'some label';
create label alabel2 from label alabel description 'child label';
select statement from table(sys_boot.mgmt.generate_ddl_for_label('ALABEL'));
select statement from table(sys_boot.mgmt.generate_ddl_for_label('ALABEL2'));
drop label alabel cascade;

