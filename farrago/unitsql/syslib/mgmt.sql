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
