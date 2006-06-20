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

select foreign_wrapper_name, foreign_server_name
from sys_boot.mgmt.dba_foreign_servers_internal2
order by 1,2;

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
    cursor(
        select '' as option_name, '' as option_value
        from sys_boot.jdbc_metadata.empty_view)))
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
