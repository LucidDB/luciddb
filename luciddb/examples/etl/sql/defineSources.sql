create server jdbc_link
foreign data wrapper sys_jdbc
options(
    driver_class 'org.hsqldb.jdbcDriver',
    url 'jdbc:hsqldb:examples/etl/jdbcdata/scott',
    user_name 'SA',
    table_types 'TABLE');

create server file_link
foreign data wrapper sys_file_wrapper
options (
    directory 'examples/etl/filedata/',
    control_file_extension 'bcp',
    file_extension 'txt',
    with_header 'NO', 
    log_directory 'trace/',
    field_delimiter '\t');

create schema extraction_schema;

import foreign schema sales
from server jdbc_link
into extraction_schema;

import foreign schema bcp
from server file_link
into extraction_schema;

select table_name, column_name
from sys_root.dba_columns
where schema_name='EXTRACTION_SCHEMA'
order by table_name,ordinal_position;

set schema 'extraction_schema';

select count(*) from timesheet;

select count(*) from emp;

select count(*) from dept;

select * from dept;
