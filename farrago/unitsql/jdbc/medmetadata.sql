-- $Id$
-- Test JDBC metadata calls for foreign data wrappers

-- !set verbose true

-- test foreign data wrappers
create foreign data wrapper test_jdbc
library 'plugin/FarragoMedJdbc.jar'
language java;

create server csv_server
foreign data wrapper test_jdbc
options(
    driver_class 'org.relique.jdbc.csv.CsvDriver',
    url 'jdbc:relique:csv:unitsql/med',
    schema_name 'TESTDATA');

-- Check the foreign data wrapper and server
select "name" from sys_fem.med."DataWrapper";
select "name" from sys_fem.med."DataServer";
select "name","value" from sys_fem.med."StorageOption";

-- Create schema and table
create schema csv_schema;
!metadata getSchemas

create foreign table csv_schema.example(
    id int not null,
    name varchar(50) not null,
    extra_field char(1) not null)
server csv_server
options (table_name 'example');

-- Check the foreign table appears and the columns are correct
!tables
!columns EXAMPLE

-- Check metadata updated when dropping
drop foreign table cvs_schema.example;
!tables
!columns EXAMPLE

drop server csv_server cascade;
select "name" from sys_fem.med."DataServer";

drop schema csv_schema cascade;
!metadata getSchemas

drop foreign data wrapper test_jdbc;
select "name" from sys_fem.med."DataWrapper";