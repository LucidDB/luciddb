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
    url 'jdbc:relique:csv:${FARRAGO_HOME}/unitsql/med',
    schema_name 'TESTDATA');

-- Check the foreign data wrapper and server (exclude some storage option names
-- that vary by repository configuration in sys_fem/cwm)
select "name" from sys_fem.med."DataWrapper" order by 1;
select "name" from sys_fem.med."DataServer" order by 1;
select "name","value" from sys_fem.med."StorageOption" where "name" in ('URL', 'DRIVER_CLASS') order by 1,2;

-- Test a join to look up the storage options for a server
select o."name",o."value" 
from 
(select * from sys_fem.med."DataServer" where "name"='SYS_MOF') s
inner join
sys_fem.med."StorageOption" o
on s."mofId"=o."StoredElement"
order by 1,2;

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
select "name" from sys_fem.med."DataServer" order by 1;

drop schema csv_schema cascade;
!metadata getSchemas

drop foreign data wrapper test_jdbc;
select "name" from sys_fem.med."DataWrapper" order by 1;
