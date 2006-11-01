-- $Id$
-- Test SQL/MED data access to CSV files

create server csv_server
foreign data wrapper sys_jdbc
options(
    driver_class 'org.relique.jdbc.csv.CsvDriver',
    url 'jdbc:relique:csv:unitsql/med',
    schema_name 'TESTDATA');

-- test direct table reference
select * from csv_server.testdata."example" order by 3;

-- create a local schema to hold foreign table definition
create schema csv_schema;

-- create a foreign table definition
create foreign table csv_schema.explicit_example(
    id int not null,
    name varchar(50) not null,
    extra_field char(1) not null)
server csv_server
options (table_name 'example');

select 
    id+1 as idplusone,
    name,
    extra_field 
from 
    csv_schema.explicit_example 
order by 3;

-- should fail:  required metadata support not available
import foreign schema testdata
from server csv_server
into csv_schema;

-- verify that missing/conflicting properties yield a meaningful 
-- user-level excn (http://issues.eigenbase.org/browse/LDB-28)

-- should fail due to missing url
create server csv_server_missing_url
foreign data wrapper sys_jdbc
options(
    driver_class 'org.relique.jdbc.csv.CsvDriver');

create server csv_server_missing_schema
foreign data wrapper sys_jdbc
options(
    driver_class 'org.relique.jdbc.csv.CsvDriver',
    url 'jdbc:relique:csv:unitsql/med');

-- should fail due to missing schema name
create foreign table csv_schema.missing_schema
server csv_server_missing_schema;

-- should fail due to missing table name
create foreign table csv_schema.missing_table
server csv_server;

-- should fail due to conflicting schema name
create foreign table csv_schema.explicit_example(
    id int not null,
    name varchar(50) not null,
    extra_field char(1) not null)
server csv_server
options (table_name 'example', schema_name 'grub');

-- test an extended option
create server csv_server_with_extended_option
foreign data wrapper sys_jdbc
options(
    driver_class 'org.relique.jdbc.csv.CsvDriver',
    url 'jdbc:relique:csv:unitsql/med',
    extended_options 'TRUE',
    schema_name 'TESTDATA',
    "suppressHeaders" 'true');

select count(*) from csv_server_with_extended_option.testdata."example";

-- verify that without extended_option enabled, extra properties are
-- not passed through
create server csv_server_without_extended_option
foreign data wrapper sys_jdbc
options(
    driver_class 'org.relique.jdbc.csv.CsvDriver',
    url 'jdbc:relique:csv:unitsql/med',
    schema_name 'TESTDATA',
    "suppressHeaders" 'true');

select count(*) from csv_server_without_extended_option.testdata."example";
