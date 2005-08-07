-- $Id$
-- Test SQL/MED data access to CSV files

-- create a private wrapper for jdbc (don't use the standard jdbc wrapper)
create foreign data wrapper test_jdbc
library 'plugin/FarragoMedJdbc.jar'
language java;

create server csv_server
foreign data wrapper test_jdbc
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
