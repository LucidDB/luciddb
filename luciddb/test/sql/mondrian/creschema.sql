create foreign data wrapper mssql_jdbc
library '/home/boris/open/luciddb/plugin/FarragoMedJdbc3p.jar'
language java;

create server mssql_server_foodmart
foreign data wrapper mssql_jdbc
options(
    url 'jdbc:jtds:sqlserver://akela.lucidera.com:1433',
    user_name 'sa',
    password 'ketajo',

    qualifying_catalog_name 'foodmart_new',
    table_types 'TABLE',
    driver_class 'net.sourceforge.jtds.jdbc.Driver'
);

create schema foodmart;
-- create user that has default schema to foodmart
create user MONDRIAN authorization 'Unknown' DEFAULT SCHEMA foodmart;
