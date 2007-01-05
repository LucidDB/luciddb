-- test the drop_schema_if_exists UDP
create schema TESTSCHEMA;
set schema 'TESTSCHEMA';
create table T(col1 varchar(255), col2 integer);

-- try to drop schema with 'RESTRICT'. should fail quietly.
call applib.drop_schema_if_exists('TESTSCHEMA', 'RESTRICT');

-- try to provide some other option than 'RESTRICT' or 'CASCADE'. should give a nice error.
call applib.drop_schema_if_exists('TESTSCHEMA', 'CASTICT');

-- see that the schema and it's table is still there
select SCHEMA_NAME from SYS_ROOT.DBA_TABLES where SCHEMA_NAME = 'TESTSCHEMA';

-- try to drop schema with 'CASCADE'. should work.
call applib.drop_schema_if_exists('TESTSCHEMA', 'CASCADE');

-- see that the schema is gone.
select SCHEMA_NAME from SYS_ROOT.DBA_SCHEMAS where SCHEMA_NAME = 'TESTSCHEMA';

-- try to delete a schema that does not exist. should fail quietly.
call applib.drop_schema_if_exists('NON_EXISTING_SCHEMA', 'RESTRICT');

-- try to delete an empty schema with 'RESTRICT'.
create schema TESTSCHEMA;
call applib.drop_schema_if_exists('TESTSCHEMA', 'RESTRICT');

-- see that the schema is gone.
select SCHEMA_NAME from SYS_ROOT.DBA_SCHEMAS where SCHEMA_NAME = 'TESTSCHEMA';
