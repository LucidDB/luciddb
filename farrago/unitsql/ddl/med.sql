-- $Id$
-- Test SQL/MED DDL

-- create a private wrapper for mdr (don't use the standard mdr wrapper)
create foreign data wrapper test_mdr
library 'class net.sf.farrago.namespace.mdr.MedMdrForeignDataWrapper'
language java;

-- test name uniqueness:  should fail
create foreign data wrapper test_mdr
library 'class net.sf.farrago.namespace.mdr.MedMdrForeignDataWrapper'
language java;

create server mof_server
foreign data wrapper test_mdr
options(
    extent_name 'MOF', 
    schema_name 'Model',
    "org.netbeans.mdr.persistence.Dir" 'unitsql/ddl/mdr');

-- test name uniqueness:  should fail
create server mof_server
foreign data wrapper test_mdr
options(
    extent_name 'MOF', 
    schema_name 'Model',
    "org.netbeans.mdr.persistence.Dir" 'unitsql/ddl/mdr');

-- test name uniqueness relative to a real catalog:  should fail
create server localdb
foreign data wrapper test_mdr
options(
    extent_name 'MOF', 
    schema_name 'Model',
    "org.netbeans.mdr.persistence.Dir" 'unitsql/ddl/mdr');

-- test a direct table reference without creating a foreign table
select "name" from mof_server."Model"."Exception" order by 1;

-- create a local schema to hold foreign table definitions
create schema mof_schema;

-- create a view with direct table reference
create view mof_schema.exception_names as
select "name" from mof_server."Model"."Exception";

-- test same query as above, but against view
select * from mof_schema.exception_names order by 1;

-- create a foreign table
-- (specifying datatypes, and using fixed-width char to make sure the
-- requested type is actually imposed and not ignored)
create foreign table mof_schema.mof_exception(
    name char(20),
    annotation varchar(128),
    container varchar(128),
    "SCOPE" varchar(128),
    visibility varchar(128),
    "mofId" varchar(128),
    "mofClassName" varchar(128))
server mof_server
options(class_name 'Exception');

-- verify that creating a local table using foreign wrapper is illegal
create table mof_schema.local_table_foreign_wrapper(
    id int not null primary key)
server mof_server;

-- and vice versa
create foreign table mof_schema.foreign_table_local_wrapper(
    id int not null primary key)
server sys_mock_data_server;

-- foreign does not allow constraint: should fail
create foreign table mof_schema.test (name char(20) not null primary key)
server mof_server
options(class_name 'Exception');
create foreign table mof_schema.test (name char(20) not null constraint n_unique_name unique)
server mof_server
options(class_name 'Exception');

-- test same query as above, but against foreign table
select name from mof_schema.mof_exception order by 1;

-- create a foreign table (inferring datatypes)
create foreign table mof_schema.mof_exception_inferred
server mof_server
options(class_name 'Exception');

-- test same query as above, but against inferred foreign table
select "name" from mof_schema.mof_exception_inferred order by 1;

-- create a view against foreign table
create view mof_schema.foreign_exception_names as
select name from mof_schema.mof_exception;

-- test same query as above, but against view
select * from mof_schema.foreign_exception_names order by 1;

-- test DROP FOREIGN DATA WRAPPER with RESTRICT:  should fail
drop foreign data wrapper test_mdr restrict;

-- test DROP SERVER with RESTRICT:  should fail
drop server mof_server restrict;

-- test DROP SERVER with CASCADE
drop server mof_server cascade;

-- table mof_exception and boths views should be gone now

select * from mof_schema.mof_exception;

select * from mof_schema.exception_names;

select * from mof_schema.foreign_exception_names;

-- should be OK to drop wrapper now
drop foreign data wrapper test_mdr restrict;

-- now make sure entries are gone from catalog too

select "name" from sys_fem.med."DataWrapper" order by 1;

select "name" from sys_fem.med."DataServer" order by 1;


-- test JDBC wrapper

-- test a direct table reference without creating a foreign table
select * from hsqldb_demo.sales.dept order by deptno;

create schema demo_schema;

-- create a foreign table (specifying datatypes)
create foreign table demo_schema.dept(
    dno integer,
    dname char(20),
    loc char(20))
server hsqldb_demo
options(table_name 'DEPT');

-- test same query as above, but against foreign table
select * from demo_schema.dept order by dno;

-- create a foreign table (inferring datatypes)
create foreign table demo_schema.dept_inferred
server hsqldb_demo
options(table_name 'DEPT');

-- test same query as above, but against foreign table with inferred types
select * from demo_schema.dept_inferred order by deptno;

