-- $Id$
-- Test SQL/MED DDL

-- create a private wrapper for mdr (don't use the standard mdr wrapper
-- because we're going to drop it)
create foreign data wrapper test_mdr
library 'class net.sf.farrago.namespace.mdr.MedMdrForeignDataWrapper'
language java
description 'private data wrapper for mdr';

-- test name uniqueness:  should fail
create foreign data wrapper test_mdr
library 'class net.sf.farrago.namespace.mdr.MedMdrForeignDataWrapper'
language java;

create server mof_server
foreign data wrapper test_mdr
options(
    extent_name 'MOF', 
    schema_name 'Model',
    "org.netbeans.mdr.persistence.Dir" 'unitsql/ddl/mdr')
description 'a server';

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
options(class_name 'Exception')
description 'a foreign table';

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
options(schema_name 'SALES', table_name 'DEPT');

-- test same query as above, but against foreign table
select * from demo_schema.dept order by dno;

-- create a foreign table (inferring datatypes)
create foreign table demo_schema.dept_inferred
server hsqldb_demo
options(schema_name 'SALES', table_name 'DEPT');

-- test same query as above, but against foreign table with inferred types
select * from demo_schema.dept_inferred order by deptno;

-- test SCHEMA_NAME of server specified
create server hsqldb_schema_qual
foreign data wrapper sys_jdbc
options(
    driver_class 'org.hsqldb.jdbcDriver',
    url 'jdbc:hsqldb:testcases/hsqldb/scott',
    user_name 'SA',
    schema_name 'SALES',
    table_types 'TABLE,VIEW');

-- create a foreign table without schema name: should fail
create foreign table demo_schema.dept_server_schema
server hsqldb_schema_qual
options(object 'DEPT');

-- test schema of server with USE_SCHEMA_NAME_AS_FOREIGN_QUALIFIER set
create or replace server hsqldb_schema_qual
foreign data wrapper sys_jdbc
options(
    driver_class 'org.hsqldb.jdbcDriver',
    url 'jdbc:hsqldb:testcases/hsqldb/scott',
    user_name 'SA',
    schema_name 'SALES',
    use_schema_name_as_foreign_qualifier 'true',
    table_types 'TABLE,VIEW');

-- create a foreign table without schema name: should pass
create foreign table demo_schema.dept_server_schema
server hsqldb_schema_qual
options(object 'DEPT');

-- test same query as above, but against foreign table with schema name gotten from server
select * from demo_schema.dept_server_schema order by deptno;

create schema demo_import_schema;

-- test full import
import foreign schema sales
from server hsqldb_demo
into demo_import_schema;

select deptno from demo_import_schema.dept order by deptno;
select empno from demo_import_schema.emp order by empno;

drop schema demo_import_schema cascade;
create schema demo_import_schema;

-- test explicit import
import foreign schema sales
limit to (dept, salgrade)
from server hsqldb_demo
into demo_import_schema;

select deptno from demo_import_schema.dept order by deptno;
-- should fail:  not there
select empno from demo_import_schema.emp order by empno;

drop schema demo_import_schema cascade;
create schema demo_import_schema;

-- should fail:  attempt to explicitly import non-existent table
import foreign schema sales
limit to (dept, salgrade, space_ghost, green_lantern)
from server hsqldb_demo
into demo_import_schema;

-- test explicit exclusion
import foreign schema sales
except (dept, salgrade)
from server hsqldb_demo
into demo_import_schema;

select empno from demo_import_schema.emp order by empno;
-- should fail:  not there
select deptno from demo_import_schema.dept order by deptno;

-- test booleans, since they need special handling
select * from demo_import_schema.bitflip order by b1,b2;

drop schema demo_import_schema cascade;
create schema demo_import_schema;

-- test pattern import
import foreign schema sales
limit to table_name like '%D%E%'
from server hsqldb_demo
into demo_import_schema;

select deptno from demo_import_schema.dept order by deptno;
-- should fail:  not there
select empno from demo_import_schema.emp order by empno;

drop schema demo_import_schema cascade;
create schema demo_import_schema;

-- test pattern exclusion
import foreign schema sales
except table_name like '%D%E%'
from server hsqldb_demo
into demo_import_schema;

select empno from demo_import_schema.emp order by empno;
-- should fail:  not there
select deptno from demo_import_schema.dept order by deptno;

-- negative test for type_substitution option; hsqldb VARCHAR comes
-- back as precision 0, which we don't accept

create server hsqldb_nosub
foreign data wrapper sys_jdbc
options(
    driver_class 'org.hsqldb.jdbcDriver',
    url 'jdbc:hsqldb:testcases/hsqldb/scott',
    user_name 'SA',
    table_types 'TABLE,VIEW',
    type_substitution 'FALSE');

-- should fail: direct table reference without creating a foreign table
select * from hsqldb_nosub.sales.dept order by deptno;

-- should fail: view against said reference
create view demo_schema.dept_nosub_direct_view as
select * from hsqldb_nosub.sales.dept order by deptno;

-- should fail: foreign table without column type info
create foreign table demo_schema.dept_inferred_nosub
server hsqldb_nosub
options(schema_name 'SALES', table_name 'DEPT');

-- should succeed: foreign table with column type info
create foreign table demo_schema.dept_nosub(
    dno integer,
    dname char(20),
    loc char(20))
server hsqldb_nosub
options(schema_name 'SALES', table_name 'DEPT');

-- should succeed: query against foreign table with column type info
select * from demo_schema.dept_nosub order by dno;

-- should succeed: view against foreign table with column type info
create view demo_schema.dept_nosub_view as
select * from demo_schema.dept_nosub;

-- should succeed: query against said view
select * from demo_schema.dept_nosub_view order by dno;

-- test lenient option
create server hsqldb_orig
foreign data wrapper sys_jdbc
options(
    driver_class 'org.hsqldb.jdbcDriver',
    url 'jdbc:hsqldb:testcases/hsqldb/scott',
    user_name 'SA',
    schema_name 'SALES',
    use_schema_name_as_foreign_qualifier 'true',
    table_types 'TABLE,VIEW',
    lenient 'true'
);

create foreign table demo_schema.dept_changing
server hsqldb_orig
options (object 'DEPT');

select * from demo_schema.dept_changing;

create or replace server hsqldb_orig
foreign data wrapper sys_jdbc
options(
    driver_class 'org.hsqldb.jdbcDriver',
    url 'jdbc:hsqldb:testcases/hsqldb_modified/scott',
    user_name 'SA',
    schema_name 'SALES',
    use_schema_name_as_foreign_qualifier 'true',
    table_types 'TABLE,VIEW',
    lenient 'true');

select * from demo_schema.dept_changing;

-- test strictness
-- missing columns, should fail
create foreign table demo_schema.dept_missing_col(
    dno integer,
    dname char(20))
server hsqldb_demo
options(schema_name 'SALES', table_name 'DEPT');

-- extra columns, should fail
create foreign table demo_schema.dept_extra_col(
    dno integer,
    dname char(20),
    loc char(20),
    extra_col integer)
server hsqldb_demo
options(schema_name 'SALES', table_name 'DEPT');

create server hsqldb_opts1
foreign data wrapper sys_jdbc
options(
    driver_class 'org.hsqldb.jdbcDriver',
    url 'jdbc:hsqldb:testcases/hsqldb/scott',
    user_name 'SA',
    schema_name 'SALES',
    use_schema_name_as_foreign_qualifier 'true',
    table_types 'TABLE,VIEW',
    autocommit 'false',
    fetch_size '3');

select deptno from hsqldb_opts1.sales.dept order by deptno;

select dname from hsqldb_opts1.sales.dept order by dname;

