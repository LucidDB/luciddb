-- $Id$
-- Test JDBC metadata calls

-- test getCatalogs
!metadata getCatalogs

-- test getSchemas
!metadata getSchemas

-- test getTableTypes
!metadata getTableTypes

-- test getTables (default catalog)
!tables

-- test getTables with pattern
!metadata getTables LOCALDB SALES %EMPS null

-- test getTables (system catalog)
set catalog 'sys_boot';
!tables

-- test getColumns
set catalog 'localdb';
!columns EMPS
!describe EMPS

-- test adding a table and the metadata updating correctly
create schema metadata_test_schema;
!metadata getSchemas

create table metadata_test_schema.new_table (
    tid integer primary key,
    address varchar(256),
    amount decimal(5,2) );

!tables
!columns NEW_TABLE

-- TODO: Try adding and dropping columns from the table and check metadata

-- test dropping the table and the metadata updating correctly
drop table metadata_test_schema.new_table;
!tables
!columns NEW_TABLE

create type metadata_test_schema.dollar_currency as double;

create type metadata_test_schema.rectilinear_coord as (
    x double,
    y double
) final;

!metadata getProcedures LOCALDB SALES %
!metadata getProcedureColumns LOCALDB SALES MAYBE_FEMALE %
!metadata getUDTs LOCALDB METADATA_TEST_SCHEMA % %
!metadata getAttributes LOCALDB METADATA_TEST_SCHEMA % %
!primarykeys EMPS
!indexes EMPS
!procedures

drop schema metadata_test_schema cascade;
!metadata getSchemas

-- test misc calls
!dbinfo
!typeinfo

-- test calls not tested by !dbinfo
!metadata getResultSetHoldability
!metadata getJDBCMajorVersion
!metadata getJDBCMinorVersion

-- test direct queries
select "name", "fennelDisabled", "userCatalogEnabled",
       "codeCacheMaxBytes", "checkpointInterval",
       "serverRmiRegistryPort", "serverSingleListenerPort",
       "calcVirtualMachine", "javaCompilerClassName"
     from sys_fem."Config"."FarragoConfig";

select "databaseInitSize", "databaseIncrementSize", "databaseMaxSize",
       "tempInitSize", "tempIncrementSize", "tempMaxSize",
       "databaseShadowLogInitSize", "databaseTxnLogInitSize",
       "cachePagesMax", "cachePagesInit", "cachePageSize"
    from sys_fem."Config"."FennelConfig";

-- verify unique lineage ID assignment
select count(distinct "lineageId") from sys_fem."SQL2003"."LocalSchema";

-- TODO jvs 7-Aug-2004:  find out why the attribute order on this
-- changes with every model edit
-- select * from sys_fem."Security"."Privilege";

-- We don't actually support these features, but JDBC says we should
-- still return empty results instead of throwing an exception
!metadata getSuperTypes LOCALDB % %
!metadata getSuperTables LOCALDB % %

-- JDBC is silent on whether it is OK to return empty result sets
-- for these when foreign keys are not supported, but since many client
-- tools ask for these, it makes sense to return an empty set instead
-- of throwing an exception since no keys can be defined without support
-- for the feature

!importedkeys EMPS
!exportedkeys DEPTS
!metadata getCrossReference LOCALDB SALES DEPTS LOCALDB SALES EMPS

-- Implemented but not included since result depends on connection
-- !metadata getConnection
-- !metadata hashCode

-- Not supported
-- !metadata getColumnPrivileges
-- !metadata getTablePrivileges
-- !metadata getBestRowIdentifier
-- !metadata getVersionColumns

-- check visibility of objects to unprivileged users

create user joe;
grant select on sales.emps to joe;

!closeall
!connect jdbc:farrago: JOE tiger

-- should only see EMPS
!tables

-- should work
!columns EMPS

-- should see nothing
!columns DEPTS
