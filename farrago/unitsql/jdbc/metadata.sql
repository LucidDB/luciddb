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
    amount integer );

!tables
!columns NEW_TABLE

-- TODO: Try adding and dropping columns from the table and check metadata

-- test dropping the table and the metadata updating correctly
drop table metadata_test_schema.new_table;
!tables
!columns NEW_TABLE

drop schema metadata_test_schema;
!metadata getSchemas

-- test misc calls
!dbinfo

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

select * from sys_fem."Security"."AuthorizationIdentifier";

-- TODO jvs 7-Aug-2004:  find out why the attribute order on this
-- changes with every model edit
-- select * from sys_fem."Security"."Privilege";

-- Implemented but not included since result depends on connection
-- !metadata getConnection
-- !metadata hashCode

-- Not supported
-- !metadata getProcedures
-- !metadata getProcedureColumns
-- !metadata getPrimaryKeys
-- !metadata getImportKeys
-- !metadata getExportKeys
-- !metadata getColumnPriviledges
-- !metadata getTablePriviledges
-- !metadata getBestRowIdentifier
-- !metadata getVersionColumns
-- !metadata getCrossReference
-- !metadata getTypeInfo
-- !metadata getIndexInfo
-- !metadata getUDTs
-- !metadata getSuperTypes
-- !metadata getSuperTables
-- !metadata getAttributes
-- !indexes
-- !importedkeys
-- !exportedkeys
-- !primarykeys
-- !procedures

