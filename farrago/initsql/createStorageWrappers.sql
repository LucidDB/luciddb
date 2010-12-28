-- $Id$
-- This script creates the builtin storage data wrappers

!set verbose true

-- create system-owned schema to hold objects like model extension plugin jars
create or replace schema sys_boot.sys_boot;

set schema 'sys_boot.sys_boot';
set path 'sys_boot.sys_boot';

-- add system objects not yet created
create or replace procedure update_system_objects()
language java
parameter style system defined java
no sql
external name
'class net.sf.farrago.syslib.FarragoUpdateCatalogUDR.updateSystemObjects';

call update_system_objects();

-- update dangling system parameters
create or replace procedure update_configuration()
language java
parameter style system defined java
no sql
external name
'class net.sf.farrago.syslib.FarragoUpdateCatalogUDR.updateConfiguration';

call update_configuration();


-- create wrapper for access to MDR repositories
create or replace foreign data wrapper sys_mdr
library 'class net.sf.farrago.namespace.mdr.MedMdrForeignDataWrapper'
language java;


-- create access to system's own MOF repository
create or replace server sys_mof
foreign data wrapper sys_mdr
options(root_package_name 'MOF', schema_name 'MODEL');

-- creation of SYS_FEM and SYS_CWM delayed.
-- See ./templates/createReposStorageServers.sql.*


-- create wrapper for access to local FTRS data
create or replace local data wrapper sys_ftrs
library 'class net.sf.farrago.namespace.ftrs.FtrsDataWrapper'
language java;

-- create wrapper for access to local LucidDB column-store data
create or replace local data wrapper sys_column_store
library 'class org.luciddb.lcs.LcsDataWrapper'
language java;

-- create wrapper for access to local mock data
create or replace local data wrapper sys_mock
library 'class net.sf.farrago.namespace.mock.MedMockLocalDataWrapper'
language java;

-- create wrapper for access to foreign mock data
create or replace foreign data wrapper sys_mock_foreign
library 'class net.sf.farrago.namespace.mock.MedMockForeignDataWrapper'
language java;

-- create singleton server for local FTRS row-store data
create or replace server sys_ftrs_data_server
local data wrapper sys_ftrs;

-- create singleton server for local LucidDB column-store data
create or replace server sys_column_store_data_server
local data wrapper sys_column_store;

-- create singleton server for local mock row-store data
create or replace server sys_mock_data_server
local data wrapper sys_mock;

-- create singleton server for foreign mock data
create or replace server sys_mock_foreign_data_server
foreign data wrapper sys_mock_foreign;

-- create wrapper for access to JDBC data
create or replace foreign data wrapper sys_jdbc
library '${FARRAGO_HOME}/plugin/FarragoMedJdbc.jar'
language java;

-- create wrapper for access to flatfile data
create or replace foreign data wrapper sys_file_wrapper
library 'class net.sf.farrago.namespace.flatfile.FlatFileDataWrapper'
language java;
