-- $Id$
-- This script creates the builtin storage data wrappers

!set verbose true
!autocommit off

-- create wrapper for access to MDR repositories
create foreign data wrapper sys_mdr
library 'class net.sf.farrago.namespace.mdr.MedMdrForeignDataWrapper'
language java;


-- create access to system's own CWM repository
create server sys_cwm
foreign data wrapper sys_mdr
options(root_package_name 'CWM');

-- create access to system's own Farrago-specific portion of repository
create server sys_fem
foreign data wrapper sys_mdr
options(root_package_name 'FEM');


-- create wrapper for access to local FTRS data
create local data wrapper sys_ftrs
library 'class net.sf.farrago.namespace.ftrs.FtrsDataWrapper'
language java;

-- create wrapper for access to local mock data
create local data wrapper sys_mock
library 'class net.sf.farrago.namespace.mock.MedMockLocalDataWrapper'
language java;

-- create wrapper for access to foreign mock data
create foreign data wrapper sys_mock_foreign
library 'class net.sf.farrago.namespace.mock.MedMockForeignDataWrapper'
language java;

-- create singleton server for local FTRS row-store data
create server sys_ftrs_data_server
local data wrapper sys_ftrs;

-- create singleton server for local mock row-store data
create server sys_mock_data_server
local data wrapper sys_mock;

-- create singleton server for foreign mock data
create server sys_mock_foreign_data_server
foreign data wrapper sys_mock_foreign;

-- create wrapper for access to JDBC data
create foreign data wrapper sys_jdbc
library '${FARRAGO_HOME}/plugin/FarragoMedJdbc.jar'
language java;

commit;
