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


-- create wrapper for access to local row-store data
create local data wrapper sys_ftrs
library 'class net.sf.farrago.namespace.ftrs.FtrsDataWrapper'
language java;

-- create singleton server for local row-store data
create server sys_rowstore
local data wrapper sys_ftrs;


-- create wrapper for access to JDBC data
create foreign data wrapper sys_jdbc
library '${FARRAGO_HOME}/plugin/FarragoMedJdbc.jar'
language java;

commit;
