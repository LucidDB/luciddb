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


-- create wrapper for access to JDBC data
create foreign data wrapper sys_jdbc
library 'class net.sf.farrago.namespace.jdbc.MedJdbcForeignDataWrapper'
language java;

commit;
