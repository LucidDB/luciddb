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
set catalog sys_boot;
!tables

-- test misc calls
!dbinfo
