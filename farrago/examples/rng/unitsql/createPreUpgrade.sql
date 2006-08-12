-- $Id$
-- Test creation of objects which are to survive plugin installation

-- before catalog upgrade, save a copy of the catalog with some objects in it
-- so that we can verify that catalog can be restored after upgrade;
-- we put stuff in sys_boot so that it won't be dropped by test cleanup
create schema sys_boot.old_stuff;
create table sys_boot.old_stuff.t(i int not null primary key);
call sys_boot.mgmt.export_catalog_xmi(
    '${FARRAGO_HOME}/testgen/upgrade/FarragoCatalogDump.xmi');
