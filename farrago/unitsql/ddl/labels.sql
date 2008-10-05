-- $Id$
-- Test creating and dropping labels

create label label0;
drop label label0;

-- Labels can only be created and dropped in the LucidDB personality.  Switch
-- to that personality so we can exercise the create/drop label code in Farrago
-- tests.  Note, however, that we cannot test setting a label in a Farrago
-- test, even if the personality is set to LucidDB because you need to have
-- a snapshot-enabled Fennel engine.
alter session implementation set jar sys_boot.sys_boot.luciddb_plugin;

create label label0;
create label label1;
create label l0alias from label label0;
create label l0alalias from label l0alias;
create label l1alias from label label1;
select label_name, parent_label_name, remarks
    from sys_boot.mgmt.dba_labels_internal order by label_name;

-- should fail, same name
create label label0 description 'this one fails';
create label label1 from label label0;

-- replace the existing label
create or replace label label0 description 'the new one';
select label_name, parent_label_name, remarks
    from sys_boot.mgmt.dba_labels_internal order by label_name;

-- should fail, non-existent label
create label bad from label foo;

-- should fail, circular labels
create label label2 from label label2;
create or replace label label0 from label l0alalias;

-- can't set label because snapshot reads aren't supported in Farrago
alter session set "label" = 'LABEL0';

-- drop a non-existent label
drop label nolabel;

-- the following should fail
drop label label0;
drop label label0 restrict;
drop label l0alias;
drop label l0alias restrict;

drop label l0alalias;
select label_name, parent_label_name, remarks
    from sys_boot.mgmt.dba_labels_internal order by label_name;
drop label label0 cascade;
select label_name, parent_label_name, remarks
    from sys_boot.mgmt.dba_labels_internal order by label_name;
drop label l1alias cascade;
select label_name, parent_label_name, remarks
    from sys_boot.mgmt.dba_labels_internal;
drop label label1;
select label_name, parent_label_name, remarks
    from sys_boot.mgmt.dba_labels_internal;
