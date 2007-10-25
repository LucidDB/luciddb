-- $Id$
-- Various constraints testing

-- use Java calc because it gives better error messages
alter system set "calcVirtualMachine"='CALCVM_JAVA';

-- switch to LucidDB personality
alter session implementation set jar sys_boot.sys_boot.luciddb_plugin;
alter session set "logDir" = 'testlog';

-- violate more than one kind of constraint, should succeed with errors
create schema test_multiple;
set schema 'TEST_MULTIPLE';
alter session set "errorMax" = 10;
create table T1(COL1 varchar(255) constraint col1_unique unique, COL2 smallint not null);
create table T2(COL1 varchar(255), COL2 integer);
insert into T2 values ('aa', 1), ('bb', 2), ('cc', null), ('dd', 4), ('ee', 100000), ('aa', 6);
merge into T1 using T2 on T2.COL1 = T1.COL1 
        when matched then update set COL1 = T2.COL1, COL2 = T2.COL2 
        when not matched then insert (COL1, COL2) values (T2.COL1, T2.COL2);
select * from sys_boot.mgmt.session_parameters_view
     where param_name = 'lastRowsRejected';
drop schema TEST_MULTIPLE cascade;
