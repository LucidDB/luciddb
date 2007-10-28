-- $Id$
-- Test NOT NULL constraints

-- use Java calc because it gives better error messages
alter system set "calcVirtualMachine"='CALCVM_JAVA';

-- NOT NULL enforcement:  should fail
insert into sales.depts values (null,'Nullification');

-- NOT NULL enforcement:  should fail due to implicit NULL
insert into sales.emps(name,empno) values ('wael',300);

-- switch to LucidDB personality
alter session implementation set jar sys_boot.sys_boot.luciddb_plugin;
alter session set "logDir" = 'testlog';

-- NOT NULL row rejection, should succeed with errors
create schema test_rejection;
set schema 'TEST_REJECTION';
alter session set "errorMax" = 10;
create table test_not_null_rejection(col1 varchar(255), col2 integer not null);
insert into test_not_null_rejection values ('aa', 1), ('bb', 2), ('cc', null), ('dd', 4);
select * from sys_boot.mgmt.session_parameters_view
     where param_name = 'lastRowsRejected';
drop schema TEST_REJECTION cascade;
