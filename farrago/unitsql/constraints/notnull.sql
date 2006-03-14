-- $Id$
-- Test NOT NULL constraints

-- use Java calc because it gives better error messages
alter system set "calcVirtualMachine"='CALCVM_JAVA';

-- NOT NULL enforcement:  should fail
insert into sales.depts values (null,'Nullification');

-- NOT NULL enforcement:  should fail due to implicit NULL
insert into sales.emps(name,empno) values ('wael',300);
