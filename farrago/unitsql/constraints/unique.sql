-- $Id$
-- Test UNIQUE and PRIMARY KEY constraints

-- should fail with duplicate
insert into sales.depts values (30,'Fudge Factor Estimation');

-- should fail with duplicate
update sales.depts set name='Sales' where deptno=30;
