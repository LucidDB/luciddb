-- $Id$
-- Test NOT NULL constraints

-- NOT NULL enforcement:  should fail
insert into sales.depts values (null,'Nullification');

-- NOT NULL enforcement:  should fail due to implicit NULL
insert into sales.emps(name,empno) values ('wael',300);
