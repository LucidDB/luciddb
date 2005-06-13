-- $Id$
-- Test NOT NULL constraints

-- FIXME jvs 12-June-2005:  the error messages resulting from both
-- statements below are unhelpful.

-- NOT NULL enforcement:  should fail
insert into sales.depts values (null,'Nullification');

-- NOT NULL enforcement:  should fail due to implicit NULL
insert into sales.emps(name,empno) values ('wael',300);
