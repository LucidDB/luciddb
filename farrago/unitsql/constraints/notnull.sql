-- $Id$
-- Test NOT NULL constraints

-- NOT NULL enforcement:  should fail
insert into sales.depts values (null,'Nullification');
