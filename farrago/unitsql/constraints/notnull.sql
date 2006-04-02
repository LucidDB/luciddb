-- $Id$
-- Test NOT NULL constraints

-- use Java calc because it gives better error messages
alter system set "calcVirtualMachine"='CALCVM_JAVA';

-- NOT NULL enforcement:  should fail
insert into sales.depts values (null,'Nullification');

-- NOT NULL enforcement:  should fail due to implicit NULL
insert into sales.emps(name,empno) values ('wael',300);

-- test inserting multiple values, some wih nulls. Only the nulls should be rejected
-- REVIEW: the java calc rejects all, the fennel calc rejects only the nulls.
-- Which is correct?
create table sales.fake(
    deptno integer not null primary key, 
    name varchar(128) not null);
insert into sales.fake values (null, 'Null'),(100, 'Fish'), (null, 'Null Again'), (200, 'Fowl'), (null, 'and again');
select * from sales.fake;

-- repeat with Fennel calc
alter system set "calcVirtualMachine"='CALCVM_FENNEL';
insert into sales.fake values (null, 'Null');
insert into sales.fake values (null, 'Null Again'),(100, 'Fish'), (null, 'again'), (200, 'Fowl'), (null, 'and again');
select * from sales.fake;

drop table sales.fake;
