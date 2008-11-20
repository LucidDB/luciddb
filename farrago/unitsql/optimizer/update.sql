-- $Id$

-----------------------------
-- Tests for UPDATE statement
-----------------------------
set schema 'upsales';
create schema upsales;
create table upemps(
  empno int primary key,
  name varchar(10),
  deptno int);
insert into upemps
  values
    (1, 'Fred', 20),
    (2, 'Bill', 10),
    (3, 'Joe', 10);
select * from upemps;
-- update with no where; modifying primary key
-- currently gives 'Duplicate key detected' - this is a bug
update upemps
  set empno = empno + 1;
select * from upemps;
-- as above, but large gap to avoid bug
update upemps
  set empno = empno + 5;
select * from upemps;
-- update two columns and use a where clause
update upemps
  set empno = empno + 5,
    name = upper(name)
where deptno = 10;
select * from upemps;
-- populate using a correlated subquery
update upemps as u
  set empno = empno + (
    select count(*)
    from upemps
    where deptno = u.deptno);
select * from upemps;
-- drive from subquery in where clause
update upemps
  set name = lower(name),
    deptno = -deptno
where deptno in (select min(deptno) from upemps);
select * from upemps;
-- update column from scalar subquery
-- note that if query returns 0 rows, null is assigned to column
-- note that we use correlating variable 'u' but without 'as' this time
update upemps u
  set name = (
    select name from upemps where empno = u.empno + 1);
select * from upemps;
-- update using values as subquery
update upemps
  set deptno = (values (10));
select * from upemps;
-- restore original data
truncate table upemps;
insert into upemps
  values
    (1,'Fred',20),
    (2,'Bill', 10),
    (3,'Joe',10);
-- attempt to update same column twice; should fail
update upemps
  set empno=10, empno=20;
-- same for insert
insert into upemps(empno,empno) values (10,20);
-- update two columns from same query
-- oracle supports this but we do not; expect validation error
update upemps
  set (empno, deptno) = (values (empno, deptno));
-- End update.sql

