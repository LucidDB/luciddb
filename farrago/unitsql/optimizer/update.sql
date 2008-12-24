-- $Id$

-----------------------------
-- Tests for UPDATE statement
-----------------------------

-- display rowcounts so diff can verify them
!set silent off
!set showtime off

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
select * from upemps order by empno;
-- update with no where; modifying primary key
-- currently gives 'Duplicate key detected' - this is a bug
update upemps
  set empno = empno + 1;
select * from upemps order by empno;
-- as above, but large gap to avoid bug
update upemps
  set empno = empno + 5;
select * from upemps order by empno;
-- update two columns and use a where clause
update upemps
  set empno = empno + 5,
    name = upper(name)
where deptno = 10;
select * from upemps order by empno;
-- populate using a correlated subquery
update upemps as u
  set empno = empno + (
    select count(*)
    from upemps
    where deptno = u.deptno);
select * from upemps order by empno;
-- drive from subquery in where clause
update upemps
  set name = lower(name),
    deptno = -deptno
where deptno in (select min(deptno) from upemps);
select * from upemps order by empno;
-- update column from scalar subquery
-- note that if query returns 0 rows, null is assigned to column
-- note that we use correlating variable 'u' but without 'as' this time
update upemps u
  set name = (
    select name from upemps where empno = u.empno + 1);
select * from upemps order by empno;
-- update using values as subquery
update upemps
  set deptno = (values (10));
select * from upemps order by empno;
-- no-op update; FTRS is not smart enough to detect that nothing
-- really changed, so it reports 3 rows affected; this is actually
-- the expected behavior according to SQL:2003
update upemps
  set deptno = (values (10));
select * from upemps order by empno;
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

drop table upemps;

-- Test LucidDB rewrite from UPDATE to MERGE

alter session implementation set jar sys_boot.sys_boot.luciddb_plugin;

create table upemps(
  empno int primary key,
  name varchar(10),
  deptno int);
insert into upemps
  values
    (1, 'Fred', 20),
    (2, 'Bill', 10),
    (3, 'Joe', 10);
select * from upemps order by empno;

-- full table update
update upemps
  set empno = empno + 5;
-- make sure lastUpsertRowsInserted comes out zero
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastUpsertRowsInserted';
select * from upemps order by empno;

-- update two columns and use a where clause
update upemps
  set empno = empno + 5,
    name = upper(name)
where deptno = 10;
select * from upemps order by empno;

-- FIXME jvs 12-Dec-2008:  LucidDB can't currently handle
-- subqueries combined with UPDATE

-- -- populate using a correlated subquery
-- update upemps as u
--   set empno = empno + (
--     select count(*)
--     from upemps
--     where deptno = u.deptno);
-- select * from upemps order by empno;
-- -- drive from subquery in where clause
-- update upemps
--   set name = lower(name),
--     deptno = -deptno
-- where deptno in (select min(deptno) from upemps);
-- select * from upemps order by empno;

-- -- update column from scalar subquery
-- update upemps u
--   set name = (
--     select name from upemps where empno = u.empno + 1);
-- select * from upemps order by empno;

-- update using values as subquery
update upemps
  set deptno = (values (10));
select * from upemps order by empno;
-- no-op update; LucidDB is smart enough to detect that nothing
-- really changed, so it reports no rows affected; this
-- is non-conforming behavior according to SQL:2003
update upemps
  set deptno = (values (10));

-- attempt to update same column twice; should fail
update upemps
  set empno=10, empno=20;

truncate table upemps;
insert into upemps
  values
    (1,'Fred',20),
    (2,'Bill', 10),
    (3,'Joe',10);

-- modify primary key overlapping:  LucidDB handles this correctly
update upemps
  set empno = empno + 1;
select * from upemps order by empno;

-- full table update with alias
update upemps u
  set empno = u.empno + 5;
select * from upemps order by empno;

-- full table update with unused alias and "as" noiseword
update upemps as u
  set empno = empno + 5;
select * from upemps order by empno;

-- update two columns and use a where clause with alias
update upemps u
  set empno = u.empno + 5,
    name = upper(u.name)
where u.deptno = 10;
select * from upemps order by empno;

-- NOTE jvs 12-Dec-2008:  we can't test execution failures which would lead
-- to rollback here because of the schizo Farrago/LucidDB setup used
-- for these tests; for that, see luciddb/test/sql/txn/rollback.sql

-- but we can test recoverable failures
alter session set "errorMax" = 100;
alter session set "logDir" = 'testlog';

-- this will lead to division by zero for rows with deptno=10
update upemps u set deptno = 7 / (deptno - 10);
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastRowsRejected';
select * from upemps order by empno;

-- can't update primary key in error tolerant mode (this test is OK
-- since it's caught by the validator)
update upemps set empno = empno+1;

alter session set "errorMax" = 0;

---------------------------------------------------------------
-- Explain output to make sure join elimination is taking place
---------------------------------------------------------------
!set outputformat csv

explain plan for
update upemps
  set empno = empno + 5;

explain plan for
update upemps
  set empno = empno + 5,
    name = upper(name)
where deptno = 10;

explain plan for
update upemps
  set deptno = (values (10));

!set silent on
-- End update.sql

