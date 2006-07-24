set schema 'mergetest';

-- Create and populate tables

create table EMPTEMP1 (
  EMPNO integer not null,
  FNAME varchar(15),
  LNAME varchar(15),
  SEX char(1),
  DEPTNO integer,
  DNAME varchar(15),
  MANAGER integer,
  MFNAME varchar(15),
  MLNAME varchar(15),
  LOCID char(2),
  CITY varchar(8),
  SAL decimal(10, 2),
  COMMISION decimal(10, 2),
  HOBBY varchar(25)
);

create table EMPTEMP2 (
  EMPNO integer not null,
  FNAME varchar(15),
  LNAME varchar(15),
  SEX char(1),
  DEPTNO integer,
  DNAME varchar(15),
  MANAGER integer,
  MFNAME varchar(15),
  MLNAME varchar(15),
  LOCID char(2),
  CITY varchar(8),
  SAL decimal(10, 2),
  COMMISION decimal(10, 2),
  HOBBY varchar(25)
);

insert into emptemp1(EMPNO, FNAME, LNAME, SEX, DEPTNO, MANAGER, LOCID, SAL, 
  COMMISION, HOBBY) 
  select * from emp where empno<=105;
insert into emptemp2 select * from emptemp1;
insert into emptemp1 (empno, locid, sal) values (201, 'FM', 40000);
insert into emptemp1 (empno, locid, sal) values (202, 'NW', 60000);

-- update 101~105, insert 106~110,201,202
merge into emptemp2 as tr
using (select * from emp UNION 
       select EMPNO, FNAME, LNAME, SEX, DEPTNO, MANAGER, LOCID, SAL, 
              COMMISION, HOBBY from emptemp1) as rf
on rf.empno = tr.empno
when matched then
  update set sal = tr.sal + 1
when not matched then
  insert (empno, locid, sal) values (rf.empno, rf.locid, rf.sal + 2);

select (empno, locid, sal) from emptemp2;

-- update 101~105
delete from emptemp2;
insert into emptemp2
  select * from emp where empno<=105;
merge into emptemp2 as tr
using (select * from emp INTERSECT 
       select EMPNO, FNAME, LNAME, SEX, DEPTNO, MANAGER, LOCID, SAL, 
              COMMISION, HOBBY from emptemp1) as rf
on rf.empno = tr.empno
when matched then
  update set sal = tr.sal + 1
when not matched then
  insert (empno, locid, sal) values (rf.empno, rf.locid, rf.sal + 2);

select (empno, locid, sal) from emptemp2;

-- insert 201,202
delete from emptemp2;
insert into emptemp2
  select * from emp where empno<=105;
merge into emptemp2 as tr
using (select EMPNO, FNAME, LNAME, SEX, DEPTNO, MANAGER, LOCID, SAL, 
              COMMISION, HOBBY from emptemp1 
       EXCEPT select * from emp) as rf
on rf.empno = tr.empno
when matched then
  update set sal = tr.sal + 1
when not matched then
  insert (empno, locid, sal) values (rf.empno, rf.locid, rf.sal + 2);

select (empno, locid, sal) from emptemp2;

-- insert or update those having locid='HQ' and sex= 'M' using intersect
delete from emptemp2;
insert into emptemp2
  select * from emp where empno<=105;
merge into emptemp2 as tr
using
  (select * from emp where locid='HQ' intersect
   select * from emp where sex='M') as rf
on rf.empno = tr.empno
when matched then
  update set sal = tr.sal + 1
when not matched then
  insert (empno, locid, sal + 2) from emptemp2;

select (empno, locid, sal) from emptemp2;

-- same query using AND filter
delete from emptemp2;
insert into emptemp2
  select * from emp where empno<=105;
merge into emptemp2 as tr
using (select * from emp where locid='HQ' and sex='M') as rf
on rf.empno = tr.empno
when matched then
  update set sal = tr.sal + 1
when not matched then
  insert (empno, locid, sal + 2) from emptemp2;

select (empno, locid, sal) from emptemp2;

-- same query using except
delete from emptemp2;
insert into emptemp2
  select * from emp where empno<=105;
merge into emptemp2 as tr
using
  (select * from emp where locid='HQ' except
   select * from emp where sex='F') as rf
on rf.empno = tr.empno
when matched then
  update set sal = tr.sal + 1
when not matched then
  insert (empno, locid, sal + 2) from emptemp2;

select (empno, locid, sal) from emptemp2;



-- clean up
drop table emptemp1;
drop table emptemp2;