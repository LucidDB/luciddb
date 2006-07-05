set schema 'mergetest';

-- Create table
create table EMPTEMP (
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


create view tt as
  select * from emp;

merge into emptemp et
  using tt
  on et.empno = tt.empno
  when matched then
    update set sal = 0
  when not matched then
    insert (empno, locid, sal)
    values (tt.empno, tt.locid, tt.sal);

select empno, locid, sal from emptemp;
delete from emptemp;
drop view tt; 


--
--basic merge with inner join as ref table 
--
insert into emptemp (empno, locid, sal)
  (select empno, locid, sal from emp where empno<=105);

select empno, locid, city, sal from emptemp order by empno;

merge into emptemp es
  using
    (select * from emp e, location l
       where e.locid = l.locid) as temp
  on es.empno = temp.empno
  when matched then 
    update set city = temp.city,
               sal = es.sal + 1
  when not matched then
    insert (empno, locid, city, sal)
    values (temp.empno, temp.locid, temp.city, temp.sal + 2);

select empno, locid, city, sal from emptemp order by empno;
delete from emptemp;


--
--basic merge with full outer join as ref table
--
insert into emp (empno, locid, sal) 
  values (200, 'MV', 40000),(201, 'PA', 40000);
insert into emptemp (empno, locid, sal)
  (select empno, locid, sal from emp where empno<=105);

select empno, locid, city, sal from emptemp order by empno;
                            
merge into emptemp es
  using
    (select * from emp as e full outer join  location as l
       on e.locid = l.locid) as temp
  on es.empno = temp.empno
  when matched then
    update set sal = es.sal + 1,
               city = temp.city
  when not matched then
    insert (empno, locid, city, sal)
    values (temp.empno, temp.locid, temp.city, temp.sal + 2);

select empno, locid, city, sal from emptemp;
delete from emptemp;


--
--basic merge with left outer join as ref table
--
insert into emptemp (empno, locid, sal)
  (select empno, locid, sal from emp where empno<=105);
                            
merge into emptemp es
  using
    (select * from emp as e left outer join  location as l
       on e.locid = l.locid) as temp
  on es.empno = temp.empno
  when matched then
    update set sal = es.sal + 1,
               city = temp.city
  when not matched then
    insert (empno, locid, city, sal)
    values (temp.empno, temp.locid, temp.city, temp.sal + 2);

select empno, locid, city, sal from emptemp;
delete from emptemp;


--
--basic merge with right outer join as ref table
--
insert into emptemp (empno, locid, sal)
  (select empno, locid, sal from emp where empno<=105);
                        
merge into emptemp es
  using
    (select * from emp as e right outer join  location as l
       on e.locid = l.locid) as temp
  on es.empno = temp.empno
  when matched then
    update set sal = es.sal + 1,
               city = temp.city
  when not matched then
    insert (empno, locid, city, sal)
    values (temp.empno, temp.locid, temp.city, temp.sal + 2);

delete from emp where empno in (200,201);

select empno, locid, city, sal from emptemp;
delete from emptemp;


--
--basic merge with star join as ref table
--
insert into emptemp(empno, deptno, manager, locid)
  select empno, deptno, manager, locid from emp
  where empno<=105;

select empno, deptno, dname, manager, locid, city from emptemp;

merge into emptemp et
  using 
    (select * from emp e, dept d, emp m, location l 
       where e.manager = m.empno and
             e.locid = l.locid and
             e.deptno = d.deptno) as temp
  on et.empno = temp.empno
  when matched then
    update set dname = temp.dname,
               city = temp.city
  when not matched then
    insert (empno, deptno, dname, manager, locid, city)
    values (temp.empno, temp.deptno, temp.dname, temp.manager, temp.locid, temp.city);

select empno, deptno, dname, manager, locid, city from emptemp;
delete from emptemp;

--
-- reference table is inner join with filters
--
insert into emptemp (empno, locid, sal)
  (select empno, locid, sal from emp where empno<=105);

select empno, locid, city, sal from emptemp;

merge into emptemp es
  using
    (select * from emp e, location l
       where e.locid = l.locid and
             l.locid <> 'MP') as temp
  on es.empno = temp.empno
  when matched then 
    update set sal = es.sal + 1
  when not matched then
    insert (empno, locid, city, sal)
    values (temp.empno, temp.locid, temp.city, temp.sal + 2);

select empno, locid, city, sal from emptemp order by empno;
delete from emptemp;


--
-- filter condition brought out
-- does not work as of 7/04/06
--
--insert into emptemp (empno, locid, sal)
--  (select empno, locid, sal from emp where empno<=105);

--merge into emptemp es
--  using
--    (select * from emp e, location l
--       where e.locid = l.locid) as temp
--  on es.empno = temp.empno and 
--     temp.locid <>'MP'
--  when matched then 
--    update set sal = es.sal + 1
--  when not matched then
--    insert (empno, locid, city, sal)
--    values (temp.empno, temp.locid, temp.city, temp.sal + 2);

--select empno, locid, city, sal from emptemp order by empno;
--delete from emptemp;


-- clean up
drop table emptemp;