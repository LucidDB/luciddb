set schema 'mergetest';


-- Create and populate tables

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


--
-- functions on update and insert
--
insert into emptemp(empno, fname, lname, manager)
  (select empno, fname, lname, manager from emp where empno <= 105);

merge into emptemp et
using
  (select e.empno, e.fname, e.lname, e.manager, m.fname as mfn, m.lname as mln
   from emp e, emp m
   where e.manager = m.empno) as t
  on t.empno = et.empno
when matched then
  update set mfname = upper(substring(t.mfn from 1 for 1)),
             mlname = t.mln   
when not matched then
  insert (empno, fname, lname, manager, mfname, mlname)
  values (t.empno, t.fname, t.lname, t.manager, t.mfn, 
          upper(substring(t.mln from 1 for 1)));

select empno, fname, lname, manager, mfname, mlname from emptemp order by 1;

delete from emptemp;


--
-- case statement in ref table
--
insert into emptemp(empno, locid, sal)
  (select empno, locid, sal from emp where empno <= 105);

merge into emptemp et 
using (select empno,
              (case when locid='HQ' then sal + 2 
                    else sal
               end) as sal,
              locid
       from emp) as temp
  on et.empno = temp.empno
when matched then
  update set sal = temp.sal
when not matched then
  insert (empno, locid, sal)
  values (temp.empno, temp.locid, temp.sal);

select empno, locid, sal from emptemp order by 1;
delete from emptemp;

-- same query with case in update and insert
isnert into emptemp(empno, locid, sal) 
  (select empno, locid, sal from emp where empno <=105);

merge into emptemp et
using emp e on et.empno = e.empno
when matched then
  update set sal = 
    (case when e.locid='HQ' then e.sal + 2
          else e.sal
     end)
when not matched then
  insert (empno, locid, sal)
  values (e.empno, e.locid, 
          (case when e.locid='Hq' then e.sal + 2
                else e.sal
           end));

select empno, locid, sal from emptemp order by 1;
delete from emptemp;

--
-- case statement in update and insert
--
insert into emp (empno, sal, locid) values(200, 40000, 'MP');
insert into emp (empno, sal, locid) values(201, 60000, 'MP');

insert into emptemp(empno, fname, lname, deptno, sal)
  (select empno, fname, lname, deptno, sal from emp where empno <= 105 or empno >= 200);

merge into emptemp et
using emp e
  on (e.empno = et.empno)
when matched then
  update set sal = 
    (case when et.sal <= 40000 then et.sal + 1
          when et.sal <= 50000 then et.sal + 2
          else et.sal + 3
    end)
when not matched then
  insert (empno, fname, lname, deptno, sal)
  values (
    e.empno,
    upper(e.fname),
    upper(e.lname),
    deptno,
    (case when e.sal <= 50000 then e.sal + 4
          else e.sal + 5  
     end));

select empno, fname, lname, sal from emptemp order by 1;

delete from emp where empno >= 200;
delete from emptemp;




drop table emptemp;
