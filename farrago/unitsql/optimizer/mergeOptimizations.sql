-- $Id$

--------------------------------------------------------------------------------
-- Tests optimizations that can be applied to MERGE statements.
--------------------------------------------------------------------------------

create schema mo;
set schema 'mo';
alter session implementation set jar sys_boot.sys_boot.luciddb_plugin;

create table emps(
    empno int not null unique, name varchar(20) unique not null, deptno int,
    gender char(1), city char(30), age int, salary numeric(10,2));
create index ideptno on emps(deptno);
create index icity on emps(city);
create table tempemps(
    t_empno int, t_name varchar(25), t_deptno int, t_gender char(1),
    t_city char(35), t_age int);
create table salarytable(empno int, salary int);

insert into emps(empno, name, deptno, gender, city, age, salary)
    select case when name = 'John' then 130 else empno end,
        name, deptno, gender, city, age, age * 900 from sales.emps;
select * from emps order by empno;
insert into tempemps values(140, 'Barney', 10, 'M', 'San Mateo', 41);
insert into tempemps values(150, 'Betty', 20, 'F', 'San Francisco', 40);
insert into tempemps
    select empno, name, deptno + 1, gender, coalesce(city, 'San Mateo'), age
        from emps;
select * from tempemps order by t_empno;
insert into salarytable values(100, 100000);
insert into salarytable values(110, 110000);
insert into salarytable values(120, 120000);
insert into salarytable values(130, 130000);
insert into salarytable values(140, 140000);
insert into salarytable values(150, 150000);
select * from salarytable order by empno;

--------------------------------------
-- Test merges that are really updates
--------------------------------------

merge into emps e1 using emps e2 
    on e1.empno = e2.empno
    when matched then update set deptno = e2.deptno + 100;
select * from emps order by empno;

-- filter on source
merge into emps e1 using emps e2 
    on e1.empno = e2.empno and e2.city is null
    when matched then update set city = 'UNKNOWN';
select * from emps order by empno;

-- filter on source and target
merge into emps e1 using emps e2
    on e1.empno = e2.empno and e1.age > 40 and e2.salary < 50000
    when matched then update set salary = e2.salary * 1.1;
select * from emps order by empno;

-- one of the join filters is on a nullable column, so not all rows should be
-- updated
merge into emps e1 using emps e2
    on e1.empno = e2.empno and e1.age = e2.age
    when matched then update set name = upper(e2.name);
select * from emps order by empno;

merge into emps e1 using emps e2
    on e1.empno = e2.empno and e1.age = e2.age and e2.city like 'San%'
    when matched then update set deptno = e1.deptno - 100;
select * from emps order by empno;

-- join filter contains non-sargable filter
merge into emps e1 using emps e2
    on e1.empno = e2.empno and abs(e1.age) = abs(e2.age)
    when matched then update set name = lower(e2.name);
select * from emps order by empno;

-- source contains projection and/or filtering
merge into emps e1 using (select * from emps where age < 30) e2
    on e1.empno = e2.empno
    when matched then update set salary = e2.salary * .9;
select * from emps order by empno;

merge into emps e1 
    using (select salary * .9 as newSalary, empno from emps) e2
    on e1.empno = e2.empno
    when matched then update set salary = e2.newSalary;
select * from emps order by empno;

merge into emps e1 
    using (select salary * 1.1 as newSalary, empno from emps
            where city <> 'UNKNOWN') e2
    on e1.empno = e2.empno
    when matched then update set salary = e2.newSalary + 1000;
select * from emps order by empno;

merge into emps e1 using (select * from emps where city = 'UNKNOWN') e2
    on e2.empno = e1.empno and e1.gender = 'F'
    when matched then update set name = upper(e1.name);
select * from emps order by empno;

-- source has a join
merge into emps e1
    using (select e.empno, s.salary from emps e, salarytable s
            where e.empno = s.empno) e2
    on e1.empno = e2.empno
    when matched then update set salary = e2.salary;
select * from emps order by empno;

-----------------
-- Explain output
-----------------
!set outputformat csv

-------------------------------------------------------
-- Make sure updates written as merges avoid self-joins
-------------------------------------------------------

explain plan for
merge into emps e1 using emps e2 
    on e1.empno = e2.empno
    when matched then update set deptno = e2.deptno + 100;

explain plan for
merge into emps e1 using emps e2 
    on e1.empno = e2.empno and e2.city is null
    when matched then update set city = 'UNKOWN';

explain plan for
merge into emps e1 using emps e2
    on e1.empno = e2.empno and e1.age > 40 and e2.salary < 50000
    when matched then update set salary = e2.salary * 1.1;

explain plan for
merge into emps e1 using emps e2
    on e1.empno = e2.empno and e1.age = e2.age
    when matched then update set name = upper(e2.name);

explain plan for
merge into emps e1 using emps e2
    on e1.empno = e2.empno and e1.age = e2.age and e2.city like 'San%'
    when matched then update set deptno = e1.deptno - 100;

explain plan for
merge into emps e1 using emps e2
    on e1.empno = e2.empno and abs(e1.age) = abs(e2.age)
    when matched then update set name = lower(e2.name);

explain plan for
merge into emps e1 using (select * from emps where age < 30) e2
    on e1.empno = e2.empno
    when matched then update set salary = e2.salary * .9;

explain plan for
merge into emps e1 
    using (select salary * .9 as newSalary, empno from emps) e2
    on e1.empno = e2.empno
    when matched then update set salary = e2.newSalary;

explain plan for
merge into emps e1 
    using (select salary * 1.1 as newSalary, empno from emps
            where city <> 'UNKNOWN') e2
    on e1.empno = e2.empno
    when matched then update set salary = e2.newSalary + 1000;

explain plan for
merge into emps e1 using (select * from emps where city = 'UNKNOWN') e2
    on e2.empno = e1.empno and e1.gender = 'F'
    when matched then update set name = upper(e1.name);

-- source has a join
explain plan for
merge into emps e1
    using (select e.empno, s.salary from emps e, salarytable s
            where e.empno = s.empno) e2
    on e1.empno = e2.empno
    when matched then update set salary = e2.salary;

----------------------------------------------
-- Self-joins cannot be removed in these cases
----------------------------------------------

-- not update-only
explain plan for
merge into emps e1 using emps e2
    on e1.empno = e2.empno
    when matched then update set salary = e2.salary * .9
    when not matched then 
        insert (empno, name, age, gender, salary, city)
        values (e2.empno, e2.name, e2.age, e2.gender, e2.salary, e2.city);

-- non-unique keys in ON condition
explain plan for
merge into emps e1 using emps e2
    on e1.deptno = e2.deptno
    when matched then update set age = e2.age * 2;

-- different columns
explain plan for
merge into emps e1 using (select deptno as empno, age from emps) e2
    on e1.empno = e2.empno 
    when matched then update set age = e2.age - 1;

-- unique keys but different columns
explain plan for
merge into emps e1
    using (select cast(empno as varchar(10)) as empno, age from emps) e2
    on e2.empno = e1.name
    when matched then update set age = e2.age - 1;

-- join condition contains derived key from source
explain plan for
merge into emps e1 using (select age, empno + 1 as empno from emps) e2
    on e1.empno = e2.empno
    when matched then update set age = e2.age + 1;

explain plan for
merge into emps e1 using (select age, empno + deptno as empno from emps) e2
    on e1.empno = e2.empno
    when matched then update set age = e2.age + 1;

-- no equality filter
explain plan for 
merge into emps e1 using emps e2
    on e1.empno >= e2.empno and e1.empno <= e2.empno
    when matched then update set age = e2.age / 10;

-- source has a group by
explain plan for
merge into emps e1 using (select empno from emps group by empno) e2
    on e1.empno = e2.empno 
    when matched then update set age = e1.age / 10;

-- different tables
create schema mo2;
create table mo2.emps(
    empno int not null unique, name varchar(20) unique not null, deptno int,
    gender char(1), city char(30), age int, salary numeric(10,2));
explain plan for
merge into emps e1 using mo2.emps e2
    on e1.empno = e2.empno
    when matched then update set deptno = e2.deptno * 1;
