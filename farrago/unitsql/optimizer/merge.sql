-- $Id$

----------------------------
-- Tests for MERGE statement
----------------------------

create schema m;
set schema 'm';
alter session implementation set jar sys_boot.sys_boot.luciddb_plugin;

create table emps(
    empno int not null, name varchar(20) not null, deptno int,
    gender char(1), city char(30), age int, salary int);
create table tempemps(
    t_empno int, t_name varchar(25), t_deptno int, t_gender char(1),
    t_city char(35), t_age int);
create table salarytable(empno int, salary int);

insert into emps(empno, name, deptno, gender, city, age, salary)
    select case when name = 'John' then 130 else empno end,
        name, deptno, gender, city, age, age * 900 from sales.emps;
select * from emps order by empno;
insert into tempemps
    select empno, name, deptno + 1, gender, coalesce(city, 'San Mateo'), age
        from emps;
insert into tempemps values(140, 'Barney', 10, 'M', 'San Mateo', 41);
insert into tempemps values(150, 'Betty', 20, 'F', 'San Francisco', 40);
select * from tempemps order by t_empno;

-- basic merge
merge into emps e
    using tempemps t on t.t_empno = e.empno
    when matched then
        update set deptno = t.t_deptno, city = upper(t.t_city),
            salary = salary * .25
    when not matched then
        insert (empno, name, age, gender, salary, city)
        values(t.t_empno, upper(t.t_name), t.t_age, t.t_gender, t.t_age * 1000,
            t.t_city);
select * from emps order by empno;

-- source select is a join
delete from emps where name in ('BARNEY', 'BETTY');
insert into salarytable values(100, 100000);
insert into salarytable values(110, 110000);
insert into salarytable values(120, 120000);
insert into salarytable values(130, 130000);
insert into salarytable values(140, 140000);
insert into salarytable values(150, 150000);
select * from emps order by empno;
select * from salarytable order by empno;

merge into emps e
    using (select s.empno, s.salary, t.* from salarytable s, tempemps t
        where t.t_empno = s.empno) t
    on t.t_empno = e.empno
    when matched then
        update set deptno = t.t_deptno-1, city = lower(t.t_city),
            salary = e.salary * 1.25
    when not matched then
        insert (empno, name, age, gender, salary, city)
        values(t.t_empno, upper(t.t_name), t.t_age, t.t_gender, t.salary * .15,
            t.t_city);
select * from emps order by empno;

-- no source rows; therefore, no rows should be affected
merge into emps
    using (select * from tempemps where t_deptno = 100) on t_empno = empno
    when matched then
        update set deptno = t_deptno, city = upper(t_city),
            salary = salary * .25
    when not matched then
        insert (empno, name, age, gender, salary, city)
        values(t_empno, upper(t_name), t_age, t_gender, t_age * 1000, t_city);
select * from emps order by empno;

-- only updates, no inserts
merge into emps
    using tempemps on t_empno = empno
    when matched then
        update set name = lower(name), deptno = t_deptno,
            city = upper(t_city),
            salary = salary * 10
    when not matched then
        insert (empno, name, age, gender, salary, city)
        values(t_empno, upper(t_name), t_age, t_gender, t_age * 1000, t_city);
select * from emps order by empno;

-- only inserts, no updates
delete from emps where empno >= 140;
select * from emps order by empno;
merge into emps
    using (select * from tempemps where t_empno >= 140) on t_empno = empno
    when matched then
        update set deptno = t_deptno, city = upper(t_city),
            salary = salary * .25
    when not matched then
        insert
            values(t_empno, upper(t_name), t_empno-100, t_gender, t_city, t_age,
                t_age * 1000);
select * from emps order by empno;

-- more than 1 row in the source table matches the target; per SQL2003, this
-- should return an error; currently, we do not return an error
insert into tempemps values(130, 'JohnClone', 41, 'M', 'Vancouver', null);
select * from tempemps order by t_empno, t_name;
merge into emps
    using (select * from tempemps where t_empno = 130) on t_empno = empno
    when matched then
        update set deptno = t_deptno, city = t_city, age = t_age,
            gender = t_gender
    when not matched then
        insert (empno, name, age, gender, salary, city)
        values(t_empno, upper(t_name), t_age, t_gender, t_age * 1000, t_city);
select * from emps order by empno, name;
delete from tempemps where t_name = 'JohnClone';
                
-- no insert substatement
insert into tempemps values(160, 'Pebbles', 60, 'F', 'Foster City', 2);
select * from tempemps order by t_empno;
merge into emps
    using (select * from tempemps) on t_empno = empno
    when matched then
        update set name = t_name, city = t_city;
select * from emps order by empno, name;

-- no update substatement
merge into emps
    using (select * from tempemps) on t_empno = empno
    when not matched then
        insert values (t_empno, t_name, t_deptno, t_gender, t_city, t_age, 0);
select * from emps order by empno, name;

-- simple update via a merge
delete from emps where empno = 130;
select * from emps order by empno;
merge into emps e1
    using (select * from emps) e2 on e1.empno = e2.empno
    when matched then
        update set age = e1.age + 1;
select * from emps order by empno;

-- the updates in the following merges are no-ops
-- verify that no updates have occurred by ensuring that the rids haven't
-- changed
select lcs_rid(empno), * from emps order by empno;
merge into emps e
    using tempemps t on t.t_empno = e.empno
    when matched then
        update set deptno = deptno, city = city,
            salary = salary
    when not matched then
        insert (empno, name, age, gender, salary, city)
        values(t.t_empno, upper(t.t_name), t.t_age, t.t_gender, t.t_age * 1000,
            t.t_city);
select lcs_rid(empno), * from emps order by empno;

merge into emps e1
    using (select * from emps) e2 on e1.empno = e2.empno
    when matched then
        update set age = e1.age + e1.deptno - e1.deptno;
select lcs_rid(empno), * from emps order by empno;

-----------------
-- Explain output
-----------------
!set outputformat csv

-- source table reference is a table
explain plan for
merge into emps e
    using tempemps t on t.t_empno = e.empno
    when matched then
        update set deptno = t.t_deptno, city = upper(t.t_city),
            salary = salary * .25
    when not matched then
        insert (empno, name, age, gender, salary, city)
        values(t.t_empno, upper(t.t_name), t.t_age, t.t_gender, t.t_age * 1000,
            t.t_city);

-- source table reference is a single table select
explain plan for
merge into emps e
    using (select * from tempemps where t_deptno = 100) t on t.t_empno = e.empno
    when matched then
        update set deptno = t.t_deptno, city = upper(t.t_city),
            salary = salary * .25
    when not matched then
        insert (empno, name, age, gender, salary, city)
        values(t.t_empno, upper(t.t_name), t.t_age, t.t_gender, t.t_age * 1000,
            t.t_city);

-- source table reference is a join
explain plan for
merge into emps e
    using (select s.empno, s.salary, t.* from salarytable s, tempemps t
        where t.t_empno = s.empno) t
    on t.t_empno = e.empno
    when matched then
        update set deptno = t.t_deptno, city = upper(t.t_city),
            salary = e.salary * .25
    when not matched then
        insert (empno, name, age, gender, salary, city)
        values(t.t_empno, upper(t.t_name), t.t_age, t.t_gender, t.salary * .15,
            t.t_city);

-- columns aren't qualified
explain plan for
merge into emps
    using (select * from tempemps where t_deptno = 100) on t_empno = empno
    when matched then
        update set deptno = t_deptno, city = upper(t_city),
            salary = salary * .25
    when not matched then
        insert (empno, name, age, gender, salary, city)
        values(t_empno, upper(t_name), t_age, t_gender, t_age * 1000, t_city);
explain plan for
merge into emps
    using tempemps on t_empno = empno
    when matched then
        update set deptno = t_deptno, city = upper(t_city),
            salary = salary * .25
    when not matched then
        insert (empno, name, age, gender, salary, city)
        values(t_empno, upper(t_name), t_age, t_gender, t_age * 1000, t_city);

-- no target column list in the insert
explain plan for
merge into emps
    using (select * from tempemps where t_deptno = 100) on t_empno = empno
    when matched then
        update set deptno = t_deptno, city = upper(t_city),
            salary = salary * .25
    when not matched then
        insert
            values(t_empno, upper(t_name), null, t_gender, t_city, t_age,
                t_age * 1000);
                
-- no target column list in the insert, but the types of the source insert
-- expressions match the target
explain plan for
merge into emps
    using (select * from tempemps where t_deptno = 100) on t_empno = empno
    when matched then
        update set deptno = t_deptno, city = upper(t_city),
            salary = salary * .25
    when not matched then
        insert
            values(t_empno, cast(upper(t_name) as varchar(20)), null, t_gender,
                cast(t_city as char(30)), t_age, t_age * 1000);

-- no insert substatement
explain plan for
merge into emps
    using (select * from tempemps) on t_empno = empno
    when matched then
        update set name = t_name, city = t_city;

-- no update substatement
explain plan for
merge into emps
    using (select * from tempemps) on t_empno = empno
    when not matched then
        insert values (t_empno, t_name, t_deptno, t_gender, t_city, t_age, 0);

-- no-op updates
explain plan for
merge into emps e
    using tempemps t on t.t_empno = e.empno
    when matched then
        update set deptno = deptno, city = city,
            salary = salary
    when not matched then
        insert (empno, name, age, gender, salary, city)
        values(t.t_empno, upper(t.t_name), t.t_age, t.t_gender, t.t_age * 1000,
            t.t_city);
-- note that in this case (no-op update only), the check for non-update filter
-- should be pushed down to the scan since there is no outer join
explain plan for
merge into emps e1
    using (select * from emps) e2 on e1.empno = e2.empno
    when matched then
        update set age = e1.age + e1.deptno - e1.deptno;

--------------
-- Error cases
--------------
-- invalid target table
explain plan without implementation for
merge into emp e
    using tempemps t on t.t_empno = e.empno
    when matched then
        update set deptno = t.t_deptno, city = upper(t.t_city),
            salary = salary * .25
    when not matched then
        insert (empno, name, age, gender, salary, city)
        values(t.t_empno, upper(t.t_name), t.t_age, t.t_gender, t.t_age * 1000,
            t.t_city);

-- invalid table in using clause
explain plan without implementation for
merge into emps e
    using tempemp t on t.t_empno = e.empno
    when matched then
        update set deptno = t.t_deptno, city = upper(t.t_city),
            salary = salary * .25
    when not matched then
        insert (empno, name, age, gender, salary, city)
        values(t.t_empno, upper(t.t_name), t.t_age, t.t_gender, t.t_age * 1000,
            t.t_city);

-- invalid column reference in on clause
explain plan without implementation for
merge into emps e
    using tempemps t on t.empno = e.empno
    when matched then
        update set deptno = t.t_deptno, city = upper(t.t_city),
            salary = salary * .25
    when not matched then
        insert (empno, name, age, gender, salary, city)
        values(t.t_empno, upper(t.t_name), t.t_age, t.t_gender, t.t_age * 1000,
            t.t_city);

-- invalid column reference in update set clause
explain plan without implementation for
merge into emps e
    using tempemps t on t.t_empno = e.empno
    when matched then
        update set t_deptno = t.t_deptno, city = upper(t.t_city),
            salary = salary * .25
    when not matched then
        insert (empno, name, age, gender, salary, city)
        values(t.t_empno, upper(t.t_name), t.t_age, t.t_gender, t.t_age * 1000,
            t.t_city);

-- invalid column reference in update set expr
explain plan without implementation for
merge into emps e
    using tempemps t on t.t_empno = e.empno
    when matched then
        update set deptno = t.t_deptno, city = upper(e.t_city),
            salary = salary * .25
    when not matched then
        insert (empno, name, age, gender, salary, city)
        values(t.t_empno, upper(t.t_name), t.t_age, t.t_gender, t.t_age * 1000,
            t.t_city);

-- ambiguous column
explain plan without implementation for
merge into emps e
    using (select s.empno, s.salary, t.* from salarytable s, tempemps t
        where t.t_empno = s.empno) t
    on t.t_empno = e.empno
    when matched then
        update set deptno = t.t_deptno, city = upper(t.t_city),
            salary = salary * .25
    when not matched then
        insert (empno, name, age, gender, salary, city)
        values(t.t_empno, upper(t.t_name), t.t_age, t.t_gender, t.salary * .15,
            t.t_city);

-- invalid column reference in insert target list
explain plan without implementation for
merge into emps e
    using tempemps t on t.t_empno = e.empno
    when matched then
        update set deptno = t.t_deptno, city = upper(t.t_city),
            salary = salary * .25
    when not matched then
        insert (empno, name, age, t_gender, salary, city)
        values(t.t_empno, upper(e.t_name), t.t_age, t.t_gender, t.t_age * 1000,
            t.t_city);

-- invalid column reference in insert values list
explain plan without implementation for
merge into emps e
    using tempemps t on t.t_empno = e.empno
    when matched then
        update set deptno = t.t_deptno, city = upper(t.t_city),
            salary = salary * .25
    when not matched then
        insert (empno, name, age, gender, salary, city)
        values(t.t_empno, upper(e.t_name), t.t_age, t.t_gender, t.t_age * 1000,
            t.t_city);
explain plan without implementation for
merge into emps e
    using tempemps t on t.t_empno = e.empno
    when matched then
        update set deptno = t.t_deptno, city = upper(t.t_city),
            salary = salary * .25
    when not matched then
        insert (empno, name, age, gender, salary, city)
        values(t.t_empno, upper(name), t.t_age, t.t_gender, t.t_age * 1000,
            t.t_city);

-- mismatch in number of column in values clause
explain plan without implementation for
merge into emps e
    using tempemps t on t.t_empno = e.empno
    when matched then
        update set deptno = t.t_deptno, city = upper(t.t_city),
            salary = salary * .25
    when not matched then
        insert (empno, name, age, gender, salary, city)
        values(t.t_empno, upper(t.t_name), t.t_age, t.t_gender, t.t_age * 1000);

-- mismatch in types in values clause
explain plan without implementation for
merge into emps e
    using tempemps t on t.t_empno = e.empno
    when matched then
        update set deptno = t.t_deptno, city = upper(t.t_city),
            salary = salary * .25
    when not matched then
        insert (empno, name, age, gender, salary, city)
        values(t.t_empno, upper(t.t_name), t.t_name, t.t_gender, t.t_age * 1000,
            t.t_city);

-- LucidDb doesn't support UPDATE
update emps set name = 'Foobar';

-- Farrago doesn't support MERGE
alter session implementation set default;
merge into emps e
    using tempemps t on t.t_empno = e.empno
    when matched then
        update set deptno = t.t_deptno, city = upper(t.t_city),
            salary = salary * .25
    when not matched then
        insert (empno, name, age, gender, salary, city)
        values(t.t_empno, upper(t.t_name), t.t_age, t.t_gender, t.t_age * 1000,
            t.t_city);

