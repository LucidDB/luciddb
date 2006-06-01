-- $Id$

----------------------------
-- Tests for MERGE statement
----------------------------

create schema m;
set schema 'm';

create table emps(
    empno int, name varchar(20), deptno int, gender char(1), city char(30),
    age int, salary int) server sys_column_store_data_server;
create table tempemps(
    t_empno int, t_name varchar(25), t_deptno int, t_gender char(1),
    t_city char(35), t_age int) server sys_column_store_data_server;
create table salarytable(empno int, salary int)
    server sys_column_store_data_server;

!set outputformat csv

-- source table reference is a table
explain plan without implementation for
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
explain plan without implementation for
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
explain plan without implementation for
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
explain plan without implementation for
merge into emps
    using (select * from tempemps where t_deptno = 100) on t_empno = empno
    when matched then
        update set deptno = t_deptno, city = upper(t_city),
            salary = salary * .25
    when not matched then
        insert (empno, name, age, gender, salary, city)
        values(t_empno, upper(t_name), t_age, t_gender, t_age * 1000, t_city);
explain plan without implementation for
merge into emps
    using tempemps on t_empno = empno
    when matched then
        update set deptno = t_deptno, city = upper(t_city),
            salary = salary * .25
    when not matched then
        insert (empno, name, age, gender, salary, city)
        values(t_empno, upper(t_name), t_age, t_gender, t_age * 1000, t_city);

-- no target column list in the insert
explain plan without implementation for
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
explain plan without implementation for
merge into emps
    using (select * from tempemps where t_deptno = 100) on t_empno = empno
    when matched then
        update set deptno = t_deptno, city = upper(t_city),
            salary = salary * .25
    when not matched then
        insert
            values(t_empno, cast(upper(t_name) as varchar(20)), null, t_gender,
                cast(t_city as char(30)), t_age, t_age * 1000);

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

