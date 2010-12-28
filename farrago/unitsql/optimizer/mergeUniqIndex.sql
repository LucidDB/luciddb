-- $Id$

--------------------------------------------------------------------------------
-- Tests for MERGE statement - similar to merge.sql except in these tests,
-- the target table has a unique constraint/primary key and additional
-- testcases have been added that are specific to unique constraints/primary
-- keys
--------------------------------------------------------------------------------

!set showwarnings true

create schema m;
set schema 'm';
alter session implementation set jar sys_boot.sys_boot.luciddb_plugin;
alter session set "errorMax" = 5;
alter session set "logDir" = 'testlog';

create table emps(
    empno int not null unique, name varchar(20) not null, deptno int,
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

-- check rowcounts before doing any merges
select table_name, current_row_count, deleted_row_count
    from sys_boot.mgmt.dba_stored_tables_internal1
    where schema_name = 'M'
    order by 1;
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastUpsertRowsInserted';
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastRowsRejected';

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

-- verify rowcounts after merge -- should be 2 new rows after the merge
select table_name, current_row_count, deleted_row_count
    from sys_boot.mgmt.dba_stored_tables_internal1
    where schema_name = 'M'
    order by 1;
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastUpsertRowsInserted';
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastRowsRejected';

-- verify that the old rows are inserted before the new ones even
-- though the new rows are stored first in the source table
select lcs_rid(empno), * from emps order by 1;

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

select table_name, current_row_count, deleted_row_count
    from sys_boot.mgmt.dba_stored_tables_internal1
    where schema_name = 'M'
    order by 1;
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastUpsertRowsInserted';
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastRowsRejected';
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
select table_name, current_row_count, deleted_row_count
    from sys_boot.mgmt.dba_stored_tables_internal1
    where schema_name = 'M'
    order by 1;
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastUpsertRowsInserted';
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastRowsRejected';

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
select table_name, current_row_count, deleted_row_count
    from sys_boot.mgmt.dba_stored_tables_internal1
    where schema_name = 'M'
    order by 1;
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastUpsertRowsInserted';
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastRowsRejected';

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
select table_name, current_row_count, deleted_row_count
    from sys_boot.mgmt.dba_stored_tables_internal1
    where schema_name = 'M'
    order by 1;
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastUpsertRowsInserted';
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastRowsRejected';

-- only inserts, no updates
delete from emps where empno >= 140;
select * from emps order by empno;
select table_name, current_row_count, deleted_row_count
    from sys_boot.mgmt.dba_stored_tables_internal1
    where schema_name = 'M'
    order by 1;
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastUpsertRowsInserted';
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastRowsRejected';
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
select table_name, current_row_count, deleted_row_count
    from sys_boot.mgmt.dba_stored_tables_internal1
    where schema_name = 'M'
    order by 1;
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastUpsertRowsInserted';
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastRowsRejected';

-- more than 1 row in the source table matches the target; per SQL2003, this
-- should return an error; currently, we do not return an error
-- FIXME - change the expected output once we detect unique constraint
-- violations
insert into tempemps values(130, 'JohnClone', 41, 'M', 'Vancouver', null);
select * from tempemps order by t_empno, t_name;
select table_name, current_row_count, deleted_row_count
    from sys_boot.mgmt.dba_stored_tables_internal1
    where schema_name = 'M'
    order by 1;
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastUpsertRowsInserted';
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastRowsRejected';
merge into emps
    using (select * from tempemps where t_empno = 130) on t_empno = empno
    when matched then
        update set deptno = t_deptno, city = t_city, age = t_age,
            gender = t_gender
    when not matched then
        insert (empno, name, age, gender, salary, city)
        values(t_empno, upper(t_name), t_age, t_gender, t_age * 1000, t_city);
select * from emps order by empno, name;
select table_name, current_row_count, deleted_row_count
    from sys_boot.mgmt.dba_stored_tables_internal1
    where schema_name = 'M'
    order by 1;
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastUpsertRowsInserted';
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastRowsRejected';

-- LER-2614 -- issue the same merge again, except modify the update values
-- so the update is not a no-op
merge into emps
    using (select * from tempemps where t_empno = 130) on t_empno = empno
    when matched then
        update set deptno = t_deptno, city = t_city, age = t_age,
            gender = lower(t_gender)
    when not matched then
        insert (empno, name, age, gender, salary, city)
        values(t_empno, upper(t_name), t_age, t_gender, t_age * 1000, t_city);
select * from emps order by empno, name;
select table_name, current_row_count, deleted_row_count
    from sys_boot.mgmt.dba_stored_tables_internal1
    where schema_name = 'M'
    order by 1;
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastUpsertRowsInserted';
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastRowsRejected';

delete from tempemps where t_name = 'JohnClone';
                
-- no insert substatement
insert into tempemps values(160, 'Pebbles', 60, 'F', 'Foster City', 2);
select table_name, current_row_count, deleted_row_count
    from sys_boot.mgmt.dba_stored_tables_internal1
    where schema_name = 'M'
    order by 1;
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastUpsertRowsInserted';
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastRowsRejected';
select * from tempemps order by t_empno;
merge into emps
    using (select * from tempemps) on t_empno = empno
    when matched then
        update set name = t_name, city = t_city;
select * from emps order by empno, name;
select table_name, current_row_count, deleted_row_count
    from sys_boot.mgmt.dba_stored_tables_internal1
    where schema_name = 'M'
    order by 1;
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastUpsertRowsInserted';
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastRowsRejected';

-- no update substatement
merge into emps
    using (select * from tempemps) on t_empno = empno
    when not matched then
        insert values (t_empno, t_name, t_deptno, t_gender, t_city, t_age, 0);
select * from emps order by empno, name;
select table_name, current_row_count, deleted_row_count
    from sys_boot.mgmt.dba_stored_tables_internal1
    where schema_name = 'M'
    order by 1;
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastUpsertRowsInserted';
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastRowsRejected';

-- simple update via a merge
delete from emps where empno = 130;
select * from emps order by empno;
select table_name, current_row_count, deleted_row_count
    from sys_boot.mgmt.dba_stored_tables_internal1
    where schema_name = 'M'
    order by 1;
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastUpsertRowsInserted';
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastRowsRejected';
merge into emps e1
    using (select * from emps) e2 on e1.empno = e2.empno
    when matched then
        update set age = e1.age + 1;
select * from emps order by empno;
select table_name, current_row_count, deleted_row_count
    from sys_boot.mgmt.dba_stored_tables_internal1
    where schema_name = 'M'
    order by 1;
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastUpsertRowsInserted';
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastRowsRejected';

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

-- make sure that when the original value is null, the update does occur
merge into emps e
    using tempemps t on t.t_empno = e.empno
    when matched then
        update set salary =
            (case when salary is null then 10000 else salary end)
    when not matched then
        insert (empno, name, age, gender, salary, city)
        values(t.t_empno, upper(t.t_name), t.t_age, t.t_gender, t.t_age * 1000,
            t.t_city);
select lcs_rid(empno), * from emps order by empno;

-- make sure that when the new value is null, the update occurs
merge into emps e
    using tempemps t on t.t_empno = e.empno
    when matched then
        update set salary =
            (case when age is null then null else salary end)
    when not matched then
        insert (empno, name, age, gender, salary, city)
        values(t.t_empno, upper(t.t_name), t.t_age, t.t_gender, t.t_age * 1000,
            t.t_city);
select lcs_rid(empno), * from emps order by empno;

-- make sure that when the new value is the same as the old one
-- except for trailing spaces, the update occurs
merge into emps e
    using tempemps t on t.t_empno = e.empno and e.name='Fred'
    when matched then
        update set name = 'Fred   ';
select name||'X' from emps order by name;

-- LER-1953
-- cast in the ON clause should cast to a not null type
merge into emps e
    using tempemps t on t.t_empno = e.empno and
        e.name = cast('Fred' as varchar(20))
    when matched then
        update set name = cast('FRED' as varchar(20));
select * from emps order by empno;

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

explain plan for
merge into emps e
    using tempemps t on t.t_empno = e.empno and
        e.name = cast('Fred' as varchar(20))
    when matched then
        update set name = cast('FRED' as varchar(20));

-------------------------------------
-- update on unique key -- disallowed
-------------------------------------
alter session set "errorMax" = 100;
explain plan for merge into emps e
    using tempemps t on t.t_empno = e.empno
    when matched then
        update set deptno = t.t_deptno, city = upper(t.t_city),
            salary = salary * .25, empno = empno + 1
    when not matched then
        insert (empno, name, age, gender, salary, city)
        values(t.t_empno, upper(t.t_name), t.t_age, t.t_gender, t.t_age * 1000,
            t.t_city);
explain plan for merge into emps e
    using tempemps t on t_empno + 1 = e.empno
    when matched then
        update set deptno = t.t_deptno, city = upper(t.t_city),
            salary = salary * .25, empno = t.t_empno
    when not matched then
        insert (empno, name, age, gender, salary, city)
        values(t.t_empno, upper(t.t_name), t.t_age, t.t_gender, t.t_age * 1000,
            t.t_city);
explain plan for merge into emps e
    using tempemps t on t.t_empno = e.empno or t.t_deptno = e.deptno
    when matched then
        update set deptno = t.t_deptno, city = upper(t.t_city),
            salary = salary * .25, empno = t.t_empno
    when not matched then
        insert (empno, name, age, gender, salary, city)
        values(t.t_empno, upper(t.t_name), t.t_age, t.t_gender, t.t_age * 1000,
            t.t_city);
explain plan for merge into emps e
    using tempemps t on t.t_empno = e.empno
    when matched then
        update set deptno = t.t_deptno, city = upper(t.t_city),
            salary = salary * .25, empno = cast(t.t_empno as smallint)
    when not matched then
        insert (empno, name, age, gender, salary, city)
        values(t.t_empno, upper(t.t_name), t.t_age, t.t_gender, t.t_age * 1000,
            t.t_city);

------------------------------------------------------------
-- update on unique key -- but update is a no-op, so allowed
------------------------------------------------------------
explain plan for merge into emps e
    using tempemps t on t.t_empno = e.empno
    when matched then
        update set deptno = t.t_deptno, city = upper(t.t_city),
            salary = salary * .25, empno = t.t_empno
    when not matched then
        insert (empno, name, age, gender, salary, city)
        values(t.t_empno, upper(t.t_name), t.t_age, t.t_gender, t.t_age * 1000,
            t.t_city);
-- dummy cast is OK
explain plan for merge into emps e
    using tempemps t on t.t_empno = e.empno
    when matched then
        update set deptno = t.t_deptno, city = upper(t.t_city),
            salary = salary * .25, empno = cast(t.t_empno as integer)
    when not matched then
        insert (empno, name, age, gender, salary, city)
        values(t.t_empno, upper(t.t_name), t.t_age, t.t_gender, t.t_age * 1000,
            t.t_city);
-- target table has no alias
explain plan for merge into emps
    using tempemps t on empno = t_empno
    when matched then
        update set deptno = t.t_deptno, city = upper(t.t_city),
            salary = salary * .25, empno = t_empno
    when not matched then
        insert (empno, name, age, gender, salary, city)
        values(t.t_empno, upper(t.t_name), t.t_age, t.t_gender, t.t_age * 1000,
            t.t_city);
-- ON condition contains multiple expressions
explain plan for merge into emps e
    using tempemps t on empno = t.t_deptno and t.t_empno = empno
    when matched then
        update set deptno = t.t_deptno, city = upper(t.t_city),
            salary = salary * .25, empno = t_empno
    when not matched then
        insert (empno, name, age, gender, salary, city)
        values(t.t_empno, upper(t.t_name), t.t_age, t.t_gender, t.t_age * 1000,
            t.t_city);
-- update unique column to itself
explain plan for merge into m.emps
    using tempemps t on t.t_empno = emps.empno
    when matched then
        update set deptno = t.t_deptno, city = upper(t.t_city),
            salary = salary * .25, empno = empno
    when not matched then
        insert (empno, name, age, gender, salary, city)
        values(t.t_empno, upper(t.t_name), t.t_age, t.t_gender, t.t_age * 1000,
            t.t_city);

-- update allowed because in fail-fast mode
alter session set "errorMax" = 0;
explain plan for merge into emps e
    using tempemps t on t.t_empno = e.empno
    when matched then
        update set deptno = t.t_deptno, city = upper(t.t_city),
            salary = salary * .25, empno = empno + 1
    when not matched then
        insert (empno, name, age, gender, salary, city)
        values(t.t_empno, upper(t.t_name), t.t_age, t.t_gender, t.t_age * 1000,
            t.t_city);

--------------------------
-- multiple unique indexes
--------------------------
create table emps2(
    empno int primary key, name varchar(20) unique, deptno int,
    gender char(1), city char(30), age int, salary numeric(10,2));
create index ideptno2 on emps2(deptno);
create index icity2 on emps2(city);

insert into emps2(empno, name, deptno, gender, city, age, salary)
    select case when name = 'John' then 130 else empno end,
        name, deptno, gender, city, age, age * 900 from sales.emps;

-- verify rowcounts before merge
!set outputformat table
select * from emps2 order by empno;
select * from tempemps order by t_empno;
select table_name, current_row_count, deleted_row_count
    from sys_boot.mgmt.dba_stored_tables_internal1
    where schema_name = 'M'
    order by 1;
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastUpsertRowsInserted';
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastRowsRejected';

merge into emps2 e
    using tempemps t on t.t_empno = e.empno
    when matched then
        update set deptno = t.t_deptno, city = upper(t.t_city),
            salary = salary * .25
    when not matched then
        insert (empno, name, age, gender, salary, city)
        values(t.t_empno, upper(t.t_name), t.t_age, t.t_gender, t.t_age * 1000,
            t.t_city);
select * from emps2 order by empno;

-- verify rowcounts after merge -- should be 3 new rows after the merge
select table_name, current_row_count, deleted_row_count
    from sys_boot.mgmt.dba_stored_tables_internal1
    where schema_name = 'M'
    order by 1;
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastUpsertRowsInserted';
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastRowsRejected';

-------------------------------
-- examples from user level doc
-- FIXME: update these once unique constraints are detected
-------------------------------
alter session set "errorMax" = 6;

-- primary keys cannot be null
create table pk1 (i int primary key);
insert into pk1 values (null);
select * from pk1;

-- primary keys cannot contain null columns
create table pk2 (i int, j int, k varchar(30),
    constraint i_j_k_pk primary key (i, j, k));
insert into pk2 values (15, null, 'foo');
select * from pk2;

-- unique keys can contain null, they are allowed to repeat
create table uk1 (i int,
    constraint i_unique unique(i));
insert into uk1 values (null);
insert into uk1 values (null);
select * from uk1;

-- unique keys can contain null columns, if so they are allowed to repeat
create table uk2 (i int, j int, k varchar(30),
    constraint i_j_k_unique unique (i, j, k));
insert into uk2 values 
    (null, 23, 'foo'),
    (null, 23, 'foo'),
    (null, 23, null);
select * from uk2;

create table comic (empid int primary key, name varchar(30));
insert into comic values
    (1, 'Harry Osborn'),
    (2, 'Mary Jane');

create table comic_stg (empid int, name varchar(30));
insert into comic_stg values
    (1, 'Peter Parker'),
    (1, 'John Jameson'),
    (2, 'Mary Parker'),
    (3, 'Drake Roberts'),
    (4, 'Anjelica Jones'),
    (4, 'Johnny Storm');

-- multiple inserts, multiple updates
merge into comic tgt using comic_stg src on src.empid = tgt.empid
    when matched then
        update set name = src.name
    when not matched then
        insert (empid, name)
        values (src.empid, src.name);
select * from comic;
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastUpsertRowsInserted';
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastRowsRejected';

create table villians (
    empid int primary key, name varchar(30), info varchar(30),
    constraint name_unique unique(name));
insert into villians values 
    (1, 'Harry Osborn', 'Wealthy teenager');

create table villians_stg (empid int, name varchar(30), info varchar(30));
insert into villians_stg values 
    (1, 'Harry Osborn', 'President of Osborn Inc'),
    (2, 'Harry Osborn', 'Hobgoblin');
    
-- simultaneous insert and update
merge into villians tgt using villians_stg src on src.empid = tgt.empid
    when matched then
        update set info = src.info
    when not matched then
        insert (empid, name, info)
        values (src.empid, src.name, src.info);
select * from villians;
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastUpsertRowsInserted';
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastRowsRejected';

create table villians2 (
    empid int primary key, name varchar(30), alias varchar(30), 
    info varchar(30),
    constraint name2_unique unique(name), 
    constraint alias2_unique unique(alias));
insert into villians2 values 
    (5, 'Dr Otto Octavius', 'Doctor Octopus', 'Precocious Child');

create table villians2_stg (
    empid int, name varchar(30), alias varchar(30), info varchar(30));
insert into villians2_stg values
    (5, 'Dr Otto Octavius', 'Doctor Octopus', 'Scientist'),
    (5, 'Dr Otto Octavius', 'Doctor Octopus', 'Archenemy'),
    (5, 'Dr Otto Octavius', 'Doctor Octopus', 'Deceased');

-- multiple unique constraints, all causing failures
-- violations from separate streams should be coupled
merge into villians2 tgt using villians2_stg src on src.empid = tgt.empid
    when matched then
        update set info = src.info
    when not matched then
        insert (empid, name, alias, info)
        values (src.empid, src.name, src.alias, src.info);
select * from villians2;
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastUpsertRowsInserted';
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastRowsRejected';

truncate table villians2_stg;
insert into villians2_stg values
    (6, 'Dr Otto Octavius', 'Doc Oct', null),
    (7, 'Dr Octavius', 'Doc Oct', null),
    (8, 'Otto', 'Doc Oct', null);

-- separate constraint cause differing errors, causing rows to be rejected 
-- if processed in one order when they may have succeeded in another order
merge into villians2 tgt using villians2_stg src on src.empid = tgt.empid
    when matched then
        update set info = src.info
    when not matched then
        insert (empid, name, alias, info)
        values (src.empid, src.name, src.alias, src.info);
select * from villians2;
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastUpsertRowsInserted';
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastRowsRejected';

select table_name, current_row_count, deleted_row_count
    from sys_boot.mgmt.dba_stored_tables_internal1
    where schema_name = 'M'
    order by 1;
