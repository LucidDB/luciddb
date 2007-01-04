-- $Id$

----------------------------------------------------------------------------
-- Tests for MERGE statement - identical to merge.sql except in these tests,
-- the target table has no indexes and the error testcases have been omitted
----------------------------------------------------------------------------

create schema m;
set schema 'm';
alter session implementation set jar sys_boot.sys_boot.luciddb_plugin;

create table emps(
    empno int not null, name varchar(20) not null, deptno int,
    gender char(1), city char(30), age int, salary numeric(10,2));
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
    order by 1;
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastUpsertRowsInserted';

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
    order by 1;
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastUpsertRowsInserted';

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
    order by 1;
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastUpsertRowsInserted';
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
    order by 1;
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastUpsertRowsInserted';

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
    order by 1;
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastUpsertRowsInserted';

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
    order by 1;
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastUpsertRowsInserted';

-- only inserts, no updates
delete from emps where empno >= 140;
select * from emps order by empno;
select table_name, current_row_count, deleted_row_count
    from sys_boot.mgmt.dba_stored_tables_internal1
    order by 1;
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastUpsertRowsInserted';
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
    order by 1;
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastUpsertRowsInserted';

-- more than 1 row in the source table matches the target; per SQL2003, this
-- should return an error; currently, we do not return an error
insert into tempemps values(130, 'JohnClone', 41, 'M', 'Vancouver', null);
select * from tempemps order by t_empno, t_name;
select table_name, current_row_count, deleted_row_count
    from sys_boot.mgmt.dba_stored_tables_internal1
    order by 1;
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastUpsertRowsInserted';
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
    order by 1;
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastUpsertRowsInserted';

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
    order by 1;
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastUpsertRowsInserted';

delete from tempemps where t_name = 'JohnClone';
                
-- no insert substatement
insert into tempemps values(160, 'Pebbles', 60, 'F', 'Foster City', 2);
select table_name, current_row_count, deleted_row_count
    from sys_boot.mgmt.dba_stored_tables_internal1
    order by 1;
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastUpsertRowsInserted';
select * from tempemps order by t_empno;
merge into emps
    using (select * from tempemps) on t_empno = empno
    when matched then
        update set name = t_name, city = t_city;
select * from emps order by empno, name;
select table_name, current_row_count, deleted_row_count
    from sys_boot.mgmt.dba_stored_tables_internal1
    order by 1;
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastUpsertRowsInserted';

-- no update substatement
merge into emps
    using (select * from tempemps) on t_empno = empno
    when not matched then
        insert values (t_empno, t_name, t_deptno, t_gender, t_city, t_age, 0);
select * from emps order by empno, name;
select table_name, current_row_count, deleted_row_count
    from sys_boot.mgmt.dba_stored_tables_internal1
    order by 1;
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastUpsertRowsInserted';

-- simple update via a merge
delete from emps where empno = 130;
select * from emps order by empno;
select table_name, current_row_count, deleted_row_count
    from sys_boot.mgmt.dba_stored_tables_internal1
    order by 1;
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastUpsertRowsInserted';
merge into emps e1
    using (select * from emps) e2 on e1.empno = e2.empno
    when matched then
        update set age = e1.age + 1;
select * from emps order by empno;
select table_name, current_row_count, deleted_row_count
    from sys_boot.mgmt.dba_stored_tables_internal1
    order by 1;
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastUpsertRowsInserted';

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

