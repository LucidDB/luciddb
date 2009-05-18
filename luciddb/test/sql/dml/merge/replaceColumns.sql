-- Test MERGE statements that are executed by replacing only the columns
-- being updated as opposed to updating entire rows.

create schema rc;

set schema 'rc';

-- use the personality that will allow index-only scans to be used, so tests
-- that verify if indexes are rebuilt properly will return data read from
-- the index instead of the column store
alter session implementation set jar
    sys_boot.sys_boot.luciddb_index_only_plugin;

create table emps(
    empno int not null unique, name varchar(20) not null, deptno int,
    gender char(1), city char(30), age smallint, salary numeric(10,2));
create index ideptno on emps(deptno);
create index icity on emps(city, age);

insert into emps values(100, 'Fred', 10, null, null, 25, 22500);
insert into emps values(110, 'Eric', 20, 'M', 'San Francisco', 80, 72000);
insert into emps values(120, 'Wilma', 20, 'F', null, 50, 45000);
insert into emps values(130, 'John', 40, 'M', 'Vancouver', null, null);
insert into emps values(140, 'Barney', 10, 'M', 'San Mateo', 41, 50000);
insert into emps values(150, 'Betty', 20, 'F', 'San Francisco', 40, 90000);
select lcs_rid(empno), * from emps order by empno;

create table tempemps(
    t_empno int unique not null, t_name varchar(25), t_deptno int,
    t_gender char(1), t_city char(35), t_age int);

insert into tempemps
    select empno, name, deptno + 1, gender, coalesce(city, 'San Mateo'), age
        from emps;
select * from tempemps order by t_empno;

-- Set fake stats so index-only scans will be used in cases where filtering is
-- done on indexed columns.
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'RC', 'EMPS', 1000);
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'RC', 'TEMPEMPS', 1000);

------------------------------------------------------------------------------
-- Cases where the optimization can be used.  Note that we can verify that it
-- was used by checking that the rows still have their original rid values.
------------------------------------------------------------------------------

-- Simplest case -- update a single column that's not indexed.  All rows are
-- updated.
merge into emps e
    using tempemps t on t.t_empno = e.empno
    when matched then
        update set salary = salary * .25;
select lcs_rid(empno), * from emps order by empno;
-- make sure the affected row count is 5, not 6 since one of the rows has
-- a null salary value, which is unaffected by the update
select * from sys_boot.mgmt.session_parameters_view
    where param_name = 'lastUpsertRowsInserted';

merge into emps e
    using tempemps t on t.t_empno = e.empno
    when matched then
        update set gender = lower(t.t_gender);
select lcs_rid(empno), * from emps order by empno;

-- Update multiple columns, both of which aren't indexed.
merge into emps e
    using tempemps t on t.t_empno = e.empno
    when matched then
        update set salary = salary * 4, gender = upper(t.t_gender);
select lcs_rid(empno), * from emps order by empno;

-- Update a subset of rows.
merge into emps e
    using tempemps t
    on t.t_empno = e.empno and e.city is not null
    when matched then
        update set gender = lower(t.t_gender), name = lower(t.t_name);
select lcs_rid(empno), * from emps order by empno;

-- Executes the updates using actual update statements
update emps set gender = upper(gender) where gender = lower(gender);
select lcs_rid(empno), * from emps order by empno;
update emps set gender = lower(gender) where gender = 'M';
select lcs_rid(empno), * from emps order by empno;

-- Update columns that are indexed.  Verify that the indexes are still usable.
-- (Double check via explain plan that the indexes are in fact being used.)

-- this only affects a single index
merge into emps e
    using tempemps t
    on t.t_empno = e.empno
    when matched then
        update set age = age * 2;
select lcs_rid(empno), * from emps order by empno;
!set outputformat csv
explain plan for
    select city, age from emps where city >= 'A' union
    select city, age from emps where city is null
    order by city, age;
!set outputformat table
select city, age from emps where city >= 'A' union
    select city, age from emps where city is null
    order by city, age;

-- repeat the above MERGE to ensure that a previously prepared statement
-- executes properly
merge into emps e
    using tempemps t
    on t.t_empno = e.empno
    when matched then
        update set age = age * 2;
select lcs_rid(empno), * from emps order by empno;
select city, age from emps where city >= 'A' union
    select city, age from emps where city is null
    order by city, age;

-- this affects two indexes
merge into emps e
    using tempemps t
    on t.t_empno = e.empno
    when matched then
        update set deptno = t.t_deptno, age = t.t_age * 2;
select lcs_rid(empno), * from emps order by empno;
!set outputformat csv
explain plan for select deptno from emps where deptno > 0 order by deptno;
!set outputformat table
select deptno from emps where deptno > 0 order by deptno;
select city, age from emps where city >= 'A' union
    select city, age from emps where city is null
    order by city, age;

-- Update results in calc errors.  Make sure the rows that result in errors
-- retain their original values.
alter session set "errorMax" = 5;
alter session set "logDir" = 'testlog';
merge into emps e
    using tempemps t
    on t.t_empno = e.empno
    when matched then
        update set age = age + 32668;
select lcs_rid(empno), * from emps order by empno;
alter session set "errorMax" = 0;

-- Update results in constraint violations.
merge into emps e
    using tempemps t
    on t.t_empno = e.empno
    when matched then
        update set empno = 0;
select lcs_rid(empno), * from emps order by empno;

-- Update that is a "real" update, i.e., doesn't need to join with the source.
!set outputformat csv
explain plan for
merge into emps e
    using emps e2
    on e.empno = e2.empno
    when matched then
        update set deptno = e2.deptno - 1, gender = upper(e2.gender),
            age = 
                (case when e2.age > 32000 then (e2.age - 32668)/2
                    else e2.age/2 end);
merge into emps e
    using emps e2
    on e.empno = e2.empno
    when matched then
        update set deptno = e2.deptno - 1, gender = upper(e2.gender),
            age = 
                (case when e2.age > 32000 then (e2.age - 32668)/2
                    else e2.age/2 end);
!set outputformat table
select lcs_rid(empno), * from emps order by empno;

-- Create a label before we delete some rows; we'll use it later to verify
-- that we still have access to the state of the data before a bunch of
-- mods.
create label l;

-- Delete some rows and then update all rows.
delete from emps where name <> lower(name);
select lcs_rid(empno), * from emps order by empno;
merge into emps e
    using tempemps t
    on t.t_empno = e.empno
    when matched then
        update set name = upper(t.t_name);
select lcs_rid(empno), * from emps order by empno;

-- update the unique index
merge into emps e using emps e2 on e.empno = e2.empno
    when matched then
        update set empno = e.empno - 100;
select lcs_rid(empno), * from emps order by empno;
!set outputformat csv
explain plan for select empno from emps where empno >= 0 order by empno;
!set outputformat table
select empno from emps where empno >= 0 order by empno;

-- put the unique column back to its original value
merge into emps e using emps e2 on e.empno = e2.empno
    when matched then
        update set empno = e.empno + 100;
select lcs_rid(empno), * from emps order by empno;
select empno from emps where empno >= 0 order by empno;

-- Combo test that exercises a combination of most of the previous conditions:
-- multiple columns, filters rows, calc errors, deleted rows, real update,
-- multiple indexes
alter session set "errorMax" = 5;
merge into emps e
    using emps e2
    on e2.empno = e.empno and e.age is not null
    when matched then
        update set
            name = lower(e2.name),
            deptno = e2.deptno - 1,           
            age = e.age + 32688;
select lcs_rid(empno), * from emps order by empno;
select deptno from emps where deptno > 0 order by deptno;
select city, age from emps where city >= 'A' union
    select city, age from emps where city is null
    order by city, age;

-- Set session label and verify we get back the old data.  Also verify the
-- indexes.
alter session set "label" = 'L';
select * from emps order by empno;
select deptno from emps where deptno > 0 order by deptno;
select city, age from emps where city >= 'A' union
    select city, age from emps where city is null
    order by city, age;
alter session set "label" = null;

-- Verify that the optimization can be used even when the join key from the
-- target table is non-unique
create table nonUniqueEmps(
    empno int not null, name varchar(20) not null, deptno int,
    gender char(1), city char(30), age smallint, salary numeric(10,2));
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'RC', 'NONUNIQUEEMPS', 1000);
insert into nonUniqueEmps select * from emps;
insert into nonUniqueEmps values(
    110, 'EricJr', 20, 'M', 'San Francisco', 40, 36000);
insert into nonUniqueEmps values(130, 'JohnJr', 40, 'M', 'Vancouver', 10, null);
select lcs_rid(empno), * from nonUniqueEmps order by empno;
merge into nonUniqueEmps e
    using tempemps t on t.t_empno = e.empno
    when matched then
        update set city = upper(t.t_city);
select lcs_rid(empno), * from nonUniqueEmps order by empno;

---------------------------------------------------------------------------
-- Exercise cases where the optimization cannot be used.  In this case, the
-- rid values after executing the MERGEs should be different.
---------------------------------------------------------------------------

-- not update-only
merge into emps e
    using tempemps t on t.t_empno = e.empno
    when matched then
        update set deptno = t.t_deptno, age = t.t_age
    when not matched then
        insert (empno, name, age, gender, salary, city)
        values(t.t_empno, t.t_name, t.t_age, t.t_gender, t.t_age, t.t_city);
select lcs_rid(empno), * from emps order by empno;

-- too many columns being updated
merge into emps e
    using tempemps t on t.t_empno = e.empno
    when matched then
        update set deptno = e.deptno - 1, age = e.age + 10,
            salary = e.salary * 2, name = lower(e.name), city = upper(e.city);
select lcs_rid(empno), * from emps order by empno;

-- too few rows being updated
merge into emps e
    using (select * from tempemps where t_name = 'Eric') t
    on t.t_empno = e.empno
    when matched then
        update set salary = e.salary/2, city = lower(e.city);
select lcs_rid(empno), * from emps order by empno;

-- non-unique join keys
merge into emps e
    using tempemps t on e.name = lower(t.t_name)
    when matched then
        update set salary = e.salary/2;
select lcs_rid(empno), * from emps order by empno;

------------------------------------------------------------------------
-- Do another combo test with a slightly larger dataset and more indexes
------------------------------------------------------------------------

create table t(
        a int generated always as identity primary key, b smallint, c smallint,
        d int, e int, f int, g int)
    create clustered index t_a on t(a)
    create clustered index t_b on t(b)
    create clustered index t_c on t(c)
    create clustered index t_defg on t(d, e, f, g);
create index it1 on t(b);
create index it2 on t(c);
create index it3 on t(a, b);
create index it4 on t(a, c);
create index it5 on t(g);
create index it6 on t(a, e);
insert into t(b, c, d, e, f, g) values(10, 100, 1, 1, 1, 1);
insert into t(b, c, d, e, f, g) values(20, 200, 2, 2, 2, 2);
insert into t(b, c, d, e, f, g) values(30, 300, 3, 3, 3, 3);
insert into t(b, c, d, e, f, g) values(40, 400, 4, 4, 4, 4);
insert into t(b, c, d, e, f, g) values(50, 500, 5, 5, 5, 5);
insert into t(b, c, d, e, f, g) values(60, 600, 6, 6, 6, 6);
insert into t(b, c, d, e, f, g) values(70, 700, 7, 7, 7, 7);
insert into t(b, c, d, e, f, g) values(80, 800, 8, 8, 8, 8);
insert into t(b, c, d, e, f, g) values(90, 900, 9, 9, 9, 9);
insert into t(b, c, d, e, f, g) values(100, 1000, 10, 10, 10, 10);
insert into t(b, c, d, e, f, g)
    select b + 100, c + 1000, d + 10, e + 10, f + 10, g + 10 from t;
delete from t where a in (0, 1, 3, 11, 12, 19);
select lcs_rid(a), * from t order by a;
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'RC', 'T', 1000);

alter session set "errorMax" = 10;
merge into t
    using t t2
    on t.a = t2.a
    when matched then update set 
        b = (case when mod(t.b, 3) = 0 then t.b*275 else t2.b/10 end),
        c = (case when mod(t.c, 8) = 0 then t.c*22 else t2.c/100 end);
alter session set "errorMax" = 0;
select lcs_rid(a), * from t order by a;
!set outputformat csv
explain plan for select a from t where a > 0 order by a;
explain plan for select b from t where b > 0 order by b;
explain plan for select c from t where c > 0 order by c;
explain plan for select a, b from t where a > 0 order by a;
explain plan for select a, c from t where a > 0 order by a;
explain plan for select g from t where g > 0 order by g;
explain plan for select a, e from t where a > 0 order by a;
!set outputformat table
select a from t where a > 0 order by a;
select b from t where b > 0 order by b;
select c from t where c > 0 order by c;
select a, b from t where a > 0 order by a;
select a, c from t where a > 0 order by a;
select g from t where g > 0 order by g;
select a, e from t where a > 0 order by a;

-- update the unique index along with other columns
merge into t
    using t t2
    on t.a = t2.a
    when matched then update set 
        a = t.a*10,
        b = (case when t.b > 1000 then t.b/275 
                when t.b < 20 then t.b*10
                else t.b end),
        c = (case when t.c > 10000 then t.c/22
                when t.c > 1000 then t.c
                else t.c*100 end);
select lcs_rid(a), * from t order by a;
select a from t where a > 0 order by a;
select b from t where b > 0 order by b;
select c from t where c > 0 order by c;
select a, b from t where a > 0 order by a;
select a, c from t where a > 0 order by a;
select g from t where g > 0 order by g;
select a, e from t where a > 0 order by a;

-- Verify that the optimization cannot be used when updating a column that's
-- part of a multi-column cluster.
merge into t
    using t t2
    on t.a = t2.a
    when matched then update set g = t.g * 2;
select lcs_rid(a), * from t order by a;

-- LER-10611 -- Verify that duplicate keys that correspond to deleted rows
-- are ignored when rebuilding the unique index; otherwise, the 4th merge
-- statement below incorrectly results in a constraint violation.

create table u(a int primary key, b int);
insert into u values(0,0);
insert into u values(1,1);
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'RC', 'U', 10000);
select lcs_rid(a), * from u order by a;
-- for the next two merge statements, we do NOT want to enable the optimization;
-- the filter on b should be selective enough so the optimization is disabled
merge into u as tgt using (select * from u where b = 1) as src
    on tgt.a = src.a
    when matched then update set b = 2;
select lcs_rid(a), * from u order by a;
merge into u as tgt using (select * from u where b = 2) as src 
    on tgt.a = src.a
    when matched then update set b = 3;
select lcs_rid(a), * from u order by a;
-- this statement is just to show that the filter "b + 1 >= 4" will
-- cause the optimization to be used; so the rid values returned should be
-- identical to those returned previously -- 0 and 3
merge into u as tgt using (select * from u where b + 1 >= 4) as src
    on tgt.a = src.a
    when matched then update set b = 4;
select lcs_rid(a), * from u order by a;
-- so for this statement, the optimization should be used and the rid values
-- returned should be the same as prior to the merge -- 0 and 3
merge into u as tgt using (select * from u where b + 1 >= 5) as src
    on tgt.a = src.a
    when matched then update set a = 2;
select lcs_rid(a), * from u order by a;
-- check that the data is properly stored in the index
!set outputformat csv
explain plan for select * from u where a >= 0 order by a;
!set outputformat table
select * from u where a >= 0 order by a;
-- verify things work as well when the key value 1 is inserted, deleted, and
-- then an existing entry is updated to the key value 1; the update should not
-- result in a constraint violation
insert into u values(1, 1);
select lcs_rid(a), * from u order by a;
delete from u where a = 1;
select lcs_rid(a), * from u order by a;
update u set a = a + 1;
select lcs_rid(a), * from u order by a;
select * from u where a >= 0 order by a;
-- make sure violations are returned when appropriate
update u set a = 2;
select lcs_rid(a), * from u order by a;
-- verify the case where the deleted keys appear as singletons
truncate table u;
insert into u values(0,0);
insert into u values(1,1);
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'RC', 'U', 10000);
merge into u as tgt using (select * from u where a = 1) as src
    on tgt.a = src.a when matched then update set b = 2;
merge into u as tgt using (select * from u where a = 1) as src 
    on tgt.a = src.a when matched then update set b = 3;
insert into u values(3,3),(4,4),(5,5),(6,6),(7,7),(8,8);
update u set a = a + 1 where a > 2;
select lcs_rid(a), * from u order by a;
select * from u where a >= 0 order by a;
-- do a no-op update; verify this by checking that no new pages are allocated
-- after the update
call applib.create_var('RC', null, 'test context');
call applib.create_var(
    'RC', 'pageCount', ' used to store current page allocation count');
call applib.set_var(
    'RC',
    'pageCount',
    (select counter_value from sys_root.dba_performance_counters
        where counter_name = 'DatabasePagesAllocated'));
update u set a = a;
select lcs_rid(a), * from u order by a;
select * from u where a >= 0 order by a;
-- sleep before retrieving the stats again
select sys_boot.mgmt.sleep(1000) from u where a = 0;
select (counter_value = applib.get_var('RC', 'pageCount'))
    from sys_root.dba_performance_counters
        where counter_name = 'DatabasePagesAllocated';

drop label l;
drop schema rc cascade;
call applib.delete_var('RC', 'pageCount');
