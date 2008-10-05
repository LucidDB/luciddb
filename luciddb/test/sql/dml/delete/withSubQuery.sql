set schema 'dt';

--------------------------------------------------
-- IN (uncorrelated-subquery | value-expression)
--------------------------------------------------
create table t (n1 int, n2 int);
create table u (n1 int, n2 int);
insert into t values (1,2),(1,3),(1,4),(1,5);
insert into u values (2,3),(2,4);
select * from t;
select * from u;
delete from t where n2 in
    (select n2 from u);
select * from t;
insert into u values (1,5);
delete from t where n2 in
    (select n2 from u where n1 < 2);
select * from t;
delete from t where n2 in
    (select n2 from u where 1 = 0);
select * from t;
insert into t values (1,3),(1,4),(1,5);
delete from t where n2 in
    (select u.n2 from t,u where t.n1 = u.n1);
select * from t;
delete from t where n2 in (2,3);
select * from t;
insert into t values (2,3),(2,4),(3,5),(3,6);
-- with boolean operators
delete from t where
    n1 in (select n1 from u) and
    n2 in (select n2 from u where n2 < 4);
select * from t;
delete from t where
    n1 in (select n1 from u where n1 <> 1) or
    n1 in (3);
select * from t;
delete from t where n1 not in (2,3,4,5);
select * from t;
insert into t values (1,2),(2,3),(5,6);
delete from t where n1 not in (select n1 from u);
select * from t;
delete from t where n1 not in (select n1 from t);
select * from t;
delete from t where n1 in (select n1 from t);
select * from t;
-- should work after LER-7693
--insert into t values (1,2),(2,3),(5,6);
--delete from t where n1 in
--  ((select min(n1) from u), (select max(n1) from u));
--select * from t;
-- not working before the above is fixed
-- should still error out after the fix
--delete from t where n1 in
--  ((select n1 from u), (select n2 from u));

drop table t cascade;
drop table u cascade;

--------------------------------------------------
-- BETWEEN, LIKE
--------------------------------------------------
-- BETWEEN
create table t (d1 date);
insert into t values
    (date '2000-01-01'),
    (date '2000-01-02'),
    (date '2100-01-01');
delete from t where d1
    between current_date
    and applib.add_days(current_date, 100*365);
select * from t;
delete from t where d1
    between current_date
    and applib.add_days(current_date, -200*365);
select * from t;
delete from t where d1
    between symmetric current_date
    and applib.add_days(current_date, -200*365);
select * from t;
insert into t values
    (date '2000-01-01'),
    (date '2000-01-02'),
    (date '2100-01-01');
create table u (d1 date);
insert into u values (date '2000-01-01'), (date '2000-12-31');
delete from t where d1 between
    (select min(d1) from u) and (select max(d1) from u);
select * from t;
-- should not delete anything
delete from t where d1 between
    (select d1 from u where d1 > date'2001-01-01') and
    (select max(d1) from u);
select * from t;
-- negative - should error out
delete from t where d1 between
    (select d1 from u) and (select max(d1) from u);
select * from t;

drop table t cascade;
drop table u cascade;

-- LIKE
create table t (s1 varchar(10));
insert into t values ('abc'),('abcd'),('xyz');
select * from t;
delete from t where s1 like 'ab%';
select * from t;
delete from t where s1 like 'xyz';
select * from t;
insert into t values ('abc'),('abcd'),('xyz');
create table u (s1 varchar(10));
insert into u values ('ab');
delete from t where substring(s1,1,2) like
  (select * from u);
select * from t;

drop table t cascade;
drop table u cascade;

--------------------------------------------------
-- EXISTS (correlated subquery)
--------------------------------------------------
create table t (n1 int);
create table u (n1 int);
insert into t values (1),(2),(3),(4),(5);
insert into u values (2),(3);

delete from t where exists
    (select * from u where u.n1 < t.n1);
select * from t;
delete from t where exists
    (select * from u where u.n1 < t.n1);
select * from t;

drop table t;
drop table u;

--------------------------------------------------
-- comparison operators
--------------------------------------------------
-- basic comparisons
create table t(n1 int, n2 decimal(6,2), n3 double);
insert into t values (1,2,3),(1,2,4),(1,3,4.5),(2,4,6),(3,3,3),(3,4,4);
select * from t;
delete from t where n3 > 4.5;
select * from t;
delete from t where n3 >= 4.5;
select * from t;
delete from t where n2 < 2;
select * from t;
delete from t where n2 <=2;
select * from t;
delete from t where n2 <> 4;
select * from t;
insert into t values (3,3,3);
select * from t;
delete from t where n2 <> 4;
select * from t;
-- FRG-50 original query
delete from t where n1 > (select count(*) from t);
select * from t;

create table u(n1 int);
insert into u values (1),(2);
-- negative - should error out
delete from u where n1 = (select * from u);

drop table t;
drop table u;

--------------------------------------------------
-- built-in functions
-- CAST COALESCE SUBSTRING TRIM
--------------------------------------------------
create table t (c1 int, c2 int, c3 date);
create table u (c1 int, c2 int, c3 timestamp);
insert into t values
  (1,1, date '2000-01-01'),
  (2,2, date '2000-02-28'),
  (3,3, date '2000-02-29');
insert into u values
  (1,null, timestamp '2000-01-01 23:59:59'),
  (2,2, timestamp '2000-02-29 00:00:00');
select * from t;
delete from t where c3 between
  cast ((select min(c3) from u) as date) and
  cast ((select max(c3) from u) as date);
select * from t;

truncate table t;
insert into t values
  (1,1, date '2000-01-01'),
  (2,2, date '2000-02-28'),
  (3,3, date '2000-02-29');
-- TODO: uncomment after LER-7693 is fixed
--delete from t where c2 in (coalesce((select c2 from u where c1 = 1),0));
--select * from t;

truncate table t;
insert into t values
  (1,1, date '1998-01-01'),
  (2,2, date '1999-02-28'),
  (3,3, date '2000-02-29');
delete from t where substring(cast(c3 as varchar(20)),1,2) =
  substring(cast((select min(c3) from u) as varchar(50)),1,2);
select * from t;

drop table t;
drop table u;

--------------------------------------------------
-- UDR
--------------------------------------------------
create table t(i1 int);
insert into t values (1),(10),(2),(-3),(15);
delete from t where i1 in
  (select * from table(applib.topn(cursor(select * from t),2)));
select * from t;
create table a(c1 varchar(10));
create table b(c1 varchar(10));
insert into a values('abc'),('abd'),('aab'),('bbb');
insert into b values('aax'),('ab');
-- see comments of LER-7693
--delete from a where applib.leftn(c1,(select i1 from t where i1 = 2)) in
--  (select applib.leftn(c1,(select i1 from t where i1 = 2)) from b);
delete from a where applib.leftn(c1,2) in
  (select applib.leftn(c1,(select i1 from t where i1 = 2)) from b);
select * from a;

drop table t;
drop table a;
drop table b;

--------------------------------------------------
-- CASE
--------------------------------------------------
--LER-7693 applies here too (by translating COALESCE)

-- simple CASE
create table t(c1 int, c2 int);
create table u(c1 int);
insert into t values (1,1),(2,2),(3,3),(4,4),(5,5);
insert into u values (0),(3);
delete from t where c1 =
  (case c2
    when (select max(c1) from u) then c1
   end);
select * from t;
-- search CASE (without scalar subquery in THEN)
delete from t where c1 =
  (case 
    when c2 > (select avg(c1) from u) then c1
   end);
select * from t;

drop table t;
drop table u;

--------------------------------------------------
-- set ops
--------------------------------------------------
create table t(c1 int);
create table u(c1 int);
create table v(c1 int);
insert into t values (1),(2),(3),(4),(5);
insert into u values (2),(3);
insert into v values (3),(4);
delete from t where c1 in
  ((select c1 from u) union (select c1 from v));
select * from t;
insert into t values (2),(3),(4);
delete from t where c1 in
  ((select c1 from u) union all (select c1 from v));
select * from t;
insert into t values (2),(3),(4);
delete from t where c1 in
  ((select c1 from u) intersect (select c1 from v));
select * from t;
insert into t values (2),(3),(4);
delete from t where c1 in
  ((select c1 from u) except (select c1 from v));
select * from t;

drop table t;
drop table u;
drop table v;

--------------------------------------------------
-- views in search conditions
--------------------------------------------------
create table t(c1 int);
create table u(c1 int);
create view v1 as select * from t;
create view v2 as select * from u;
create view v3 as
  select * from t,u where t.c1 = u.c1;
insert into t values (1),(2),(3),(4),(5);
insert into u values (2);
delete from t where c1 = (select min(c1) from v2);
select * from t;
delete from t where c1 = (select min(c1) from v1);
select * from t;
insert into u values (3);
delete from t where c1 in (select c1 from v3);
select * from t;

drop table t cascade;
drop table u cascade;

--------------------------------------------------
-- time-sensitive functions
--------------------------------------------------
create table h1(i1 int);
create table h2(i2 int);
insert into h1 values (1),(2),(3),(4);
insert into h2 values (1);

delete from h1 where (i1, sys_boot.mgmt.sleep(500), current_timestamp) in
  (select i2, sys_boot.mgmt.sleep(1000), current_timestamp from h2);
select * from h1;
delete from h1 where (i1, sys_boot.mgmt.sleep(500), current_timestamp) in
  (select i2 + 1, sys_boot.mgmt.sleep(100), current_timestamp from h2
   union
   select i2 + 2, sys_boot.mgmt.sleep(200), current_timestamp from h2
   union
   select i2 + 3, sys_boot.mgmt.sleep(300), current_timestamp from h2
   except
   select i2 + 1, sys_boot.mgmt.sleep(400), current_timestamp from h2);
select * from h1;

-- to run manually
-- adjust system timestamp to 5sec before midnight before running this
-- otherwise it may result in false positive 
--insert into h1 values (1);
--delete from h1 where 
--  (i1, sys_boot.mgmt.sleep(0), applib.current_date_in_julian()) in
--  (select i2, sys_boot.mgmt.sleep(0000), applib.current_date_in_julian()
--   from h2
--   union
--   select i2+ 1, sys_boot.mgmt.sleep(5500), applib.current_date_in_julian()
--   from h2);
--select * from h1;

drop table h1;
drop table h2;


--------------------------------------------------
-- Scalar subqueries in search condition
--------------------------------------------------
-- included in other sections

--------------------------------------------------
-- search with null
--------------------------------------------------
create table t(n1 int, n2 decimal(6,2), n3 double);
insert into t values 
    (1,null,3),(null,null,null),(1,3,null),(2,4,null),(3,3,3),(3,4,4);
select * from t;
delete from t where n2 < 2;
select * from t;
delete from t where n1 is null;
select * from t;
delete from t where not (n2 is not null);
select * from t;
delete from t where n3 <> 3;
select * from t;
delete from t where n3 in (select n3 from t);
select * from t;
insert into t values (3,3,3);
delete from t where n3 between
  (select min(n3) from t) and
  (select max(n3) from t);
select * from t;
delete from t where n3 between
  (select min(n3) from t) and
  (select max(n3) from t);
select * from t;

drop table t;

--------------------------------------------------
-- nested sub query
--------------------------------------------------
create table a(i1 int);
create table b(i1 int);
create table c(i1 int);
insert into a values (1),(2),(3),(4),(5);
insert into b values (1),(2),(3),(4),(5);
insert into c values (0),(3);

delete from a where i1 between
  (select min(i1) from b where i1 > (select max(i1) from c))
  and
  (select max(i1) from b where i1 > (select max(i1) from c));
select * from a;

drop table a;
drop table b;
drop table c;
