!set force on

set schema 'sales';

alter system set "calcVirtualMachine" = 'CALCVM_JAVA';
alter session implementation set jar sys_boot.sys_boot.luciddb_plugin;

create table depts2 (deptno integer, name varchar(20));

-- 1.1 uncorrelated IN
explain plan without implementation for
select name from emps where deptno in (select deptno from depts) order by name;

explain plan for
select name from emps where deptno in (select deptno from depts) order by name;

select name from emps where deptno in (select deptno from depts) order by name;

explain plan without implementation for
select name from emps
where (empno, deptno) in (select emps.empno, depts.deptno from emps, depts)
order by name;

explain plan for
select name from emps
where (empno, deptno) in (select emps.empno, depts.deptno from emps, depts)
order by name;

select name from emps
where (empno, deptno) in (select emps.empno, depts.deptno from emps, depts)
order by name;

-- 1.2 correlated IN:
explain plan without implementation for
select name from emps
where deptno in (select deptno from depts where emps.empno < depts.deptno*10)
order by name;

explain plan for
select name from emps
where deptno in (select deptno from depts where emps.empno < depts.deptno*10)
order by name;

select name from emps
where deptno in (select deptno from depts where emps.empno < depts.deptno*10)
order by name;

-- no table qualifier is necessary if correlated reference is not ambiguous
explain plan without implementation for
select name from emps
where deptno in (select deptno from depts where empno < depts.deptno*10)
order by name;

explain plan without implementation for
select name from emps
where deptno in (select deptno from depts where empno < deptno*10)
order by name;

-- 1.2 is a special case of correlated exists. Equivalent to:
explain plan without implementation for
select name from emps where
exists (select deptno from depts 
        where emps.empno < depts.deptno*10 and depts.deptno = emps.deptno)
order by name;

explain plan for
select name from emps where
exists (select deptno from depts 
        where emps.empno < depts.deptno*10 and depts.deptno = emps.deptno)
order by name;

select name from emps where
exists (select deptno from depts 
        where emps.empno < depts.deptno*10 and depts.deptno = emps.deptno)
order by name;

-- 1.3 NOT IN
explain plan for
select name from emps where deptno not in (10, 20);

select name from emps where deptno not in (10, 20) order by name;

-- uncorrelated NOT IN
explain plan without implementation for
select name from emps where deptno not in (select deptno from depts);

explain plan for
select name from emps where deptno not in (select deptno from depts);

select name from emps where deptno not in (select deptno from depts) order by name;

-- 1.4 uncorrelated NOT(x IN (subq)):
explain plan without implementation for
select name from emps where not (deptno in (select deptno from depts));

explain plan for
select name from emps where not (deptno in (select deptno from depts));

select name from emps where not (deptno in (select deptno from depts)) order by name;

-- 1.5 correlated NOT IN
explain plan without implementation for
select name from emps
where deptno not in (select deptno from depts where emps.empno < depts.deptno*10)
order by name;

explain plan for
select name from emps
where deptno not in (select deptno from depts where emps.empno < depts.deptno*10)
order by name;

select name from emps
where deptno not in (select deptno from depts where emps.empno < depts.deptno*10)
order by name;

-- 1.6 correlated NOT (x IN subq)
explain plan without implementation for
select name from emps
where not (deptno in (select deptno from depts where emps.empno < depts.deptno*10))
order by name;

explain plan for
select name from emps
where not (deptno in (select deptno from depts where emps.empno < depts.deptno*10))
order by name;

select name from emps
where not (deptno in (select deptno from depts where emps.empno < depts.deptno*10))
order by name;

-- 1.7 test NOT push down works
explain plan without implementation for
select empno from emps 
where not ((deptno, name) in (select deptno, name from depts) and deptno is not null 
           or deptno in (select deptno + 10 from depts));

explain plan for
select empno from emps 
where not ((deptno, name) in (select deptno, name from depts) and deptno is not null 
           or deptno in (select deptno + 10 from depts));

select empno from emps 
where not ((deptno, name) in (select deptno, name from depts) and deptno is not null 
           or deptno in (select deptno + 10 from depts))
order by name;

explain plan without implementation for
select min(empno) from emps group by deptno, name
having not ((deptno, name) in (select deptno, name from depts) and name is not null 
           or deptno in (select deptno + 10 from depts));

explain plan for
select min(empno) from emps group by deptno, name
having not ((deptno, name) in (select deptno, name from depts) and name is not null 
           or deptno in (select deptno + 10 from depts));

select min(empno) from emps group by deptno, name
having not ((deptno, name) in (select deptno, name from depts) and name is not null 
           or deptno in (select deptno + 10 from depts));

-- a bug discovered when doing the NOT pushdown
explain plan without implementation for 
select empno from emps 
where not (name in (select name from depts))
      and exists(select * from depts where deptno = emps.deptno);

explain plan for 
select empno from emps 
where not (name in (select name from depts))
      and exists(select * from depts where deptno = emps.deptno);

select empno from emps 
where not (name in (select name from depts))
      and exists(select * from depts where deptno = emps.deptno)
order by empno;

-- 2.1 uncorrelated exists.
-- Need to limit to at most one row on join RHS; Broadbase inserts count(*).
-- LucidDB uses a aggregate function(MIN(TRUE)) that generates the value TRUE
-- for each group
explain plan without implementation for
select name from emps where exists(select * from depts);

explain plan for
select name from emps where exists(select * from depts);

select name from emps where exists(select * from depts) order by name;

-- make sure empty subquery in exists will disqualify a row
select name from emps where exists(select * from depts2) order by name;

-- 2.2 correlated exists.
explain plan without implementation for
select name from emps
where exists(select * from depts where depts.deptno=emps.deptno)
order by name;

-- no table qualifier is necessary if correlated reference is not ambiguous
explain plan without implementation for
select name from emps
where exists(select * from depts where depts.deptno=empno)
order by name;

explain plan for
select name from emps
where exists(select * from depts where depts.deptno=emps.deptno)
order by name;

-- verify result is correct
select deptno, name from emps order by deptno, name;

select deptno from emps order by deptno;

select name from emps
where exists(select * from depts where depts.deptno=emps.deptno)
order by name;

-- 3.1 uncorrelated scalar subquery.
explain plan without implementation for
select name,
       (select count(*) from depts)
from emps;

explain plan for
select name,
       (select count(*) from depts)
from emps;

select name, 
       (select count(*) from depts)
from emps
order by name;

-- should return null in deptno
explain plan for
select name, 
       (select deptno from depts where deptno > 100)
from emps 
order by name;

select name, 
       (select deptno from depts where deptno > 100)
from emps 
order by name;

-- should report runtime error
select name, 
       (select deptno from depts)
from emps 
order by name;

-- this should report validation error
explain plan without implementation for
select name, (select * from depts) from emps;

-- check that scalar subquery type inference is correct
create table s (a int);
select empno,
       (select min(a) from s)
from emps
order by empno;

select empno, 
       (select count(a) from s)
from emps
order by empno;

drop table s;

create table s (a int not null);

select empno,
       (select min(a) from s)
from emps 
order by empno;

select empno,
       (select count(a) from s)
from emps
order by empno;

drop table s;

-- 3.2 correlated scalar subquery in select list:  
explain plan without implementation for
select name,
       (select name from depts where depts.deptno=emps.deptno)
from emps
order by name;

explain plan for
select name,
       (select name from depts where depts.deptno=emps.deptno)
from emps
order by name;

select name,
       (select name from depts where depts.deptno=emps.deptno)
from emps
order by name;

-- 3.3 non correlated in where clause
-- note can also use semi join
explain plan without implementation for 
select * from emps
where deptno = (select min(deptno) from depts);

explain plan for 
select * from emps
where deptno = (select min(deptno) from depts);

select * from emps
where deptno = (select min(deptno) from depts)
order by emps.empno;

-- Note: this too can use semi join
explain plan without implementation for
select * from emps
where deptno = (select deptno from depts);

explain plan for
select * from emps
where deptno = (select deptno from depts);

-- should report runtime error
select * from emps
where deptno = (select deptno from depts);

-- this should report validation error
explain plan without implementation for
select * from emps
where deptno = (select * from depts);

-- 3.4 correlated scalar subquery in where clause:
-- 
create table emps2 (deptno integer, name varchar(20));

insert into emps2 select deptno, name from emps;

explain plan without implementation for
select name
from emps
where name=(select name from emps2 where emps.deptno=emps2.deptno);

explain plan for
select name
from emps
where name=(select name from emps2 where emps.deptno=emps2.deptno);

-- should report runtime error: more than one row
select name
from emps
where name=(select name from emps2 where emps.deptno=emps2.deptno);

explain plan without implementation for
select name
from emps
where name=(select max(name) from emps2 where emps.deptno=emps2.deptno);

explain plan for
select name
from emps
where name=(select max(name) from emps2 where emps.deptno=emps2.deptno);

select name
from emps
where name=(select max(name) from emps2 where emps.deptno=emps2.deptno);

-- 3.5 scalar subquery as operand for an aggregation
explain plan without implementation for
select name, min((select name from emps2))
from emps
group by name;

explain plan for
select name, min((select name from emps2))
from emps
group by name;

-- should report runtime error
select name, min((select name from emps2))
from emps
group by name;

-- this query runs fine
explain plan without implementation for
select name, min((select max(name) from emps2))
from emps
group by name;

explain plan for
select name, min((select max(name) from emps2))
from emps
group by name;

select name, min((select max(name) from emps2))
from emps
group by name
order by name;

-- correlated
explain plan without implementation for
select deptno, min((select name from emps2 where emps2.name=emps.name))
from emps
group by deptno
order by deptno;

explain plan for
select deptno, min((select name from emps2 where emps2.name=emps.name))
from emps
group by deptno
order by deptno;

select deptno, min((select name from emps2 where emps2.name=emps.name))
from emps
group by deptno
order by deptno;

-- this should report run time error
explain plan without implementation for
select deptno, min((select name from emps2 where emps2.deptno=emps.deptno))
from emps
group by deptno;

explain plan for
select deptno, min((select name from emps2 where emps2.deptno=emps.deptno))
from emps
group by deptno;

select deptno, min((select name from emps2 where emps2.deptno=emps.deptno))
from emps
group by deptno;

-- this should report validation error
explain plan without implementation for
select deptno, sum((select * from emps2))
from emps
group by deptno;

-- Aggregate over window functions
explain plan without implementation for
select last_value((select deptno from depts)) over (order by empno)
from emps;

explain plan without implementation for
select last_value((select min(deptno) from depts)) over w
from emps window w as (order by empno);

-- select list contains correlated references
explain plan without implementation for 
select sum((select emps.empno from depts where depts.deptno = emps.deptno))
from emps;

explain plan for 
select sum((select emps.empno from depts where depts.deptno = emps.deptno))
from emps;

explain plan without implementation for 
select empno
from emps
where empno = (select min(emps.empno) from depts 
               where depts.deptno = emps.deptno);

explain plan for 
select empno
from emps
where empno = (select min(emps.empno) from depts
               where depts.deptno = emps.deptno);

-- bug with RexShuttle.visitCall() during efficient decorrelation
-- ABS function input type is changed to integer nullable after decorrelation, so
-- should the return type of this function.
explain plan without implementation for
select sum((select abs(depts.deptno) from depts where depts.deptno = emps.deptno)) from emps;

explain plan for
select sum((select abs(depts.deptno) from depts where depts.deptno = emps.deptno)) from emps;

-- Without bug fix, this will result in assertion failure for setNull() method in generated
-- java code for the cast expression.
select sum((select abs(depts.deptno) from depts where depts.deptno = emps.deptno)) from emps;

-- 3.6 HAVING clause scalar subquery.
--
--     Note: SQL2003 seems to contradict itself wrt to aggregates in HAVING clause.
--     In the rules for <set function specification>(which include aggregates),
--     this is allowed; However, in the rules for HAVING clause, only GBY columns can
--     be referenced "directly" in HAVING clause. The query below, probably not very
--     useful, satisfies both rules since the aggregate references the GBY columns
--
explain plan without implementation for
select name
from emps
group by name
having min(emps.name)=(select max(name) from depts);

-- the above query can also be rewritten into this
explain plan without implementation for
select name
from
(select name, min(emps.name) min_name
 from emps
 group by name) v
where v.min_name=(select max(name) from depts);

-- 3.6.1 HAVING clause with row types
explain plan without implementation for
select name from emps group by empno, name 
having (emps.name, emps.empno) in (('ab', 10), ('cd', 20));

-- 3.6.2 having clause column reference should not need to name the table
explain plan without implementation for
select name from emps group by name
having name in ('ab', 'cd');

-- 3.6.3 will fail when looking up emps.empno in group by clause
explain plan without implementation for
select count(*) from emps
where exists (select count(*) from depts group by emps.empno)
group by emps.empno;

-- will not decorrelate
explain plan without implementation for
select count(*) from emps
group by emps.empno
having exists (select count(*) from depts group by emps.empno);

-- 4.1 nested correlations
insert into depts2 select * from depts;

explain plan without implementation for 
select name 
from emps 
where exists(select * 
             from depts 
             where depts.deptno > emps.deptno or 
                   exists (select *
                           from depts2
                           where depts.name = depts2.name
                                 and depts2.deptno <> emps.empno));

explain plan for 
select name 
from emps 
where exists(select * 
             from depts 
             where depts.deptno > emps.deptno or 
                   exists (select *
                           from depts2
                           where depts.name = depts2.name
                                 and depts2.deptno <> emps.empno));

select name 
from emps 
where exists(select * 
             from depts 
             where depts.deptno > emps.deptno or 
                   exists (select *
                           from depts2
                           where depts.name = depts2.name
                                 and depts2.deptno <> emps.empno))
order by name;

-- 4.2 correlation in more than one child
explain plan without implementation for 
select empno from emps
where exists (select * from (select * from depts where depts.deptno = emps.deptno) t,
                            (select * from depts2 where depts2.deptno <> emps.empno) v);

explain plan for 
select empno from emps
where exists (select * from (select * from depts where depts.deptno = emps.deptno) t,
                            (select * from depts2 where depts2.deptno <> emps.empno) v);

select empno from emps
where exists (select * from (select * from depts where depts.deptno = emps.deptno) t,
                            (select * from depts2 where depts2.deptno <> emps.empno) v)
order by empno;

-- 4.3 correlation from two outer relations, which are at the same level
explain plan without implementation for
select depts.name, emps.deptno from emps, depts
where exists (
    select * from depts2
    where depts2.name = depts.name and depts2.deptno = emps.deptno);

explain plan for
select depts.name, emps.deptno from emps, depts
where exists (
    select * from depts2
    where depts2.name = depts.name and depts2.deptno = emps.deptno);

select depts.name, emps.deptno from emps, depts
where exists (
    select * from depts2
    where depts2.name = depts.name and depts2.deptno = emps.deptno);

-- 4.4 correlations from one relation to two outer relations at different level
explain plan without implementation for 
select name 
from emps 
where exists(select * 
             from depts 
             where depts.deptno > 10 and
                   exists (select *
                           from depts2
                           where depts.name = depts2.name
                                 and depts2.deptno = emps.deptno));

explain plan for 
select name 
from emps 
where exists(select * 
             from depts 
             where depts.deptno > 10 and
                   exists (select *
                           from depts2
                           where depts.name = depts2.name
                                 and depts2.deptno = emps.deptno));

select name 
from emps 
where exists(select * 
             from depts 
             where depts.deptno > 10 and
                   exists (select *
                           from depts2
                           where depts.name = depts2.name
                                 and depts2.deptno = emps.deptno))
order by name;

-- subquery in "lateral derived table"
-- 5.1 no correlation
explain plan without implementation for
select emps.empno, d.deptno
from emps,
lateral (select * from depts) as d
order by emps.empno, d.deptno;

explain plan for
select emps.empno, d.deptno
from emps,
lateral (select * from depts) as d
order by emps.empno, d.deptno;

select emps.empno, d.deptno
from emps,
lateral (select * from depts) as d
order by emps.empno, d.deptno;

-- 5.2 correlated: one correlation
explain plan without implementation for
select emps.empno, d.deptno
from emps,
lateral (select * from depts where depts.deptno = emps.deptno) as d
order by emps.empno;

explain plan for
select emps.empno, d.deptno
from emps,
lateral (select * from depts where depts.deptno = emps.deptno) as d
order by emps.empno;

select emps.empno, d.deptno
from emps,
lateral (select * from depts where depts.deptno = emps.deptno) as d
order by emps.empno;

-- 5.3 two lateral views: two correlations
explain plan without implementation for
select emps.empno, d.deptno, d2.deptno
from emps,
lateral (select * from depts where depts.deptno = emps.deptno) as d,
lateral (select * from depts2 where depts2.deptno <> emps.deptno) as d2
order by emps.empno;

explain plan for
select emps.empno, d.deptno, d2.deptno
from emps,
lateral (select * from depts where depts.deptno = emps.deptno) as d,
lateral (select * from depts2 where depts2.deptno <> emps.deptno) as d2
order by emps.empno;

select emps.empno, d.deptno, d2.deptno
from emps,
lateral (select * from depts where depts.deptno = emps.deptno) as d,
lateral (select * from depts2 where depts2.deptno <> emps.deptno) as d2
order by emps.empno, d.deptno, d2.deptno;

-- 5.4 two lateral views: three correlations
explain plan without implementation for
select emps.empno, d.deptno, d2.deptno
from emps,
lateral (select *
         from depts 
         where depts.deptno = emps.deptno) as d,
lateral (select *
         from depts2
         where depts2.deptno = d.deptno and depts2.deptno <> emps.deptno) as d2
order by emps.empno;

explain plan for
select emps.empno, d.deptno, d2.deptno
from emps,
lateral (select *
         from depts 
         where depts.deptno = emps.deptno) as d,
lateral (select *
         from depts2
         where depts2.deptno = d.deptno and depts2.deptno <> emps.deptno) as d2
order by emps.empno;

-- result set should be empty for this query
select emps.empno, d.deptno, d2.deptno
from emps,
lateral (select *
         from depts 
         where depts.deptno = emps.deptno) as d,
lateral (select *
         from depts2
         where depts2.deptno = d.deptno and depts2.deptno <> emps.deptno) as d2
order by emps.empno;

-- Correlations through set ops are not decorrelated.
-- 6.1 union/union all
-- Decorrelation is not performed.
explain plan without implementation for 
select empno from emps
where exists (select * from (select * from depts where depts.deptno = emps.deptno union all
                             select * from depts2 where depts2.deptno <> emps.empno));

explain plan without implementation for 
select empno from emps
where exists (select * from (select * from depts where depts.deptno = emps.deptno union
                             select * from depts2 where depts2.deptno <> emps.empno));

-- 6.1.1 A solution to 6.1 could be to expand the union and rewrite the exists
-- condition into exists(union branch 1) or exists(union branch 2).
explain plan without implementation for 
select empno from emps
where exists (select * from depts where depts.deptno = emps.deptno) 
      or exists (select * from depts2 where depts2.deptno <> emps.empno);

explain plan for 
select empno from emps
where exists (select * from depts where depts.deptno = emps.deptno) 
      or exists (select * from depts2 where depts2.deptno <> emps.empno);

-- 6.1.2 The following, less complex, equivalent plan is possible with OR-expansion.
-- Note the IN lookup is required because union all does not remove duplicates.
-- Similarly, if using union the lookup is also required because union removes duplicates.
-- This could be a better plan than 6.1.1 because there're one fewer joins.
explain plan without implementation for
select empno from emps where empno in (
    select empno from emps
    where exists (select * from depts where depts.deptno = emps.deptno)
    union all
    select empno from emps
    where exists (select * from depts2 where depts2.deptno <> emps.empno));

explain plan for
select empno from emps where empno in (
    select empno from emps
    where exists (select * from depts where depts.deptno = emps.deptno)
    union all
    select empno from emps
    where exists (select * from depts2 where depts2.deptno <> emps.empno));

-- 6.2 intersect
explain plan without implementation for 
select empno from emps
where exists (select * from (select * from depts where depts.deptno = emps.deptno intersect
                             select * from depts2 where depts2.deptno <> emps.empno));

-- 6.2.1 however, this is not equivalent to 6.2.
explain plan without implementation for 
select empno from emps
where exists (select * from depts where depts.deptno = emps.deptno) 
      and exists (select * from depts2 where depts2.deptno <> emps.empno);

explain plan for 
select empno from emps
where exists (select * from depts where depts.deptno = emps.deptno) 
      and exists (select * from depts2 where depts2.deptno <> emps.empno);

-- 6.2.2 The following plan is equivalent to 6.2.1 and has one fewer joins.
-- Note the IN lookup is required because intersect removes duplicates.
explain plan without implementation for
select empno from emps
where empno in (
    select empno from emps
    where exists (select * from depts where depts.deptno = emps.deptno)
    intersect
    select empno from emps
    where exists (select * from depts2 where depts2.deptno <> emps.empno));

explain plan for
select empno from emps
where empno in (
    select empno from emps
    where exists (select * from depts where depts.deptno = emps.deptno)
    intersect
    select empno from emps
    where exists (select * from depts2 where depts2.deptno <> emps.empno));

-- 6.3 except
explain plan without implementation for 
select empno from emps
where exists (select * from (select * from depts where depts.deptno = emps.deptno except
                             select * from depts2 where depts2.deptno <> emps.empno));

-- 6.3.1 however, this is not equivalent to 6.3.
explain plan without implementation for 
select empno from emps
where exists (select * from depts where depts.deptno = emps.deptno) 
      and not exists (select * from depts2 where depts2.deptno <> emps.empno);

explain plan for 
select empno from emps
where exists (select * from depts where depts.deptno = emps.deptno) 
      and not exists (select * from depts2 where depts2.deptno <> emps.empno);

-- 6.3.2 The following plan is equivalent to 6.3.1 and has one fewer joins.
-- Note the IN lookup is required because intersect removes duplicates.
explain plan without implementation for
select empno from emps
where empno in (
    select empno from emps
    where exists (select * from depts where depts.deptno = emps.deptno)
    except
    select empno from emps
    where exists (select * from depts2 where depts2.deptno <> emps.empno));

explain plan for
select empno from emps
where empno in (
    select empno from emps
    where exists (select * from depts where depts.deptno = emps.deptno)
    except
    select empno from emps
    where exists (select * from depts2 where depts2.deptno <> emps.empno));

-- 7.1 some multiset queries are not decorrelated because they contain set ops.
explain plan without implementation for 
select 'abc', multiset[deptno,empno] from emps;

explain plan without implementation for 
select * from unnest(select multiset[deptno] from depts);

-- 8.1 on clause
-- correlation from outer qb is not decorrelated
explain plan without implementation for
select name from emps
where exists (select * from depts d1 left outer join depts2 d2 
              on d1.deptno = emps.deptno and d1.deptno = d2.deptno);

-- non correlated scalar subq in ON clause gives parsing error
explain plan without implementation for
select * from emps left outer join depts
on emps.deptno = depts.deptno and emps.deptno = (select min(deptno) from depts2);

-- but this works
explain plan without implementation for
select * from emps left outer join depts
on emps.deptno = depts.deptno
where emps.deptno = (select min(deptno) from depts2);

-- so does this
explain plan without implementation for
select * from emps left outer join depts
on emps.deptno = depts.deptno
where emps.deptno = (select min(deptno) from depts2 where depts2.deptno = depts.deptno);

explain plan for
select * from emps left outer join depts
on emps.deptno = depts.deptno
where emps.deptno = (select min(deptno) from depts2 where depts2.deptno = depts.deptno);

-- 9 views built on top of correlated queries
drop table emps2;
drop table depts2;

create table emps2 (name varchar(40), empno int, deptno int);
create table depts2 (name varchar(40), deptno int);

insert into emps2 select name, empno, deptno from emps;
insert into depts2 select name, deptno from depts;

-- 9.1 view over query with correlated IN subquery
create view v1 (ename, empno, deptno) as
select name, empno, deptno from emps
where deptno in (select deptno from depts where emps.empno < depts.deptno*10);

explain plan for
select name, empno, deptno from emps
where deptno in (select deptno from depts where emps.empno < depts.deptno*10);

explain plan for
select * from v1;

select name, empno, deptno from emps
where deptno in (select deptno from depts where emps.empno < depts.deptno*10)
order by name;

select * from v1 order by ename;

-- 9.2 view over query with correlated EXISTS subquery
create view v2 (ename, empno, deptno) as
select name, empno, deptno from emps2
where exists(select * from depts2 where depts2.deptno=emps2.deptno);

explain plan for
select name, empno, deptno from emps2
where exists(select * from depts2 where depts2.deptno=emps2.deptno);

explain plan for
select * from v2;

select name, empno, deptno from emps2
where exists(select * from depts2 where depts2.deptno=emps2.deptno)
order by name;

select * from v2 order by ename;

-- 9.3 view on top of joined views, each over queries with correlations
create view v3 (empnov1, empnov2) as
select v1.empno, v2.empno from v1, v2 where v1.empno = v2.empno;

explain plan for
select v1.empno, v2.empno from v1, v2 where v1.empno = v2.empno;

explain plan for
select * from v3;

select v1.empno, v2.empno from v1, v2
where v1.empno = v2.empno order by v1.empno;

select * from v3 order by v3.empnov1;

-- 9.4 view over views that are correlated to each other.
-- Each of the views is itself over queries with correlations.
create view v4 (empno, deptno) as
select empno, deptno from v1
where v1.empno in (select v2.empno from v2 where v2.deptno = v1.deptno);

explain plan for
select empno, deptno from v1
where v1.empno in (select v2.empno from v2 where v2.deptno = v1.deptno);

explain plan for
select * from v4;

select empno, deptno from v1
where v1.empno in (select empno from v2 where v2.deptno = v1.deptno)
order by empno;

select * from v4 order by empno;

-- 9.5 view on top of two views that are correlated to each other, within a
-- scalar subquery.
create view v5 (empno, deptno) as
select empno, deptno from v1
where v1.empno = (select max(v2.empno) from v2 where v2.deptno = v1.deptno);

explain plan for
select empno, deptno from v1
where v1.empno = (select max(v2.empno) from v2 where v2.deptno = v1.deptno);

explain plan for
select * from v5;

select empno, deptno from v1
where v1.empno = (select max(v2.empno) from v2 where v2.deptno = v1.deptno);

select * from v5;

drop view v3;
drop view v4;
drop view v5;

drop view v1;
drop view v2;

-- 10 Optimization to decorrelate a scalar subquery without using value generator.
-- This can be done when the inner relation itself can be the lookup table
-- without having to join with the outer relation first

-- 10.1 outer relations are not referenced in the select list of the subquery.
explain plan without implementation for
select
    avg((select deptno from depts where deptno = emps.deptno))
from emps;

explain plan for
select
    avg((select deptno from depts where deptno = emps.deptno))
from emps;

select
    avg((select deptno from depts where deptno = emps.deptno))
from emps;

-- check results against this query
explain plan for
select
    deptno,
    (select deptno from depts where deptno = emps.deptno)
from emps;

select
    deptno,
    (select deptno from depts where deptno = emps.deptno) 
from emps
order by deptno;


-- 10.2 outer relations are referenced in the select list of the subquery.
explain plan without implementation for
select
    avg((select emps.deptno from depts where deptno = emps.deptno))
from emps;

explain plan for
select
    avg((select emps.deptno from depts where deptno = emps.deptno))
from emps;

select
     avg((select emps.deptno from depts where deptno = emps.deptno))
from emps;

-- check result against this query
explain plan for
select 
    deptno,
    (select emps.deptno from depts where deptno = emps.deptno) 
from emps;

select
    deptno,
     (select emps.deptno from depts where deptno = emps.deptno) 
from emps
order by deptno;

-- negative cases
explain plan without implementation for
select 
    deptno,
    (select deptno from emps where deptno = depts.deptno) 
from depts;

explain plan for
select
    deptno,
     (select deptno from emps where deptno = depts.deptno) 
from depts;

explain plan without implementation for
select
    deptno,
     (select depts.deptno from emps where deptno = depts.deptno) 
from depts;

explain plan for
select
    deptno,
     (select depts.deptno from emps where deptno = depts.deptno) 
from depts;

-- 10.3 Unique columns need to be not null to be considered unique keys.
create table test1(a int primary key, b int);
create table test2(a int primary key, b int);
create table test3(a int primary key, b int unique);
create table test4(a int primary key, b int not null unique);

explain plan for 
select (select test1.b from test1 where test1.a = test2.a) from test2;

explain plan for 
select (select test1.b from test1 where test1.b = test2.a) from test2;

explain plan for 
select (select test2.b from test1 where test1.a = test2.a) from test2;

explain plan for 
select (select test3.b from test3 where test3.b = test2.a) from test2;

explain plan for 
select (select test4.b from test4 where test4.b = test2.a) from test2;

-- lookup table is a join
explain plan for
select 
    (select (test1.b + test3.b) 
     from test1, test3 
     where test3.a = test2.b and test1.a = test2.a) 
from test2;

explain plan for
select 
    (select (test1.b + test4.b) 
     from test1, test4 
     where test4.b = test2.b and test1.a = test2.a) 
from test2;

drop table test1;
drop table test2;
drop table test3;
drop table test4;

-- 10.4 subquery selects constants
explain plan for
SELECT 
    deptno,
    (select 1 FROM depts where deptno = emps.deptno)
FROM 
    emps;

SELECT 
    deptno,
    (select 1 FROM depts where deptno = emps.deptno)
FROM 
    emps
order by deptno;

explain plan for
SELECT 
    deptno,
    (select cast(1 as decimal(10,2)) FROM depts where deptno = emps.deptno)
FROM 
    emps;

SELECT
    deptno,
    (select cast(1 as decimal(10,2)) FROM depts where deptno = emps.deptno)
FROM 
    emps
order by deptno;

-- 10.5 correlation in filter references expressions from the RHS
explain plan for
select deptno,
    (select deptno from depts where deptno = emps.deptno+10)
from emps;

select deptno,
    (select deptno from depts where deptno = emps.deptno+10)
from emps
order by deptno;

-- 10.6 the only correlation is in the select list of the subquery
explain plan for
select deptno,
    (select emps.deptno from depts where deptno = 20)
from emps;

select deptno, 
    (select emps.deptno from depts where deptno = 20)
from emps
order by deptno;

-- 10.7 subquery select list is an aggregate
-- 10.7.1 the subquery is correlated
explain plan for
select deptno,
    (select sum(deptno) from emps where deptno = depts.deptno)
from depts;

select deptno,
    (select sum(deptno) from emps where deptno = depts.deptno)
from depts
order by deptno;

explain plan for
select deptno, 
    (select sum(depts.deptno) from emps where deptno = depts.deptno) 
from depts;

select deptno, 
    (select sum(depts.deptno) from emps where deptno = depts.deptno)
from depts
order by deptno;

explain plan for
select deptno, 
    (select sum(depts.deptno) from emps where deptno + 10 = depts.deptno) 
from depts;

select deptno, 
    (select sum(depts.deptno) from emps where deptno + 10 = depts.deptno)
from depts
order by deptno;

-- negative cases
explain plan for
select deptno, 
    (select sum(deptno) from emps where deptno = depts.deptno+1)
from depts;

-- NOTE: count() always returns a non-null value even in scalar subqueries
-- however currently correlated scalar subquery produces nullable field
-- for the count() in the outer query. This will probably be the behavior
-- so when moving agg across subquery boundaries(for example, during decorrelate),
-- the type might need to be patched up to return nullable for count() aggs.
explain plan for
select deptno, 
    (select count(deptno) from emps where deptno = depts.deptno)
from depts;

select deptno, 
    (select count(deptno) from emps where deptno = depts.deptno) a
from depts
order by deptno;

-- count(*) is translated to count(true)
explain plan for
select deptno, 
    (select count(*) from emps where deptno = depts.deptno)
from depts;

select deptno, 
    (select count(*) from emps where deptno = depts.deptno) a
from depts
order by deptno;

-- multiple aggregates
explain plan for
select deptno, 
    (select count(*) from emps where deptno = depts.deptno),
    (select sum(deptno) from emps where deptno = depts.deptno)
from depts;

select deptno, 
    (select count(*) from emps where deptno = depts.deptno),
    (select sum(deptno) from emps where deptno = depts.deptno)
from depts
order by deptno;

-- 10.7. the subquery is uncorrelated
explain plan for
select deptno, 
    (select sum(depts.deptno) from emps) 
from depts;

select deptno, 
    (select sum(depts.deptno) from emps)
from depts
order by deptno;

-- negative test
create table depts3(deptno int, name varchar(20));
insert into depts3 select * from depts;

explain plan for
select deptno, 
    (select sum(depts.deptno) from emps) 
from depts3 depts;

select deptno, 
    (select sum(depts.deptno) from emps)
from depts3 depts
order by deptno;

-- NOTE: count() always returns a non-null value even in scalar subqueries
-- however currently correlated scalar subquery produces nullable field
-- for the count() in the outer query. This will probably be the behavior
-- so when moving agg across subquery boundaries(for example, during decorrelate),
-- the type might need to be patched up to return nullable for count() aggs.
create table emps3 (a int);

-- check null indicator works
-- count($cor) is transformed to count(true from RHS)
explain plan for
select deptno, 
    (select count(depts.deptno) from emps3)
from depts;

select deptno, 
    (select count(depts.deptno) from emps3)
from depts
order by deptno;

-- test removing trivial single_value aggregate works
explain plan for
select deptno, 
    (select count(*) + count(depts.deptno) + count(a) from emps3)
from depts;

select deptno, 
    (select count(*) + count(depts.deptno) + count(a) from emps3)
from depts
order by deptno;

insert into emps3 values(null);

select deptno, 
    (select count(depts.deptno) from emps3)
from depts
order by deptno;

select deptno, 
    (select count(*) + count(depts.deptno) + count(a) from emps3)
from depts
order by deptno;

-- multiple aggregates
explain plan for
select deptno, 
    (select count(depts.deptno) from emps),
    (select sum(depts.deptno) from emps)
from depts;

select deptno, 
    (select count(depts.deptno) from emps),
    (select sum(depts.deptno) from emps)
from depts
order by deptno;

-- verify that nullability is preserved when not decorrelating with value generators
create table emps4 (a int);
create table depts4(a int primary key);
create table depts5(a int);

insert into depts4 values (1);
insert into depts5 values (2);

select a, (select max(d.a) from emps4) from depts4 d;
select a, (select max(d.a) from emps4) from depts5 d;

insert into emps4 values (null);

select a, (select max(d.a) from emps4) from depts4 d;
select a, (select max(d.a) from emps4) from depts5 d;

-- The count() fix when using value generater to decorrelate
-- now it returns 0 correctly
truncate table emps3;
truncate table emps4;
insert into emps4 values (2);

explain plan for
select (select count(e2.a) from emps3 e1) from emps4 e2;

-- should return 0
select (select count(e2.a) from emps3 e1) from emps4 e2;

-- The planner workaround: for the planner used in RelDecorrelator,
-- make sure the RelNode representation remains a tree.
explain plan for
select (select count(e2.a) from emps3 e1) from emps3 e2;

-- TODO: in predicate correlations can be improved too
-- Inner correlation
explain plan for
SELECT empno
FROM 
    emps
where 
    deptno in (select deptno FROM depts where deptno = emps.deptno);

explain plan for
SELECT empno
FROM 
    emps
where 
    deptno in (select emps.deptno FROM depts where deptno = emps.deptno);

-- Outer correlation
explain plan for
SELECT empno
FROM 
    emps
where 
    deptno not in (select deptno FROM depts where deptno = emps.deptno);

explain plan for
SELECT empno
FROM 
    emps
where 
    deptno not in (select emps.deptno FROM depts where deptno = emps.deptno);

-- This used to cause a hang due to a Hep bug
explain plan for
select
   (select min(name||' Jr.') from emps2 where deptno = depts2.deptno),
   (select max(name||' Jr.') from emps2 where deptno = depts2.deptno)
from depts2;

--------------
-- clean up --
--------------
drop table emps2;
drop table emps3;
drop table emps4;
drop table depts2;
drop table depts3;
drop table depts4;
drop table depts5;
