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
-- only in list (and not transformed into valueRel) has correct plan and result
explain plan for
select name from emps where deptno in (10, 20);

select name from emps where deptno in (10, 20) order by name;

-- wrong translation
-- This should be the same as 1.4. Solve them together.
explain plan without implementation for
select name from emps where deptno not in (select deptno from depts);

-- 1.4 uncorrelated NOT(x IN (sq)):
-- incorrect translation (should be anti-semijoin)
-- this needs to handle NULL semantics
-- initial thinking is to translate this into not exists and use antijoin 
-- with the value generator (with special semantics for NULL keys, they do not 
-- belong to either matched or unmatched set)
explain plan without implementation for
select name from emps where not (deptno in (select deptno from depts));

explain plan for
select name from emps where not (deptno in (select deptno from depts));

-- WRONG RESULTS:  should have one row for employee John
select name from emps where not (deptno in (select deptno from depts)) order by name;

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

-- 3.6 HAVING clause scalar subquery currently produces incorrect plan
--     if HAVING clause references aggs. This is because HAVING clause is processed
--     before agg. So the subqueries get transformed into joins too early.
--     (Currently having clause is processed before agg processing because the way aggs
--      are gathered -- via expression conversion).
--     Ideally, aggs should be gathered first,
--     then AggRels are generated, followed by processing of HAVING clause.
--
explain plan without implementation for
select name
from emps
group by name
having min(emps.name)=(select max(name) from depts);

-- work around is to rewrite the above query into this
explain plan without implementation for
select name
from
(select name, min(emps.name) min_name
 from emps
 group by name) v
where v.min_name=(select max(name) from depts);

-- 4.1 nested correlations
-- plan is still incorrect
-- for the lowest filterRel above the correlatorRel, the input reference
-- is not updated
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
order by emps.empno;

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

-- 6.1.2 The following, less complex, equivalent plan is possible with OR-expansion.
-- Note the IN lookup is required because union all does not remove duplicates.
-- Similarly, if using union the lookup is also required because union removes duplicates.
-- This could be a better plan than 6.1.1 because there's one fewer join.
explain plan without implementation for
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

-- 6.2.2 This is not equivalent to 6.2.1 because intersect removes duplicates.
explain plan without implementation for 
select empno from emps
where exists (select * from depts where depts.deptno = emps.deptno)
intersect
select empno from emps
where exists (select * from depts2 where depts2.deptno <> emps.empno);

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

-- 6.3.2 This is not equivalent to 6.3.1 because except removes duplicates.
explain plan without implementation for 
select empno from emps
where exists (select * from depts where depts.deptno = emps.deptno)
except
select empno from emps
where exists (select * from depts2 where depts2.deptno <> emps.empno);

-- 7.1 some multiset queries are not decorrelated because it contains set ops.
explain plan without implementation for 
select 'abc', multiset[deptno,empno] from emps;

explain plan without implementation for 
select * from unnest(select multiset[deptno] from depts);

--------------
-- clean up --
--------------
drop table emps2;
drop table depts2;
