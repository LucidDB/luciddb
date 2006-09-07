!set verbose on
!set force on

set schema 'sales';

alter system set "calcVirtualMachine" = 'CALCVM_JAVA';
alter session implementation set jar sys_boot.sys_boot.luciddb_plugin;

-- 1.1 uncorrelated IN:  the only subquery that actually works!
explain plan without implementation for
select name from emps where deptno in (select deptno from depts) order by name;

explain plan for
select name from emps where deptno in (select deptno from depts) order by name;

select name from emps where deptno in (select deptno from depts) order by name;

-- 1.2 NOT IN
-- parsing works now
-- only in list (and not transformed into valueRel) has correct plan and result
explain plan for
select name from emps where deptno in (10, 20);

select name from emps where deptno in (10, 20) order by name;

-- wrong translation
-- This should be the same as 1.3. Solve them together.
explain plan without implementation for
select name from emps where deptno not in (select deptno from depts);

-- 1.3 uncorrelated NOT(x IN (sq)):
-- incorrect translation (should be anti-semijoin)
-- should be the same as (deptno not in (select deptno from depts));
-- this needs to handle NULL semantics as well
-- initial thinking is to translate that into not exists and use antijoin 
-- with the value generator (with special semantics for NULL keys, they do not 
-- belong to either matched or unmatched set)
explain plan without implementation for
select name from emps where not (deptno in (select deptno from depts));

explain plan for
select name from emps where not (deptno in (select deptno from depts));

-- 1.3 (WRONG RESULTS:  should have one row for employee John)
select name from emps where not (deptno in (select deptno from depts)) order by name;

-- 1.4 correlated IN:
-- correct translation. needs decorrelation
explain plan without implementation for
select name from emps where
deptno in (select deptno from depts where emps.empid*10=depts.deptno);

-- 1.4 is a special case of correlated exists. Equivalent to:
explain plan without implementation for
select name from emps where
exists (select deptno from depts 
        where emps.empid*10=depts.deptno and depts.deptno = emps.deptno);

-- 2.1 uncorrelated exists:  incorrect translation produces too many rows
-- (need to limit to at most one on join RHS; Broadbase inserts count(*))
-- LucidDB uses a special aggregate function that generates the value TRUE for each group
explain plan without implementation for
select name from emps where exists(select * from depts);

explain plan for
select name from emps where exists(select * from depts);

-- (WRONG RESULTS:  shoud not be filter over cross product)
select name from emps where exists(select * from depts) order by name;

-- 2.2 correlated exists:  passes translation; needs decorrelation;
explain plan without implementation for
select name from emps
where exists(select * from depts where depts.deptno=emps.deptno);

-- 3.1 uncorrelated scalar subquery:  passes most of translation
explain plan without implementation for
select name, (select count(*) from depts) from emps;

-- and even passes optimization
explain plan for
select name, (select count(*) from depts) from emps;

-- produces correct result only when null or one value is produced from the subquery.
-- need to add AggSingleRel for other cases
select name, (select count(*) from depts) from emps order by name;

-- correct
-- need to implement SingleValueAgg
explain plan without implementation for
select name, (select deptno from depts where deptno > 100) from emps order by name;

-- this should fail in validation
-- explain plan without implementation for
-- select name, (select * from depts) from emps;

-- check that scalar subquery type inference is correct
create table s (a int);
select empno, (select min(a) from s) from emps order by empno;
select empno, (select count(a) from s) from emps order by empno;
drop table s;

create table s (a int not null);
select empno, (select min(a) from s) from emps order by empno;
select empno, (select count(a) from s) from emps order by empno;
drop table s;

-- 3.2 correlated scalar subquery in select list:  
-- passes translation; needs decorrelation
-- also needs to add AggSingleRel()
explain plan without implementation for
select name, (select name from depts where depts.deptno=emps.deptno)
from emps;

-- 3.3 non correlated in where clause
-- parses now, need to add AggSingleRel
-- note can also use semi join
explain plan for 
select * from emps
where deptno = (select min(deptno) from depts);

-- result is correct simply because subquery produces only one value
select * from emps
where deptno = (select min(deptno) from depts) order by emps.empno;

-- incorrect result
-- need to implement SingleValueAgg
-- note can also use semi join
explain plan without implementation for
select * from emps
where deptno = (select deptno from depts);

-- 3.4 correlated scalar subquery in where clause:
-- 
explain plan without implementation for
select name
from emps
where name=(select name from depts where depts.deptno=emps.deptno);

explain plan without implementation for
select name
from emps
where name=(select max(name) from depts where depts.deptno=emps.deptno);

-- 3.5 scalar subquery as operand for an aggregation
-- needs to implement SingleValueAgg
explain plan without implementation for
select name, min((select name from depts where depts.deptno=emps.deptno))
from emps
group by name;

-- 3.6 HAVING clause scalar subquery currently produces incorrect plan
--     if HAVING clause references aggs. This is because HAVING clause is processed
--     before agg. So the subqueries get transformed into joins too early.
--     (Currently having clause is processed before agg processing because the way aggs
--      are gathered -- via expression conversion).
--     Ideally, aggs shoudl be gathered first,
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

-- 3.7 These two will report error for scalar subquery if its select list
-- contains more than one field.

-- explain plan without implementation for
-- select empno, (select min(empno), min(deptno) from emps)
-- from emps;

-- explain plan without implementation for
-- select empno
-- from emps
-- where empno = (select min(empno), min(deptno) from emps);

-- 4.1 nested correlations
--
create table depts2 (deptno integer, name varchar(20));

-- depts2 thinks that the correlation on emps.deptno comes from depts.deptno
-- currently the correlation lookup(by name) can only see correlation coming from the 
-- immediate outer relation. If emps.deptno is changed to emps.empno, an assert will
-- fail in createJoin()
-- also needs decorrelation
explain plan without implementation for 
select name 
from emps 
where exists(select * 
             from depts 
             where depts.deptno > emps.deptno or 
                   exists (select *
                           from depts2
                           where depts.deptno = depts2.deptno 
                                 and depts2.deptno = emps.deptno));


-- 4.2 correlation in more than one child
-- also has the same problem as 4.1 during createJoin if the correlation is on 
-- emps.empno for depts2.
--
-- depts2 sees the left neighbor for correlation while it should search for it on the
-- outer relation emps.
explain plan without implementation for 
select * from emps
where exists (select * from (select * from depts where depts.deptno = emps.deptno) t,
                            (select * from depts2 where depts2.deptno = emps.deptno) v);

--------------
-- clean up --
--------------
drop table depts2;
