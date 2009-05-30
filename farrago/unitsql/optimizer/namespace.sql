-- $Id$
-- Test queries which make use of foreign namespaces

-- force usage of Java calculator
alter system set "calcVirtualMachine" = 'CALCVM_JAVA';

create server mof_repository
foreign data wrapper sys_mdr
options(
    "org.eigenbase.enki.implementationType" 'NETBEANS_MDR',
    "org.netbeans.mdr.persistence.Dir" 'unitsql/ddl/mdr',
    extent_name 'MOF', 
    schema_name 'MODEL');

-- special foreign server with some pushdown rules disabled
create server hsqldb_demo_limited_pushdown
foreign data wrapper sys_jdbc
options(
    driver_class 'org.hsqldb.jdbcDriver',
    url 'jdbc:hsqldb:testcases/hsqldb/scott',
    user_name 'SA',
    disabled_pushdown_rel_pattern '.*on proj.*',
    table_types 'TABLE,VIEW');

-- single-table projection with no filters
select "name" from mof_repository.model."Exception" order by 1;

-- single-table projection with filter
select "name" from mof_repository.model."Class" where "isAbstract"
order by 1;

-- two-way one-to-many join
select 
    e."name" as exception_name,
    p."name" as param_name
from
    mof_repository.model."Exception" e
inner join
    mof_repository.model."Parameter" p
on 
    e."mofId" = p."container"
order by 
    exception_name,param_name;

-- two-way join with filter
select 
    e."name" as exception_name,
    p."name" as param_name
from
    (select * 
    from mof_repository.model."Exception"
    where "name"='NameNotResolved') e
inner join
    mof_repository.model."Parameter" p
on 
    e."mofId" = p."container"
order by 
    exception_name,param_name;

-- two-way many-to-one join
select 
    p."name" as param_name,
    e."name" as exception_name
from
    mof_repository.model."Parameter" p
inner join
    mof_repository.model."Exception" e
on 
    p."container" = e."mofId"
order by 
    param_name,exception_name;

-- three-way join
select 
    namespace_name,
    exception_name,
    p."name" as param_name
from
    (select n."name" as namespace_name,e.
        "mofId" as e_id,e."name" as exception_name
    from
        mof_repository.model."Namespace" n
    inner join
        mof_repository.model."Exception" e
    on n."mofId" = e."container") ne
inner join
    mof_repository.model."Parameter" p
on 
    ne.e_id = p."container"
order by 
    namespace_name,exception_name,param_name;

-- use outputformat xmlattr for outer joins so we can see nulls
!set outputformat xmlattr

-- one-to-many left outer join
select 
    p."name" as package_name,
    i."name" as import_name
from
    mof_repository.model."Package" p
left outer join
    mof_repository.model."Import" i
on 
    p."mofId" = i."container"
order by 
    package_name,import_name;

-- many-to-one left outer join
select 
    p."name" as param_name,
    e."name" as exception_name
from 
    mof_repository.model."Parameter" p
left outer join
    mof_repository.model."Exception" e
on 
    p."container" = e."mofId"
where 
    p."name"='name'
order by
    param_name,exception_name;

-- filter which can be pushed down to foreign DBMS
select dname 
from hsqldb_demo.sales.dept
where deptno=20;

-- GROUP BY which can be pushed down to foreign DBMS
select deptno, sum(sal), count(*)
from hsqldb_demo.sales.emp
group by deptno
order by deptno;

-- GROUP BY with standalone count(distinct) can be pushed down
-- as two-level agg
select deptno, count(distinct sal)
from hsqldb_demo.sales.emp
group by deptno
order by deptno;

-- now explain plans for above queries
!set outputformat csv

explain plan for
select "name" from mof_repository.model."Exception" order by 1;

explain plan for
select "name" from mof_repository.model."Class" where "isAbstract"
order by 1;

explain plan for
select 
    e."name" as exception_name,
    p."name" as param_name
from
    mof_repository.model."Exception" e
inner join
    mof_repository.model."Parameter" p
on 
    e."mofId" = p."container"
order by 
    exception_name,param_name;

explain plan for
select 
    e."name" as exception_name,
    p."name" as param_name
from
    (select * 
    from mof_repository.model."Exception"
    where "name"='NameNotResolved') e
inner join
    mof_repository.model."Parameter" p
on 
    e."mofId" = p."container"
order by
     exception_name,param_name;

explain plan for
select 
    p."name" as param_name,
    e."name" as exception_name
from
    mof_repository.model."Parameter" p
inner join
    mof_repository.model."Exception" e
on 
    p."container" = e."mofId"
order 
    by param_name,exception_name;

explain plan for
select 
    namespace_name,
    exception_name,
    p."name" as param_name
from
    (select n."name" as namespace_name,e.
        "mofId" as e_id,e."name" as exception_name
    from
        mof_repository.model."Namespace" n
    inner join
        mof_repository.model."Exception" e
    on 
        n."mofId" = e."container") ne
inner join
    mof_repository.model."Parameter" p
on 
    ne.e_id = p."container"
order by 
    namespace_name,exception_name,param_name;

explain plan for
select 
    p."name" as package_name,
    i."name" as import_name
from
    mof_repository.model."Package" p
left outer join
    mof_repository.model."Import" i
on 
    p."mofId" = i."container"
order by 
    package_name,import_name;

explain plan for
select 
    p."name" as param_name,
    e."name" as exception_name
from 
    mof_repository.model."Parameter" p
left outer join
    mof_repository.model."Exception" e
on 
    p."container" = e."mofId"
where 
    p."name"='name'
order by 
    param_name,exception_name;

explain plan for 
select dname 
from hsqldb_demo.sales.dept
where deptno=20;

-- verify that even with complex pushdown rules disabled, we can
-- still push down both projection and filter
explain plan for 
select dname 
from hsqldb_demo_limited_pushdown.sales.dept
where deptno=20;

explain plan for 
select deptno, sum(sal), count(*)
from hsqldb_demo.sales.emp
group by deptno
order by deptno;

explain plan for 
select deptno, count(distinct sal)
from hsqldb_demo.sales.emp
group by deptno
order by deptno;

-- join on pseudocolumn (FRG-69)

explain plan for
select 
    e."name" as exception_name,
    p."name" as param_name
from
    mof_repository.model."Exception" e
inner join
    mof_repository.model."Parameter" p
on 
    e."mofId" = p."mofClassName"
;
