-- $Id$
-- Test queries which make use of foreign namespaces

-- create a private wrapper for mdr (don't use the standard mdr wrapper)
create foreign data wrapper test_mdr
library 'plugin/FarragoMedMdr.jar'
language java;

create server mof_repository
foreign data wrapper test_mdr
options(
    "org.netbeans.mdr.persistence.Dir" 'unitsql/ddl/mdr',
    extent_name 'MOF', 
    schema_name 'MODEL');

-- single-table projection with no filters
select "name" from mof_repository.model."Exception" order by 1;

-- single-table projection with filter
select "name" from mof_repository.model."Class" where "isAbstract"
order by 1;

-- two-way join
select 
    e."name" as exception_name,
    p."name" as param_name
from
    mof_repository.model."Exception" e
inner join
    mof_repository.model."Parameter" p
on e."mofId" = p."container"
order by exception_name,param_name;

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
on e."mofId" = p."container"
order by exception_name,param_name;

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
on ne.e_id = p."container"
order by namespace_name,exception_name,param_name;


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
on e."mofId" = p."container"
order by exception_name,param_name;

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
on e."mofId" = p."container"
order by exception_name,param_name;

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
    on n."mofId" = e."container") ne
inner join
    mof_repository.model."Parameter" p
on ne.e_id = p."container"
order by namespace_name,exception_name,param_name;
