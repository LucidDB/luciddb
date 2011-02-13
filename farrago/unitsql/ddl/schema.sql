-- $Id$
-- Test DDL on schemas

create schema s description 'a schema';

create table s.t(i int not null primary key);

!metadata getSchemas

-- should fail
drop schema s restrict;

!metadata getSchemas

-- should fail
drop schema s;

!metadata getSchemas

-- should succeed
drop schema s cascade;

!metadata getSchemas

-- should fail with duplicate name
create schema sales;

-- should fail due to invalid identifier
create schema "";

-- set a nonexistent schema:  should succeed
set schema 'erehwon';

-- but all table references should fail
select * from t;

-- set a numeric constant schema name:  should fail
set schema 1;

-- set an identifier as schema name:  should fail (surprise!)
set schema sales;

-- set a schema name which evaluates to a non-identifier:  should fail
set schema '1';

-- set a schema list:  should fail
set schema 'sales, marketing';

-- set a qualifed schema name:  should succeed
set schema 'localdb.sales';
select * from depts where 1=0;

-- set a quoted schema name:  should succeed
set schema '"SALES"';
select * from depts where 1=0;

-- set a non-existent quoted schema name:  should succeed
set schema '"sales"';
-- but lookup should fail
select * from depts where 1=0;

-- see what happens when we drop the current schema
create schema n;

set schema 'n';

create table nt(i int not null primary key);

drop schema n cascade;

select * from nt;

create table nt2(i int not null primary key);

-- test an easy compound schema definition
create schema nice
    create table t(i int not null primary key)
    create view v as select * from t
;

-- test a difficult compound schema definition
create schema nasty
    create view v as select * from t
    create table t(i int not null primary key)
;

-- test an impossible compound schema definition
create schema wicked
    create view v1 as select * from v2
    create view v2 as select * from v1
;

-- test usage of a non-reserved keyword (DATA) as an identifier
create table nice.strange(data int not null primary key);

insert into nice.strange(data) values (5);

select data from nice.strange;
