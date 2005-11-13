-- $Id$
-- Test create or replace DDL 

create schema createorreplace;
set schema 'createorreplace';

create table foo (bar integer primary key);
insert into foo (bar) values (128);
create table foo2 (bar2 integer primary key);
insert into foo2 (bar2) values (256);

-- should fail:  replace not allowed on TABLE
create or replace table foo (bar2 integer primary key);

create view fooview as select * from foo;
select * from fooview;

-- simple case:  replace view, no dependencies
create or replace view fooview as select * from foo2;
select * from fooview;

create view fooview2 as select * from fooview;
select * from fooview2;

-- should succeed since dependency FOOVIEW2 remains valid
create or replace view fooview as select * from foo2;

-- should fail, cannot replace object if dependencies are invalidated
create or replace view fooview as select * from foo;

-- make sure fooview2 didn't get nuked from above
select * from fooview2;

create table foo3 (bar integer primary key, bar2 integer, bar3 varchar(25));
insert into foo3 (bar, bar2, bar3) values (512, 1024, 'FOOBAR');

-- should succeed because dependent view FOOVIEW2 is still valid (column BAR2 exists)
create or replace view fooview as select * from foo3;

select * from fooview;
select * from fooview2;

create index idx on foo(bar);

-- should fail:  duplicate index
create index idx on foo(bar);

-- should fail:  replace not allowed on INDEX
create or replace index idx on foo(bar);
