-- $Id$
-- Test create or replace DDL 

create schema createorreplace;
set schema 'createorreplace';

create table foo (bar integer primary key);
insert into foo (bar) values (128);
create table foo2 (bar2 integer primary key);
insert into foo2 (bar2) values (256);

--
-- Table (disallowed)
--
create or replace table foo (bar2 integer primary key);

--
-- View
--
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

-- try to create a loop
create view loop1 as select * from foo;
create view loop2 as select * from loop1;
-- this should fail
create or replace view loop1 as select * from loop2;

-- but a diamond should be OK
create view diamond0 as select * from foo;
create view diamond1 as select * from diamond0;
create view diamond2 as select * from diamond0;
create view diamond3 as select * from diamond1,diamond2;
create or replace view diamond0 as select * from foo;

--
-- Index (disallowed)
--
create index idx on foo(bar);

-- should fail:  duplicate index
create index idx on foo(bar);

create or replace index idx on foo(bar);

--
-- Schema
--
create schema foo;
set schema 'foo';
create view v1 as select * from sales.depts;
create table bar (col integer primary key);
select * from v1;
select * from bar;

create or replace schema foo description 'blah';

select "description" from sys_fem."SQL2003"."LocalSchema"
 where "name" = 'FOO';

select * from foo.v1;
select * from foo.bar;

set schema 'foo';
create view v2 as select * from sales.depts;
select * from v2;

create or replace view v2 as select * from sales.emps;
select * from v2;

--
-- Foreign Server
--
create foreign data wrapper foo_wrapper
 library 'class net.sf.farrago.namespace.mock.MedMockForeignDataWrapper'
 language java;
                                                                                
create server foo_server
 foreign data wrapper foo_wrapper;
                                                                                
create foreign table foo_table(
    id int not null)
server foo_server
options (executor_impl 'JAVA', row_count '3');
                                                                                
select * from foo_table;
                                                                                
create or replace server foo_server
 foreign data wrapper foo_wrapper
 description 'blah';
                                                                                
select "description" from sys_fem.med."DataServer" where "name" = 'FOO_SERVER';
                                                                                
select * from foo_table;
                                                                                
--
-- Foreign Wrapper
--
create or replace foreign data wrapper foo_wrapper
  library 'class net.sf.farrago.namespace.mock.MedMockForeignDataWrapper'
  language java
  description 'blah';
                                                                                
select "description" from sys_fem.med."DataWrapper" where "name" = 'FOO_WRAPPER';
                                                                                
select * from foo_table;

--
-- Local Server
--

create server extra_ftrs_data_server
local data wrapper sys_ftrs;

create table extra_foo (bar integer primary key)
server extra_ftrs_data_server;

create or replace server extra_ftrs_data_server
local data wrapper sys_ftrs;

select * from extra_foo;

--
-- Bugs!
--
create schema bug453;
set schema 'bug453';
                                                                                
CREATE VIEW V1 AS SELECT name FROM SALES.EMPS;
CREATE VIEW V2 AS SELECT name FROM V1;
CREATE OR REPLACE VIEW V1 AS SELECT name FROM SALES.EMPS;
 
