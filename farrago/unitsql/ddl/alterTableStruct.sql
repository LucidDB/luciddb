-- $Id$
-- Test ALTER TABLE statements which actually modify table structure

-- use Java calc for better error messages
alter system set "calcVirtualMachine" = 'CALCVM_JAVA';

create schema x;

-- start with an analyzed table so that some columns will have stats
-- and some won't
create table x.t(a varchar(5) not null primary key);
create view x.v as select * from x.t;
insert into x.t values ('hi');
analyze table x.t compute statistics for all columns;
alter table x.t add b varchar(20);
select * from x.t order by a;
insert into x.t values ('bye', 'for now');
select * from x.t order by a;

-- make sure new column ordinal got assigned correctly
select column_name, ordinal_position
from sys_boot.jdbc_metadata.columns_view_internal
where table_schem = 'X' and table_name = 'T'
order by ordinal_position;

-- verify that view was early-bound to original table definition
select * from x.v order by a;

-- add column with default value; also test filler keyword "column"
alter table x.t add column c int default 5;

-- make sure we can index new columns, and that subsequent
-- alters can deal with presence of unclustered index
create index z on x.t(c);

-- one more column with a default value of more complicated type
alter table x.t add cc date default date '2008-10-30';

-- add an identity column
alter table x.t add g bigint generated always as identity;

-- make sure new view can see new columns
create view x.v2 as select * from x.t;
select * from x.v2 order by a;

-- negative test:  duplicate column name
alter table x.t add a int;

-- negative test:  unknown datatype
alter table x.t add d scrounge;

-- negative test:  illegal datatype
alter table x.t add e numeric(5000);

-- negative test:  NOT NULL column with no default value
alter table x.t add f int not null;

-- negative test:  can't alter a view
alter table x.v add h int;

-- negative test:  can't tack on constraints other than NOT NULL
alter table x.t add i int not null unique;

-- negative test:  at most one identity column per table
alter table x.t add j int generated always as identity;

-- make sure we can analyze table with new columns
analyze table x.t compute statistics for all columns;

-- make sure we can rebuild table with new columns
alter table x.t rebuild;

-- make sure we can truncate table with new columns
truncate table x.t;

-- make sure we can drop table with new columns
drop table x.t cascade;

create table x.t2(a varchar(15) not null primary key);

-- this should work, since t2 is empty
alter table x.t2 add b int not null;

-- but this should fail
insert into x.t2(a) values ('whoops');

-- make sure we can add a UDT-typed column
create type x.rectilinear_coord as (
    x_off double default 0,
    y_off double default 0
) final;
alter table x.t2 add c x.rectilinear_coord;
insert into x.t2 values('bullseye', 0, new x.rectilinear_coord());
select a, b, t2.c.x_off, t2.c.y_off from x.t2 order by a;
alter table x.t2 add d x.rectilinear_coord;
select a, b, t2.c.x_off, t2.c.y_off, t2.d.x_off, t2.d.y_off from x.t2 
order by a;
insert into x.t2 
values('miss', 1, new x.rectilinear_coord(), new x.rectilinear_coord());
select a, b, t2.c.x_off, t2.c.y_off, t2.d.x_off, t2.d.y_off from x.t2 
order by a;

-- negative test:  can't alter a foreign table
create foreign table x.foreign_dept(
    dno integer,
    dname char(20),
    loc char(20))
server hsqldb_demo
options(schema_name 'SALES', table_name 'DEPT');
alter table x.foreign_dept add sal int;

-- negative test:  can't add a sequence which cannot produce enough values
-- to satisfy existing rows
create table x.t3(i int not null primary key);
insert into x.t3 values (1), (2), (3), (4), (5);
alter table x.t3 add j bigint generated always as identity (maxvalue 3);
-- make sure table was left intact
insert into x.t3 values (6);
select * from x.t3 order by i;

-- negative test:  mock data server doesn't implement ALTER TABLE ADD COLUMN
create server mock_local_server
local data wrapper sys_mock;
create table x.mock_empty_table(
    id int not null primary key)
server mock_local_server;
alter table x.mock_empty_table add column blah int;

-- test crash recovery via fault injection
-- (we add a column of multiset type to make sure the special cleanup
--  required for this kicks in)
create procedure x.set_farrago_property(
in name varchar(128),val varchar(128))
language java
no sql
external name 'class net.sf.farrago.test.FarragoTestUDR.setFarragoProperty';
create procedure x.simulate_catalog_recovery()
language java
no sql
external name 'class net.sf.farrago.test.FarragoTestUDR.simulateCatalogRecovery';
call x.set_farrago_property(
  'net.sf.farrago.ddl.DdlReloadTableStmt.crash', 'true');
alter table x.t3 add z integer multiset;
-- x.t3 is in a bad state now:  a query or insert would crash for real;
-- but we can fix it
call x.simulate_catalog_recovery();
insert into x.t3(i) values (8);
select * from x.t3 order by i;


-- retest most of the above with LucidDB session personality

drop schema x cascade;
alter session implementation set jar sys_boot.sys_boot.luciddb_plugin;

create schema x;

-- start with an analyzed table so that some columns will have stats
-- (and more importantly as it turns out, index page counts) while some won't
create table x.t(a varchar(5) not null primary key);
create view x.v as select * from x.t;
insert into x.t values ('hi');
analyze table x.t compute statistics for all columns;
alter table x.t add b varchar(20);
select * from x.t order by a;
insert into x.t values ('bye', 'for now');
select * from x.t order by a;

-- make sure new column ordinal got assigned correctly
select column_name, ordinal_position
from sys_boot.jdbc_metadata.columns_view_internal
where table_schem = 'X' and table_name = 'T'
order by ordinal_position;

-- verify that view was early-bound to original table definition
select * from x.v order by a;

-- add column with default value
alter table x.t add c int default 5;

-- make sure we can index new columns, and that subsequent
-- alters can deal with presence of unclustered index
create index z on x.t(c);

-- one more column with a default value of more complicated type
alter table x.t add cc date default date '2008-10-30';

-- add an identity column
alter table x.t add g bigint generated always as identity;

-- make sure new view can see new columns
create view x.v2 as select * from x.t;
select * from x.v2 order by a;

-- negative test:  NOT NULL column with no default value
alter table x.t add f int not null;

-- make sure we can analyze table with new columns
analyze table x.t compute statistics for all columns;

-- make sure we can rebuild table with new columns
alter table x.t rebuild;

-- make sure we can truncate table with new columns
truncate table x.t;

-- make sure we can drop table with new columns
drop table x.t cascade;

create table x.t2(a varchar(15) not null primary key);

-- this should work, since t2 is empty
alter table x.t2 add b int not null;

-- but this should fail
insert into x.t2(a) values ('whoops');

-- make sure we can add a UDT-typed column
create type x.rectilinear_coord as (
    x_off double default 0,
    y_off double default 0
) final;
alter table x.t2 add c x.rectilinear_coord;
insert into x.t2 values('bullseye', 0, new x.rectilinear_coord());
select a, b, t2.c.x_off, t2.c.y_off from x.t2 order by a;
alter table x.t2 add d x.rectilinear_coord;
select a, b, t2.c.x_off, t2.c.y_off, t2.d.x_off, t2.d.y_off from x.t2 
order by a;
insert into x.t2 
values('miss', 1, new x.rectilinear_coord(), new x.rectilinear_coord());
select a, b, t2.c.x_off, t2.c.y_off, t2.d.x_off, t2.d.y_off from x.t2 
order by a;

-- negative test:  can't add a sequence which cannot produce enough values
-- to satisfy existing rows
create table x.t3(i int not null primary key);
insert into x.t3 values (1), (2), (3), (4), (5);
alter table x.t3 add j bigint generated always as identity (maxvalue 3);
-- also make sure rejected rows setting is ignored for reentrant SQL
alter session set "errorMax" = 500;
alter table x.t3 add j bigint generated always as identity (maxvalue 3);
alter session set "errorMax" = 0;
-- make sure table was left intact
insert into x.t3 values (20);
select * from x.t3 order by i;

-- test deleted rows; this also demonstrates "holes" in new sequences,
-- which are expected behavior (if you don't like them, rebuild the table
-- before altering it)
delete from x.t3 where i=3;
alter table x.t3 add k int generated by default as identity;
insert into x.t3 values (6, 100);
insert into x.t3(i) values (7);
select * from x.t3 order by i;
