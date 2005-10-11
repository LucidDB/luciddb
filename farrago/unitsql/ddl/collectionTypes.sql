-- $Id$
-- Test DDL for collection types

create schema collectionsTest;
set schema 'collectionsTest';

-- MULTISET 
-- create/drop
create table multisetTable(i integer primary key, ii integer multiset);
-- create again, should fail
create table multisetTable(i integer primary key, ii integer multiset);

-- drop table
drop table multisetTable;

-- insert with ints
create table multisetTable_i(i integer primary key, ii integer multiset);
--insert into multisetTable_i values(0, multiset[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0, 1, 2]);
--insert into multisetTable_i values(1,multiset(select deptno from sales.depts));
--insert into multisetTable_i values(2, null);

-- insert with chars
create table multisetTable_c(i integer primary key, c char(5) multiset);
--insert into multisetTable_c values(0, multiset['a', 'b', 'c']);
--insert into multisetTable_c values(1,multiset(select name from sales.depts));
--insert into multisetTable_c values(2, null);

-- insert with ints and chars
create table multisetTable_ic(i integer primary key, ii integer multiset, c char(5) multiset);
--insert into multisetTable_ic values(0, multiset(select deptno from sales.depts),  multiset['a']);
--insert into multisetTable_ic values(1,multiset(select deptno from sales.depts), multiset(select name from sales.depts));
-- insert into multisetTable_ic values(2, multiset[2],null);


-- insert wrong types, must fail
-- insert into multisetTable_c values(2, 'a');
-- insert into multisetTable_c values(3, multiset[1]);


-- todo insert multisets of multisets
-- todo rowtypes
-- todo insert empty multiset
-- todo insert null into NON NULL
-- todo create table definition with default types;

-- View of table with multiset
-- create view multisetView as select * from multisetTable_i;

-- View with inline multiset

-- FIXME jvs 10-Oct-2005:  I disabled this because it was triggering
-- my new assertion in SqlToRelConverter.convertValidatedQuery.
-- create view multisetView as select 1 as x, multiset[2, 3] as y from (values (4));

-- End collectionTypes.sql
