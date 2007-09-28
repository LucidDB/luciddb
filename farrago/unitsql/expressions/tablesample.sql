-- $Id$
-- Test TABLESAMPLE clause

create schema tblsample;
set schema 'tblsample';

create table t (i int primary key, j int, k int);
insert into t (i, j ,k)
values (1,  1, 1),
       (2,  2, 2),
       (3,  1, 3),
       (4,  2, 4),
       (5,  1, 5),
       (6,  2, 1),
       (7,  1, 2),
       (8,  2, 3),
       (9,  1, 4),
       (10, 2, 5);

-- empty table
create table t2 (i int primary key, j int, k int);

create view v as select i, j, k from t where j = 2;

select * from t tablesample bernoulli(50) repeatable(42);

-- FTRS only supports Bernoulli, verify that it's used despite the request
-- for SYSTEM.
!outputformat csv
explain plan for select * from t tablesample system(10) repeatable(31415);
!outputformat table

select * from t tablesample system(10) repeatable(31415);

-- Some slightly more complex queries

!outputformat csv
explain plan for select * from t tablesample bernoulli(50) repeatable(1776) where j = 1;
!outputformat table

select * from t tablesample bernoulli(50) repeatable(1776) where j = 1;

-- Verify that empty tables pose no issues.
select * from t2 tablesample bernoulli(50) repeatable(1);

-- Sample from a view

select * from v tablesample bernoulli(50) repeatable(12345);

-- Sample from a subquery

!outputformat csv
explain plan for
select * 
from
  (select * from t where j = 1) tablesample bernoulli(33) repeatable(12345) 
where i >= 5;
!outputformat table

select * 
from
  (select * from t where j = 1) tablesample bernoulli(33) repeatable(280) 
where i >= 5;

-- Negative testing of TABLESAMPLE BERNOULLI/SYSTEM clauses.  These all fail
-- with parser errors.

select * from t tablesample bernoulli;

select * from t tablesample bernoulli();

select * from t tablesample bernoulli(1000);

select * from t tablesample bernoulli(-1);

select * from t tablesample bernoulli(50) repeatable(3.1415);

select * from t tablesample bernoulli(50) repeatable();

select * from t tablesample bernoulli(50) repeatable;

select * from t tablesample system;

select * from t tablesample system();

select * from t tablesample system(1000);

select * from t tablesample system(-1);

select * from t tablesample system(50) repeatable(3.1415);

select * from t tablesample system(50) repeatable();

select * from t tablesample system(50) repeatable;

select * from t tablesample bob(10);

-- end tablesample.sql
