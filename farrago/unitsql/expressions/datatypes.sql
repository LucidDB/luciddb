-- $Id$
-- Test expressions with different datatypes

-- VARCHAR comparison
select name from sales.emps where city='San Francisco';

-- test CHAR pad/truncation and VARCHAR truncation
create schema s;
set schema 's';
create table t(i int not null primary key,c char(10),v varchar(10));
insert into t values (1,'goober','goober');
insert into t values (2,'endoplasmic reticulum','endoplasmic reticulum');
!set outputformat csv
select * from t;
!set outputformat table

-- Binary as hexstring
select public_key from sales.emps order by 1;

-- Date/time/timestamp literals

values DATE '2004-12-01';
values TIME '12:01:01';
values TIMESTAMP '2004-12-01 12:01:01';

-- Exponent literals
-- dtbug 271
select 0e0 from (values (0));

-- End datatypes.sql


