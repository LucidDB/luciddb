-- $Id$
-- Test expressions with different datatypes

-- VARCHAR comparison
select name from sales.emps where city='San Francisco';

-- test CHAR pad/truncation and VARCHAR truncation
create schema s;
create table t(i int not null primary key,c char(10),v varchar(10));
insert into t values (1,'goober','goober');
insert into t values (2,'endoplasmic reticulum','endoplasmic reticulum');
!set outputformat csv
select * from t;

-- Date/time/timestamp literals

select DATE '2004-12-01' as D1 from values('TRUE');
select TIME '12:01:01' as T1 from values('TRUE');
select TIMESTAMP '2004-12-01 12:01:01' as TS1 from values('TRUE');
