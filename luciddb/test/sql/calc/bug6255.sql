---
--- LER6255: use current_date_in_julian() udf in subqueries
---
create schema bug6255;

set schema 'bug6255';

create table t (a int primary key);
create table f (b int);

insert into t values (1);
insert into f values (2);

-- This bug occurs when efficient decorrelation(via rules) is possible, and
-- nullIndicator is used to rewrite expressions produced by the correlated
-- subquery.
explain plan without implementation for
select sum(select applib.current_date_in_julian() from t where t.a = f.b) from f;

select sum(select applib.current_date_in_julian() from t where t.a = f.b) from f;

drop table t;
create table t (a int);
insert into t values (1);
 
-- Value generator decorrelation is not affected by the bug.
explain plan without implementation for
select sum(select applib.current_date_in_julian() from t where t.a = f.b) from f;

select sum(select applib.current_date_in_julian() from t where t.a = f.b) from f;

drop table t;
drop table f;

drop schema bug6255;