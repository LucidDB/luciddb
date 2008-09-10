create schema presort;
set schema 'presort';
set path 'presort';

create table t_int(
    i int, 
    pk int generated always as identity not null primary key);
insert into t_int(i) values (5), (null), (-1), (null), (-1), (12), (5);

create table t_varchar(
    v varchar(20),
    pk int generated always as identity not null primary key);
insert into t_varchar(v)
values ('jkl'), (null), ('abc '), (null), ('abc'), ('xyz'), ('jkl  ');

create table t_numeric(
    n numeric(10,2),
    pk int generated always as identity not null primary key);
insert into t_numeric(n)
values (5.27), (null), (-1.45), (null), (-1.45), (5.28), (5.27);

create table t_date(
    d date,
    pk int generated always as identity not null primary key);
insert into t_date(d)
values (date '2005-07-25'), (null), (date '1997-06-01'), (null), 
(date '1997-06-01'), (date '2008-04-19'), (date '2005-07-25');

create table t_varbinary(
    b varbinary(20),
    pk int generated always as identity not null primary key);
insert into t_varbinary(b)
values (x'CAFEBABE'), (null), (x'CAFE'), (null), 
(x'CAFE'), (x'FEEDF00D'), (x'CAFEBABE');

create function uniq(c cursor)
returns table(c.*)
language java
parameter style system defined java
no sql
external name 
'class net.sf.farrago.test.FarragoTestUDR.removeDupsFromPresortedCursor';

select * from
table(uniq(cursor(select i from t_int order by i)))
order by i;

select * from
table(uniq(cursor(select v from t_varchar order by v)))
order by v;

select * from
table(uniq(cursor(select n from t_numeric order by n)))
order by n;

select * from
table(uniq(cursor(select d from t_date order by d)))
order by d;

select * from
table(uniq(cursor(select b from t_varbinary order by b)))
order by b;

-- should fail since out of order
select * from
table(uniq(cursor(select i from t_int order by i desc)))
order by i;

-- empty set
select * from
table(uniq(cursor(select i from t_int where i = 7)))
order by i;
