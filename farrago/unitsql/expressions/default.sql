-- $Id$
-- Tests default values

-- Test bug (FRG-285) which occurs when default value is used by consecutive
-- statements.
create or replace schema expr_default;
set schema 'expr_default';
create table t (
  i integer not null primary key,
  j varchar(10) default 'foo');
insert into t (i) values (1);
insert into t (i) values (2);
select * from t order by i;

-- End default.sql

