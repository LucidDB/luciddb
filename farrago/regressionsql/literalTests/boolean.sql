-- $Id$ 

-- test boolean literal
values true;
values TRUE;
values false;
values FALSE;
values tRUe;
values unknown;

-- test datatype
create schema test;
set schema 'test';
create table t_boolean(n int not null primary key, boolean_col boolean);
create table t_boolean2(n int not null primary key, boolean_col boolean not null);

-- negative test
insert into t_boolean values(1,'not bool');
insert into t_boolean values(2, 1234);
-- TODO: Fix error message, shouldn't get
--       Internal error: type BOOLEAN does not have a scale
insert into t_boolean values(3, -1234.03);
insert into t_boolean values(4, 1e400);
insert into t_boolean values(5, -1.2345678901234e-200);
insert into t_boolean values(6, x'ff');
insert into t_boolean values(7, date '1999-01-08');
insert into t_boolean values(8, time '12:01:01');
insert into t_boolean values(9, timestamp '2004-12-01 12:01:01');

-- insert the right value
insert into t_boolean values(101, true);
insert into t_boolean values(102, TRUE);
insert into t_boolean values(103, false);
insert into t_boolean values(104, FALSE);
insert into t_boolean values(105, null);
insert into t_boolean values(106, unknown);
insert into t_boolean values(107, 'true');
insert into t_boolean values(108, '  TRUE  ');
insert into t_boolean values(109, 'false');
insert into t_boolean values(110, '  FALSE  ');

select * from t_boolean;

drop table t_boolean;

-- null value test, should fail with error state 22, code 004
insert into t_boolean2 values(1, null); 

-- These tests are okay
insert into t_boolean2 values(2, unknown); 
insert into t_boolean2 values(3, true);
insert into t_boolean2 values(4, false);
insert into t_boolean2 values(5, 'true');
insert into t_boolean2 values(6, 'false');

select * from t_boolean2;

drop table t_boolean2;

drop schema test;
