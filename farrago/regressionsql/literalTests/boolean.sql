-- $Id$ 

-- test boolean literal
select true from values('true');
select TRUE from values('true');
select false from values('true');
select FALSE from values('true');

-- test datatype
create schema test;
set schema 'test';
create table t_boolean(boolean_col boolean not null primary key,boolean_col2 boolean);

-- negative test
insert into t_boolean values('true',null);
insert into t_boolean values('TRUE',null);
insert into t_boolean values('false',null);
insert into t_boolean values('FALSE',null);
insert into t_boolean values(1234,null);
insert into t_boolean values(-1234.03,null);
insert into t_boolean values(1e400,null);
insert into t_boolean values(-1.2345678901234e-200,null);
insert into t_boolean values(x'ff',null);
insert into t_boolean values(date '1999-01-08',null);
insert into t_boolean values(time '12:01:01',null);
insert into t_boolean values(timestamp '2004-12-01 12:01:01',null);

-- insert the right value
insert into t_boolean values(true,null);
insert into t_boolean values(TRUE,null);
insert into t_boolean values(false,unknown);
insert into t_boolean values(FALSE,null);

-- null value test, should fail with error state 22, code 004
insert into t_boolean values(null, null); 

select * from t_boolean;

drop table t_boolean;

