-- $Id$ 

-- test int literal

values 0;
values 1234;
values -1234;
values 34.5;
values 32767;
values -32767;
values 100000;


values 123456;
values -123456;

values 2147483647;
values -2147483647;
values 1000000000000;



values 4567890123456789;
values -4567890123456789;

-- test datatype
create schema test;
set schema 'test';
create table t_int(int_col int not null primary key,
        int_col2 int);

-- negative test
insert into t_int values('true',null);
insert into t_int values(TRUE,null);
insert into t_int values(1e400,null);
insert into t_int values(x'ff',null);
insert into t_int values(date '1999-01-08',null);
insert into t_int values(time '12:01:01',null);
insert into t_int values(timestamp '2004-12-01 12:01:01',null);

-- insert the right value
insert into t_int values(-1235.03,null);
insert into t_int values(1234,null);
insert into t_int values(-4567890123456789,null);
insert into t_int values(-1.2345678901234e-200,null);
-- null value test
--insert into t_int values(null, null); 

select * from t_int;

drop table t_int;
drop schema test;
