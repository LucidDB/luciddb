-- $Id$ 

-- test real literal
 select 0.0 as t1 from values ('true');
 select 1. as t1 from values ('true');
 select .1 as t1 from values ('true');
 select 1004.30 as t1 from values ('true');
 select -34.84 as t1 from values ('true');
 select 1.2345678901234e+20 as t1 from values ('true');
 select 1.2345678901234e-20 as t1 from values ('true');
 select 10e40 as t1 from values ('true');
 select -10e40 as t1 from values ('true');
 select 10e-40 as t1 from values ('true');
 select -10e-40 as t1 from values ('true');
 select 0.0 as t1 from values ('true');
 select 1004.30 as t1 from values ('true');
 select -34.84 as t1 from values ('true');
 select 1.2345678901234e+200 as t1 from values ('true');
 select 1.2345678901234e-200 as t1 from values ('true');
 select 10e400 as t1 from values ('true');
 select -10e400 as t1 from values ('true');
 select 10e-400 as t1 from values ('true');
 select -10e-400 as t1 from values ('true');
 select -34.84 as t1 from values ('true');
 select -1004.30 as t1 from values ('true');
 select -1.2345678901234e+200 as t1 from values ('true');
 select -1.2345678901234e-200 as t1 from values ('true');

-- test datatype
create schema test;
set schema test;
create table t_real(real_col real not null primary key,
        real_col2 int);

-- negative test
insert into t_real values('true',null);
insert into t_real values(TRUE,null);
insert into t_real values(x'ff',null);
insert into t_real values(b'10',null);
insert into t_real values(date '1999-01-08',null);
insert into t_real values(time '12:01:01',null);
insert into t_real values(timestamp '2004-12-01 12:01:01',null);
insert into t_real values(10e400,null);

-- insert the right value
insert into t_real values(1234.999,null);
insert into t_real values(-4567890123456789.9,null);
insert into t_real values(-1.2345678901234e-200,null);

-- null value test
--insert into t_real values(null, null); 

select * from t_real;

drop table t_real;

