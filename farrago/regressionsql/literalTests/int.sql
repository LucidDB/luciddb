-- $Id$ 

-- test int literal

 select 0 as t1 from values ('true');
 select 1234 as t1 from values ('true');
 select -1234 as t1 from values ('true');
 select 34.5 as t1 from values ('true');
 select 32767 as t1 from values ('true');
 select -32767 as t1 from values ('true');
 select 100000 as t1 from values ('true');


 select 123456 as t1 from values ('true');
 select -123456 as t1 from values ('true');

 select 2147483647 as t1 from values ('true');
 select -2147483647 as t1 from values ('true');
 select 1000000000000 as t1 from values ('true');



select 4567890123456789 as t1 from values ('true');
select -4567890123456789 as t1 from values ('true');

-- test datatype
create schema test;
set schema test;
create table t_int(int_col int not null primary key,
        int_col2 int);

-- negative test
insert into t_int values('true',null);
insert into t_int values(TRUE,null);
insert into t_int values(-1234.03,null);
insert into t_int values(10e400,null);
insert into t_int values(-1.2345678901234e-200,null);
insert into t_int values(x'ff',null);
insert into t_int values(b'10',null);
insert into t_int values(date '1999-01-08',null);
insert into t_int values(time '12:01:01',null);
insert into t_int values(timestamp '2004-12-01 12:01:01',null);

-- insert the right value
insert into t_int values(1234,null);
insert into t_int values(-4567890123456789,null);

-- null value test
--insert into t_int values(null, null); 

select * from t_int;

drop table t_int;

