-- $Id$ 

-- test time literal
 select TIME '12:01:01' as T1 from values('TRUE');
 select TIME '00:00' as t1 from values ('true');
 select TIME '01:00' as t1 from values ('true');
 select TIME '02:03 PST' as t1 from values ('true');
 select TIME '11:59 EDT' as t1 from values ('true');
 select TIME '12:00' as t1 from values ('true');
 select TIME '12:01' as t1 from values ('true');
 select TIME '23:59' as t1 from values ('true');
 select TIME '11:59:59.99 PM' as t1 from values ('true');

-- more ISO format tests that should work
select time '3:4:5' from values ('true');
select time '0003:0005:0002' from values ('true');
-- with precision
select time '10:00:00.5' from values ('true');
select time '10:00:00.35' from values ('true');
select time '10:00:00.3523' from values ('true');

-- these should fail
select time '1003:1005:1002' from values ('true');
select time '23.54.43..' from values ('true');
select time '23.54.43.' from values ('true');
select time '23.54.43.1,000' from values ('true');

-- test datatype
create schema test;
set schema 'test';
create table t_time(time_col time(0) not null primary key, time_col2 time(0));

-- negative test
insert into t_time values('string value',null);
insert into t_time values(true,null);
insert into t_time values(1234,null);
insert into t_time values(1e400,null);
insert into t_time values(-1.2345678901234e-200,null);
insert into t_time values(-1234.03,null);
insert into t_time values(x'ff',null);
insert into t_date values(date '1999-01-08',null);
insert into t_time values(timestamp '2004-12-01 12:01:01',null);

-- insert the right values
insert into t_time values(time '12:01:01',null);

-- null value test
--insert into t_time values(null, null); 

select * from t_time;

drop table t_time;

