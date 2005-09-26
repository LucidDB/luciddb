-- $Id$ 

-- test time literal
values TIME '12:01:01';
values TIME '00:00';
values TIME '01:00';
values TIME '02:03 PST';
values TIME '11:59 EDT';
values TIME '12:00';
values TIME '12:01';
values TIME '23:59';
values TIME '11:59:59.99 PM';

-- more ISO format tests that should work
values time '3:4:5';
values time '0003:0005:0002';
-- with precision
values time '10:00:00.5';
values time '10:00:00.35';
values time '10:00:00.3523';

-- these should fail
values time '1003:1005:1002';
values time '23.54.43..';
values time '23.54.43.';
values time '23.54.43.1,000';

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
-- the following line should be allowed.
-- insert into t_time values(timestamp '2004-12-01 12:01:01',null);

-- insert the right values
insert into t_time values(time '12:01:01',null);

-- null value test
--insert into t_time values(null, null); 

select * from t_time;

drop table t_time;

drop schema test;
