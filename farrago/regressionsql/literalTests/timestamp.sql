-- $Id$ 

-- test timestamp literal
-- we don'tsupport time w/ timezone in this release.
-- it happens to parse, but the results are potentially wrong.
select TIMESTAMP '2004-12-01 12:01:01' as t1 from values('TRUE');
--select timestamp 'invalid' as t1 from values ('true');
--select timestamp 'Mon Feb 10 17:32:01 1997 PST' as t1 from values ('true');
--select timestamp 'Invalid Abstime' as t1 from values ('true');
--select timestamp 'Undefined Abstime' as t1 from values ('true');
--select timestamp 'Mon Feb 10 17:32:01.000001 1997 PST' as t1 from values ('true');
--select timestamp 'Mon Feb 10 17:32:01.999999 1997 PST' as t1 from values ('true');
--select timestamp 'Mon Feb 10 17:32:01.4 1997 PST' as t1 from values ('true');
--select timestamp 'Mon Feb 10 17:32:01.5 1997 PST' as t1 from values ('true');
--select timestamp 'Mon Feb 10 17:32:01.6 1997 PST' as t1 from values ('true');
--select timestamp '1997-01-02' as t1 from values ('true');
 select timestamp '1997-01-02 03:04:05' as t1 from values ('true');
--  select timestamp '1997-02-10 17:32:01-08' as t1 from values ('true');
--  select timestamp '1997-02-10 17:32:01-0800' as t1 from values ('true');
--  select timestamp '1997-02-10 17:32:01 -08:00' as t1 from values ('true');
--select timestamp '19970210 173201 -0800' as t1 from values ('true');
--select timestamp '1997-06-10 17:32:01 -07:00' as t1 from values ('true');
--select timestamp '2001-09-22T18:19:20' as t1 from values ('true');
-- select timestamp '2000-03-15 08:14:01 GMT+8' as t1 from values ('true');
--  select timestamp '2000-03-15 13:14:02 GMT-1' as t1 from values ('true');
-- select timestamp '2000-03-15 12:14:03 GMT -2' as t1 from values ('true');
-- select timestamp '2000-03-15 03:14:04 EST+3' as t1 from values ('true');
-- select timestamp '2000-03-15 02:14:05 EST +2:00' as t1 from values ('true');
--select timestamp 'Feb 10 17:32:01 1997 -0800' as t1 from values ('true');
--select timestamp 'Feb 10 17:32:01 1997' as t1 from values ('true');
--select timestamp 'Feb 10 5:32PM 1997' as t1 from values ('true');
--select timestamp '1997/02/10 17:32:01-0800' as t1 from values ('true');
-- select timestamp '1997-02-10 17:32:01 PST' as t1 from values ('true');
--select timestamp 'Feb-10-1997 17:32:01 PST' as t1 from values ('true');
-- select timestamp '02-10-1997 17:32:01 PST' as t1 from values ('true');
--select timestamp '19970210 173201 PST' as t1 from values ('true');
--select timestamp '97FEB10 5:32:01PM UTC' as t1 from values ('true');
--select timestamp '97/02/10 17:32:01 UTC' as t1 from values ('true');
--select timestamp '1997.041 17:32:01 UTC' as t1 from values ('true');
-- select timestamp '1997-06-10 18:32:01 PDT' as t1 from values ('true');
--select timestamp 'Feb 10 17:32:01 1997' as t1 from values ('true');
--select timestamp 'Feb 11 17:32:01 1997' as t1 from values ('true');
--select timestamp 'Feb 12 17:32:01 1997' as t1 from values ('true');
--select timestamp 'Feb 13 17:32:01 1997' as t1 from values ('true');
--select timestamp 'Feb 14 17:32:01 1997' as t1 from values ('true');
--select timestamp 'Feb 15 17:32:01 1997' as t1 from values ('true');
--select timestamp 'Feb 16 17:32:01 1997' as t1 from values ('true');
--select timestamp 'Feb 16 17:32:01 0097 BC' as t1 from values ('true');
--select timestamp 'Feb 16 17:32:01 0097' as t1 from values ('true');
--select timestamp 'Feb 16 17:32:01 0597' as t1 from values ('true');
--select timestamp 'Feb 16 17:32:01 1097' as t1 from values ('true');
--select timestamp 'Feb 16 17:32:01 1697' as t1 from values ('true');
--select timestamp 'Feb 16 17:32:01 1797' as t1 from values ('true');
--select timestamp 'Feb 16 17:32:01 1897' as t1 from values ('true');
--select timestamp 'Feb 16 17:32:01 1997' as t1 from values ('true');
--select timestamp 'Feb 16 17:32:01 2097' as t1 from values ('true');
--select timestamp 'Feb 28 17:32:01 1996' as t1 from values ('true');
--select timestamp 'Feb 29 17:32:01 1996' as t1 from values ('true');
--select timestamp 'Mar 01 17:32:01 1996' as t1 from values ('true');
--select timestamp 'Dec 30 17:32:01 1996' as t1 from values ('true');
--select timestamp 'Dec 31 17:32:01 1996' as t1 from values ('true');
--select timestamp 'Jan 01 17:32:01 1997' as t1 from values ('true');
--select timestamp 'Feb 28 17:32:01 1997' as t1 from values ('true');
--select timestamp 'Feb 29 17:32:01 1997' as t1 from values ('true');
--select timestamp 'Mar 01 17:32:01 1997' as t1 from values ('true');
--select timestamp 'Dec 30 17:32:01 1997' as t1 from values ('true');
--select timestamp 'Dec 31 17:32:01 1997' as t1 from values ('true');
--select timestamp 'Dec 31 17:32:01 1999' as t1 from values ('true');
--select timestamp 'Jan 01 17:32:01 2000' as t1 from values ('true');
--select timestamp 'Dec 31 17:32:01 2000' as t1 from values ('true');
--select timestamp 'Jan 01 17:32:01 2001' as t1 from values ('true');
--select timestamp 'Feb 16 17:32:01 -0097' as t1 from values ('true');
--select timestamp 'Feb 16 17:32:01 5097 BC' as t1 from values ('true');

-- more ISO format tests that should work
select timestamp '1930-003-005 8:4:9' from values ('true');
select timestamp '2005-12-6 0006:0008:0022' from values ('true');
-- with precision
select timestamp '1966-03-4 12:00:45.009' from values ('true');
select timestamp '1945-4-3 0001:00:01.1000' from values ('true');
select timestamp '2003-12-31 0004:00:34.999' from values ('true');

-- these should fail
select timestamp '2004-13-4 05:06:07' from values ('true');
select timestamp '2004-04-6 05:06:67' from values ('true');
select timestamp '2004-10-8 23.54.43.' from values ('true');

-- test datatype
create schema test;
set schema test;
create table t_timestamp(timestamp_col timestamp(0) not null primary key, timestamp_col2 timestamp(0));

-- negative test
insert into t_timestamp values('string value',null);
insert into t_timestamp values(true,null);
insert into t_timestamp values(1234,null);
insert into t_timestamp values(10e400,null);
insert into t_timestamp values(-1.2345678901234e-200,null);
insert into t_timestamp values(-1234.03,null);
insert into t_timestamp values(x'ff',null);
insert into t_timestamp values(b'10',null);
insert into t_timestamp values(time '12:01:01',null);
insert into t_timestamp values(date '1999-01-08',null);

-- insert the right values
insert into t_timestamp values(TIMESTAMP '2004-12-01 12:01:01',null);

-- null value test
--insert into t_timestamp values(null, null); 

select * from t_timestamp;

drop table t_timestamp;

