-- $Id$ 

-- test timestamp literal
-- we don'tsupport time w/ timezone in this release.
-- it happens to parse, but the results are potentially wrong.
values TIMESTAMP '2004-12-01 12:01:01';
--values timestamp 'invalid';
--values timestamp 'Mon Feb 10 17:32:01 1997 PST';
--values timestamp 'Invalid Abstime';
--values timestamp 'Undefined Abstime';
--values timestamp 'Mon Feb 10 17:32:01.000001 1997 PST';
--values timestamp 'Mon Feb 10 17:32:01.999999 1997 PST';
--values timestamp 'Mon Feb 10 17:32:01.4 1997 PST';
--values timestamp 'Mon Feb 10 17:32:01.5 1997 PST';
--values timestamp 'Mon Feb 10 17:32:01.6 1997 PST';
--values timestamp '1997-01-02';
 values timestamp '1997-01-02 03:04:05';
--  values timestamp '1997-02-10 17:32:01-08';
--  values timestamp '1997-02-10 17:32:01-0800';
--  values timestamp '1997-02-10 17:32:01 -08:00';
--values timestamp '19970210 173201 -0800';
--values timestamp '1997-06-10 17:32:01 -07:00';
--values timestamp '2001-09-22T18:19:20';
-- values timestamp '2000-03-15 08:14:01 GMT+8';
--  values timestamp '2000-03-15 13:14:02 GMT-1';
-- values timestamp '2000-03-15 12:14:03 GMT -2';
-- values timestamp '2000-03-15 03:14:04 EST+3';
-- values timestamp '2000-03-15 02:14:05 EST +2:00';
--values timestamp 'Feb 10 17:32:01 1997 -0800';
--values timestamp 'Feb 10 17:32:01 1997';
--values timestamp 'Feb 10 5:32PM 1997';
--values timestamp '1997/02/10 17:32:01-0800';
-- values timestamp '1997-02-10 17:32:01 PST';
--values timestamp 'Feb-10-1997 17:32:01 PST';
-- values timestamp '02-10-1997 17:32:01 PST';
--values timestamp '19970210 173201 PST';
--values timestamp '97FEB10 5:32:01PM UTC';
--values timestamp '97/02/10 17:32:01 UTC';
--values timestamp '1997.041 17:32:01 UTC';
-- values timestamp '1997-06-10 18:32:01 PDT';
--values timestamp 'Feb 10 17:32:01 1997';
--values timestamp 'Feb 11 17:32:01 1997';
--values timestamp 'Feb 12 17:32:01 1997';
--values timestamp 'Feb 13 17:32:01 1997';
--values timestamp 'Feb 14 17:32:01 1997';
--values timestamp 'Feb 15 17:32:01 1997';
--values timestamp 'Feb 16 17:32:01 1997';
--values timestamp 'Feb 16 17:32:01 0097 BC';
--values timestamp 'Feb 16 17:32:01 0097';
--values timestamp 'Feb 16 17:32:01 0597';
--values timestamp 'Feb 16 17:32:01 1097';
--values timestamp 'Feb 16 17:32:01 1697';
--values timestamp 'Feb 16 17:32:01 1797';
--values timestamp 'Feb 16 17:32:01 1897';
--values timestamp 'Feb 16 17:32:01 1997';
--values timestamp 'Feb 16 17:32:01 2097';
--values timestamp 'Feb 28 17:32:01 1996';
--values timestamp 'Feb 29 17:32:01 1996';
--values timestamp 'Mar 01 17:32:01 1996';
--values timestamp 'Dec 30 17:32:01 1996';
--values timestamp 'Dec 31 17:32:01 1996';
--values timestamp 'Jan 01 17:32:01 1997';
--values timestamp 'Feb 28 17:32:01 1997';
--values timestamp 'Feb 29 17:32:01 1997';
--values timestamp 'Mar 01 17:32:01 1997';
--values timestamp 'Dec 30 17:32:01 1997';
--values timestamp 'Dec 31 17:32:01 1997';
--values timestamp 'Dec 31 17:32:01 1999';
--values timestamp 'Jan 01 17:32:01 2000';
--values timestamp 'Dec 31 17:32:01 2000';
--values timestamp 'Jan 01 17:32:01 2001';
--values timestamp 'Feb 16 17:32:01 -0097';
--values timestamp 'Feb 16 17:32:01 5097 BC';

-- more ISO format tests that should work
values timestamp '1930-003-005 8:4:9';
values timestamp '2005-12-6 0006:0008:0022';
-- with precision
values timestamp '1966-03-4 12:00:45.009';
values timestamp '1945-4-3 0001:00:01.1000';
values timestamp '2003-12-31 0004:00:34.999';

-- these should fail
values timestamp '2004-13-4 05:06:07';
values timestamp '2004-04-6 05:06:67';
values timestamp '2004-10-8 23.54.43.';

-- test datatype
create schema test;
set schema 'test';
create table t_timestamp(timestamp_col timestamp(0) not null primary key, timestamp_col2 timestamp(0));

-- negative test
insert into t_timestamp values('string value',null);
insert into t_timestamp values(true,null);
insert into t_timestamp values(1234,null);
insert into t_timestamp values(1e400,null);
insert into t_timestamp values(-1.2345678901234e-200,null);
insert into t_timestamp values(-1234.03,null);
insert into t_timestamp values(x'ff',null);
insert into t_timestamp values(time '12:01:01',null);
insert into t_timestamp values(date '1999-01-08',null);

-- insert the right values
insert into t_timestamp values(TIMESTAMP '2004-12-01 12:01:01',null);

-- null value test
--insert into t_timestamp values(null, null); 

select * from t_timestamp;

drop table t_timestamp;

