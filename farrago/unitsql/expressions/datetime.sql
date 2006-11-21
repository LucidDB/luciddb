-- $Id$
-- test datetime comparisons.

create schema tempo;
set schema 'tempo';

create table datetime1(
    col1 int not null primary key,
    datecol date,
    timecol time(0),
    timestampcol timestamp(0));

-- first test with Java calc
alter system set "calcVirtualMachine"  = 'CALCVM_JAVA';

insert into datetime1(col1,datecol,timecol,timestampcol) 
values(0, DATE '2004-12-21', TIME '12:22:33', TIMESTAMP '2004-12-21 12:22:33');

select * from datetime1 where datecol > DATE '2003-12-21';
select * from datetime1 where datecol > DATE '2005-12-21';


select * from datetime1 where timecol > TIME '23:00:00';
select * from datetime1 where timecol < TIME '23:00:00';

select * from datetime1 where timestampcol > TIMESTAMP '2004-12-21 23:00:00';
select * from datetime1 where timestampcol < TIMESTAMP '2004-12-21 23:00:00';

-- simple casting (not fully supported in java calc)
values cast(date '1994-07-08' as varchar(10));
values cast(time '17:05:08' as varchar(10));
// NOTE: this shows precision with Jdbc formatting
values cast(timestamp '1994-07-08 17:05:08' as varchar(20));
values cast('1994-07-08 17:05:08' as timestamp);
values cast('1994-07-08' as date);
values cast('17:05:08' as time);

values cast(null as date);
values cast(null as time);
values cast(null as timestamp);

-- nullability testing (FRG-237)
create table initial_null(i int primary key, d date);
insert into initial_null values
  (1, null), (2, DATE'1987-2-5'), (3,DATE'1998-12-3');
select * from initial_null;
select extract(day from ((current_date - d)day)) from initial_null;

values cast( cast(null as time) as timestamp);

-- now test with Fennel calc, which supports casts
alter system set "calcVirtualMachine"  = 'CALCVM_FENNEL';

values cast(date '2004-12-21' as varchar(10));
values cast(time '12:01:01' as varchar(10));
values cast(date '2004-12-21' as char(10));
values cast(time '12:01:01' as char(10));

-- should fail due to truncation
values cast(timestamp '2004-12-21 12:01:01' as varchar(10));
values cast(timestamp '2004-12-21 12:01:01' as char(10));

-- should succeed

values cast(timestamp '2004-12-21 12:01:01' as varchar(20));
values cast(timestamp '2004-12-21 12:01:01' as char(20));

-- values cast('2004-12-21 12:01:01' as timestamp);

values cast('2004-12-21' as date);
values cast('12:01:01' as time);

-- test casting from date/time to char and back
values cast( cast(timestamp '2004-12-21 12:01:01' as char(20)) 
            as timestamp);
values cast( cast(date '2004-12-21' as char(20))
            as date);
values cast( cast(time '12:01:01' as char(20))
            as time);

select * from datetime1 where datecol > DATE '2003-12-21';
select * from datetime1 where datecol > DATE '2005-12-21';

select * from datetime1 where timecol > TIME '23:00:00';
select * from datetime1 where timecol < TIME '23:00:00';

select * from datetime1 where timestampcol > TIMESTAMP '2004-12-21 23:00:00';
select * from datetime1 where timestampcol < TIMESTAMP '2004-12-21 23:00:00';

values cast(null as date);
values cast(null as time);
values cast(null as timestamp);

-- a few more Java Calc tests involving insertions
alter system set "calcVirtualMachine"  = 'CALCVM_JAVA';

create table d (
  d date primary key);
insert into d values (
  date'2006-09-27');

-- should fail
insert into d values (
  time'19:31:00');
insert into d values (
  timestamp'2006-09-27 20:31:00');
insert into d values (
  cast(timestamp'2006-09-27 20:31:00' as date));

select * from d;

truncate table d;

create table t (
  t time primary key);
insert into t values (
  time'19:31:00');

-- should fail
insert into t values (
  date'2006-09-27');
insert into t values (
  timestamp'2006-09-27 19:31:00');
insert into t values (
  cast(timestamp'2006-09-27 19:31:00' as time));

select * from t;

create table ts(
  ts timestamp primary key);
insert into ts values (
  timestamp'2006-09-27 19:31:00');
insert into ts values (
  cast(date'2006-09-27' as timestamp));

-- should fail
insert into ts values (
  date'2006-09-27');
insert into ts values (
  time'19:31:00');
-- Note: duplicate key value changes with time
-- insert into ts values 
--   (current_timestamp),
--   (cast (current_time as timestamp));

select * from ts order by 1;

-- boundary cases
values cast (timestamp'2006-09-27 00:00:00' as date);
values cast (timestamp'2006-09-27 23:59:59' as date);

values cast (timestamp'2006-09-27 00:00:00' as time);
values cast (timestamp'2006-09-27 23:59:59' as time);

-- test comparison

-- should set date to current_date
values cast(
  cast(time'19:31:00' as timestamp)
  as date) = current_date;

-- ensure time does not keep unecessary components
values cast (timestamp'2006-09-27 23:59:59' as time) = time'23:59:59';
