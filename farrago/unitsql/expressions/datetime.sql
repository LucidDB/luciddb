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
select cast(date '1994-07-08' as varchar(10)) from values('true');
select cast(time '17:05:08' as varchar(10)) from values('true');
select cast(timestamp '1994-07-08 17:05:08' as varchar(20)) from values('true');
select cast('1994-07-08 17:05:08' as timestamp) from values('true');
select cast('1994-07-08' as date) from values('true');
select cast('17:05:08' as time) from values('true');

select cast(null as date) from values(1);
select cast(null as time) from values(1);
select cast(null as timestamp) from values(1);

-- now test with Fennel calc, which supports casts
alter system set "calcVirtualMachine"  = 'CALCVM_FENNEL';

select cast(date '2004-12-21' as varchar(10)) from values('true');
select cast(time '12:01:01' as varchar(10)) from values('true');
select cast(date '2004-12-21' as char(10)) from values('true');
select cast(time '12:01:01' as char(10)) from values('true');

-- should fail due to truncation
select cast(timestamp '2004-12-21 12:01:01' as varchar(10)) from values('true');
select cast(timestamp '2004-12-21 12:01:01' as char(10)) from values('true');

-- should succeed
select cast(timestamp '2004-12-21 12:01:01' as varchar(20)) from values('true');
select cast(timestamp '2004-12-21 12:01:01' as char(20)) from values('true');

select cast('2004-12-21 12:01:01' as timestamp) from values('true');
select cast('2004-12-21' as date) from values('true');
select cast('12:01:01' as time) from values('true');

-- test casting from date/time to char and back
select cast( cast(timestamp '2004-12-21 12:01:01' as char(20)) 
            as timestamp) from values('true');
select cast( cast(date '2004-12-21' as char(20))
            as date) from values('true');
select cast( cast(time '12:01:01' as char(20))
            as time) from values('true');

select * from datetime1 where datecol > DATE '2003-12-21';
select * from datetime1 where datecol > DATE '2005-12-21';

select * from datetime1 where timecol > TIME '23:00:00';
select * from datetime1 where timecol < TIME '23:00:00';

select * from datetime1 where timestampcol > TIMESTAMP '2004-12-21 23:00:00';
select * from datetime1 where timestampcol < TIMESTAMP '2004-12-21 23:00:00';

select cast(null as date) from values(1);
select cast(null as time) from values(1);
select cast(null as timestamp) from values(1);
