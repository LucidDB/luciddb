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
values cast(timestamp '1994-07-08 17:05:08' as varchar(20));
values cast('1994-07-08 17:05:08' as timestamp);
values cast('1994-07-08' as date);
values cast('17:05:08' as time);

values cast(null as date);
values cast(null as time);
values cast(null as timestamp);

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

values cast('2004-12-21 12:01:01' as timestamp);
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
