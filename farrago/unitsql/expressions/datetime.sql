-- $Id$
-- test datetime comparisons.
alter system set "calcVirtualMachine"  = 'CALCVM_JAVA';

set schema sales;

delete from datetime1;

insert into datetime1(col1,datecol,timecol,timestampcol) values(0, DATE '2004-12-21', TIME '12:22:33', TIMESTAMP '2004-12-21 12:22:33');


select * from datetime1 where datecol > DATE '2003-12-21';
select * from datetime1 where datecol > DATE '2005-12-21';


select * from datetime1 where timecol > TIME '23:00:00';
select * from datetime1 where timecol < TIME '23:00:00';

select * from datetime1 where timestampcol  > TIMESTAMP '2004-12-21 23:00:00';
select * from datetime1 where timestampcol < TIMESTAMP  '2004-12-21 23:00:00';

-- fennel calc still crashes.
-- clear cache.
-- alter system set "codeCacheMaxBytes" = 0; 

alter system set "calcVirtualMachine"  = 'CALCVM_FENNEL';

select cast(date '2004-12-21' as varchar(10)) from values('true');
select cast(time '12:01:01' as varchar(10)) from values('true');
-- select cast(timestamp '2004-12-21 12:01:01' as varchar(10)) from values('true'); -- error not caught yet.
select cast(timestamp '2004-12-21 12:01:01' as varchar(20)) from values('true');

select cast('2004-12-21 12:01:01' as timestamp) from values('true');
select cast( '2004-12-21' as date) from values('true');
select cast('12:01:01' as time) from values('true');

-- select * from datetime1 where datecol > DATE '2003-12-21';



delete from datetime1;
alter system set "calcVirtualMachine"  = 'CALCVM_JAVA';
