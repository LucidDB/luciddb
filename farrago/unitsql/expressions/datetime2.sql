-- $Id$
-- test datetime comparisons.
-- at some point: alter system set "calcVirtualMachine"  = 'CALCVM_FENNEL';
set schema sales;
alter system set "codeCacheMaxBytes" = min; 

create table datetime2(
    col1 int not null primary key,
    datecol date not null,
    timecol time(0) not null ,
    timestampcol timestamp(0) not null);


insert into datetime2(col1,datecol,timecol,timestampcol) values(0, DATE '2004-12-21', TIME '12:22:33', TIMESTAMP '2004-12-21 12:22:33');


-- test ordinary comparisons:
select * from datetime2 where col1 > 0;

select * from datetime2 where datecol > DATE '2003-12-21';
select * from datetime2 where datecol > DATE '2005-12-21';


select * from datetime2 where timecol > TIME '23:00:00';
select * from datetime2 where timecol < TIME '23:00:00';

select * from datetime2 where timestampcol  > TIMESTAMP '2004-12-21 23:00:00';
select * from datetime2 where timestampcol < TIMESTAMP  '2004-12-21 23:00:00';

-- fennel calc still crashes.
-- clear cache.


 select * from datetime2 where datecol > DATE '2003-12-21';


select * from datetime2 where col1 > 0;

alter system set "codeCacheMaxBytes" = min; 
-- alter system set "calcVirtualMachine"  = 'CALCVM_FENNEL';
-- test ordinary comparisons:


select * from datetime2 where datecol > DATE '2003-12-21';

alter system set "calcVirtualMachine"  = 'CALCVM_JAVA';
alter system set "codeCacheMaxBytes" = max;
-- drop table datetime1;
drop table datetime2;

