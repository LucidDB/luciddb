-- $Id$
-- test datetime comparisons.
set schema sales;

delete from datetime1;

insert into datetime1(col1,datecol,timecol,timestampcol) values(0, DATE '2004-12-21', TIME '12:22:33', TIMESTAMP '2004-12-21 12:22:33');

alter system set "calcVirtualMachine"  = 'CALCVM_JAVA';
select * from datetime1 where datecol > DATE '2003-12-21';
select * from datetime1 where datecol > DATE '2005-12-21';


select * from datetime1 where timecol > TIME '23:00:00';
select * from datetime1 where timecol < TIME '23:00:00';

select * from datetime1 where timestampcol  > TIMESTAMP '2004-12-21 23:00:00';
select * from datetime1 where timestampcol < TIMESTAMP  '2004-12-21 23:00:00';

-- fennel calc still crashes.
-- clear cache.
-- alter system set "codeCacheMaxBytes" = 0; 

-- alter system set "calcVirtualMachine"  = 'CALCVM_FENNEL';
-- select * from datetime1 where datecol > DATE '2003-12-21';



delete from datetime1;

