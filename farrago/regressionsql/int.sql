-- $Id$ 

-- test int literal

 select 0 as t1 from values ('true');
 select 1234 as t1 from values ('true');
 select -1234 as t1 from values ('true');
 select 34.5 as t1 from values ('true');
 select 32767 as t1 from values ('true');
 select -32767 as t1 from values ('true');
 select 100000 as t1 from values ('true');


 select 123456 as t1 from values ('true');
 select -123456 as t1 from values ('true');

 select 2147483647 as t1 from values ('true');
 select -2147483647 as t1 from values ('true');
 select 1000000000000 as t1 from values ('true');



select 4567890123456789 as t1 from values ('true');
select -4567890123456789 as t1 from values ('true');
