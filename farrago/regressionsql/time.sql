-- $Id$ 

-- test time literal
 select TIME '12:01:01' as T1 from values('TRUE');
 select TIME '00:00' as t1 from values ('true');
 select TIME '01:00' as t1 from values ('true');
 select TIME '02:03 PST' as t1 from values ('true');
 select TIME '11:59 EDT' as t1 from values ('true');
 select TIME '12:00' as t1 from values ('true');
 select TIME '12:01' as t1 from values ('true');
 select TIME '23:59' as t1 from values ('true');
 select TIME '11:59:59.99 PM' as t1 from values ('true');
