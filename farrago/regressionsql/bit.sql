-- $Id$ 

-- test bid literal
 select B'10' as t1 from values ('true');
 select B'00000000000' as t1 from values ('true');
 select B'11011000000' as t1 from values ('true');
 select B'01010101010' as t1 from values ('true');
