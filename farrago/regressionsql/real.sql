-- $Id$ 

-- test real literal
 select 0.0 as t1 from values ('true');
 select 1. as t1 from values ('true');
 select .1 as t1 from values ('true');
 select 1004.30 as t1 from values ('true');
 select -34.84 as t1 from values ('true');
-- select 1.2345678901234e+20 as t1 from values ('true');
 select 1.2345678901234e-20 as t1 from values ('true');
-- select 10e40 as t1 from values ('true');
-- select -10e40 as t1 from values ('true');
 select 10e-40 as t1 from values ('true');
 select -10e-40 as t1 from values ('true');
 select 0.0 as t1 from values ('true');
 select 1004.30 as t1 from values ('true');
 select -34.84 as t1 from values ('true');
-- select 1.2345678901234e+200 as t1 from values ('true');
 select 1.2345678901234e-200 as t1 from values ('true');
-- select 10e400 as t1 from values ('true');
-- select -10e400 as t1 from values ('true');
-- select 10e-400 as t1 from values ('true');
-- select -10e-400 as t1 from values ('true');
 select -34.84 as t1 from values ('true');
 select -1004.30 as t1 from values ('true');
-- select -1.2345678901234e+200 as t1 from values ('true');
 select -1.2345678901234e-200 as t1 from values ('true');
