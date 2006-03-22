-- calc18.sql   test real64 --> real32 conversions

set schema 's';

create table tab2(a real)
;
insert into tab2 values (0.01)
;
insert into tab2 values (100)
;
insert into tab2 values (300E-4)
;
insert into tab2 values (500E-4)
;
insert into tab2 values (0.05)
;
select a from tab2 order by a
;
drop table tab2
;
