-- create ts with 32 blocks
create schema s;
set schema 's';

create table t (
 c1 int,
 c2 int,
 c3 int,
 c4 int,
 c5 int,
 c6 int,
 c7 int)
;
create index i1 on t(c1)
;
create index i2 on t(c2)
;
create index i3 on t(c3)
;
create index i4 on t(c4)
;
create index i5 on t(c5)
;
create index i6 on t(c6)
;
create index i7 on t(c7)
;
