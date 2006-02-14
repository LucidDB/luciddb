-- add datafile to ts
set schema 's';

alter tablespace ts add datafile 'ts1.dat' size 1m
;
alter table t recover indexes
;
select index_name,status,num_blocks_vi from indexes where table_name='T'
;
drop index i3
;
create index i3 on t(c3)
;
