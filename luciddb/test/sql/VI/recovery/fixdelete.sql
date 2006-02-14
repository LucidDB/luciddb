set schema 's';

alter table t recover indexes
;
select index_name,status,num_blocks_vi from indexes where table_name='T'
;
drop index i4
;
create index i4 on t(c4)
;
