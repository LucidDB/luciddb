-- at this point, 23 blocks in ts should be used

set schema 's';

insert into t values (8, 8, 8, 8, 8, 8, 8)
;
select index_name,status,num_blocks_vi from indexes where table_name='T'
;
-- at this point, 30 blocks in ts should be used
insert into t values (7, 7, 7, 7, 7, 7, 7)
;
select index_name,status,num_blocks_vi from indexes where table_name='T'
;
-- at this point, should hit out of tablespace when insert data into index i3
-- the status of i3 should be unrecoverable, i4-i7 shouldbe recoverable-insert
insert into t values (6, 6, 6, 6, 6, 6, 6)
;
select index_name,status,num_blocks_vi from indexes where table_name='T'
;
