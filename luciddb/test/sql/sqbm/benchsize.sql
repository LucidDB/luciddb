-- Detect any changes due to space usages of  VI, RI 
-- set echo=ON
select column_name, num_blocks_ri from stats where table_name = 'BENCH1M';

select sum(num_blocks_ri) from stats where table_name = 'BENCH1M';

select table_name, index_type, index_name, num_columns, 
num_blocks_vi from indexes where table_name='BENCH1M'
;

select sum(num_blocks_vi) from indexes where table_name = 'BENCH1M'
;
