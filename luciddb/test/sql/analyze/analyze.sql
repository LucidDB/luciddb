!set headerinterval 0

set schema 'analyzetest';

-- System sampling should cause the same results across runs.  If row insertion
-- order changes, this test may produce different results.  If row insertion
-- order becomes non-deterministic, this test will become non-deterministic.

analyze table bench10k estimate statistics for all columns;
analyze table bench1m estimate statistics for all columns;
analyze table index_est estimate statistics for all columns;

select
  table_name,
  column_name,
  distinct_value_count,
  is_distinct_value_count_estimated,
  percent_sampled,
  sample_size
from sys_root.dba_column_stats
where schema_name = 'ANALYZETEST'
order by table_name, column_name;

select table_name, column_name, ordinal, start_value, value_count
from sys_root.dba_column_histograms
where schema_name = 'ANALYZETEST' 
  and table_name = 'BENCH1M' 
  and column_name = 'kseq';

select table_name, column_name, ordinal, start_value, value_count
from sys_root.dba_column_histograms
where schema_name = 'ANALYZETEST' 
  and table_name = 'BENCH1M' 
  and column_name = 'k2';

select table_name, column_name, ordinal, start_value, value_count
from sys_root.dba_column_histograms
where schema_name = 'ANALYZETEST' 
  and table_name = 'BENCH1M' 
  and column_name = 'k1k';

select table_name, column_name, ordinal, start_value, value_count
from sys_root.dba_column_histograms
where schema_name = 'ANALYZETEST' 
  and table_name = 'BENCH10K' 
  and column_name = 'kseq';

select table_name, column_name, ordinal, start_value, value_count
from sys_root.dba_column_histograms
where schema_name = 'ANALYZETEST' 
  and table_name = 'BENCH10K' 
  and column_name = 'k2';

select table_name, column_name, ordinal, start_value, value_count
from sys_root.dba_column_histograms
where schema_name = 'ANALYZETEST' 
  and table_name = 'BENCH10K' 
  and column_name = 'k1k';

-- delete rows from the index_est table and recompute the stats
delete from index_est where mod("kseq", 3) = 0;

-- 
analyze table index_est estimate statistics for all columns sample 25 percent;

select
  table_name,
  column_name,
  distinct_value_count,
  is_distinct_value_count_estimated,
  percent_sampled,
  sample_size
from sys_root.dba_column_stats
where schema_name = 'ANALYZETEST' and table_name = 'INDEX_EST'
order by column_name;
