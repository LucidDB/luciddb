!set headerinterval 1000

set schema 'analyzetest';

-- Note: BERNOULLI sampling requires the use of the REPEATABLE clause
-- to produce repeatable results across invocations.  As of its initial
-- implementation, LCS SYSTEM sampling does not require the REPEATABLE clause.

-- Note: The results of these queries depend on the order in which rows 
-- are stored in the table.  If the insertion order becomes non-deterministic 
-- (perhaps due to some performance improvement), these tests will fail .

-- Verify that sampling is handled via LCS-specific XOs

!outputformat csv

explain plan for select * from bench10k tablesample bernoulli(0.1);

explain plan for select * from bench10k tablesample system(0.1);

-- Validate that only k2 is projected, despite count(*)
explain plan for select "k2", count(*)
from bench1m tablesample bernoulli(1)
group by "k2"
order by "k2";

-- Validate that only k2 is projected, despite count(*)
explain plan for select "k2", count(*)
from bench1m tablesample system(1)
group by "k2"
order by "k2";

!outputformat table

select "k2" from bench10k tablesample bernoulli(0.1) repeatable(19571004);

select "k2" from bench10k tablesample system(0.1);

select "k1k" from bench10k tablesample bernoulli(0.1) repeatable(19580131);

select "k1k" from bench10k tablesample system(0.1);

select "k2", count(*)
from bench1m tablesample bernoulli(1) repeatable(19690716)
group by "k2"
order by "k2";

select "k2", count(*)
from bench1m tablesample system(1)
group by "k2"
order by "k2";

select "k100", count(*) 
from bench1m tablesample bernoulli(1) repeatable(19721219)
group by "k100"
order by "k100";

select "k100", count(*) 
from bench1m tablesample system(1)
group by "k100"
order by "k100";

select 
  s."k2",
  s."sample_k2_cnt" * 100 as "est_k2_cnt",
  f."full_k2_cnt" as "true_k2_cnt",
  ((s."sample_k2_cnt" * 100) - f."full_k2_cnt") / f."full_k2_cnt" as "err"
from
  (select "k2", count(*) as "full_k2_cnt"
   from bench1m
   group by "k2") as f,
  (select "k2", count(*) as "sample_k2_cnt"
   from bench1m tablesample bernoulli(1) repeatable(19810412)
   group by "k2") as s
where s."k2" = f."k2"
order by s."k2";

select 
  s."k2",
  s."sample_k2_cnt" * 100 as "est_k2_cnt",
  f."full_k2_cnt" as "true_k2_cnt",
  ((s."sample_k2_cnt" * 100) - f."full_k2_cnt") / f."full_k2_cnt" as "k2_err"
from
  (select "k2", count(*) as "full_k2_cnt"
   from bench1m
   group by "k2") as f,
  (select "k2", count(*) as "sample_k2_cnt"
   from bench1m tablesample system(1)
   group by "k2") as s
where s."k2" = f."k2"
order by s."k2";


-------------------------------------------------------------------------------
-- Test tables with deleted rows
-------------------------------------------------------------------------------

-- 10 rows, each separated by 110 rows
select "kseq" from bench1k tablesample system(1);

-- ~10 rows
select "kseq" from bench1k tablesample bernoulli(1) repeatable(1);

-- reduce bench1k to 100 rows
delete from bench1k where "kseq" > 50 and "kseq" < 951;
select count(*) from bench1k;

-- 10 rows, each separated by 11 rows
select "kseq" from bench1k tablesample system(10);

-- ~10 rows
select "kseq" from bench1k tablesample bernoulli(10) repeatable(1);

-- reduce bench1k to fewer than 10 rows
delete from bench1k where "kseq" > 5;
select count(*) from bench1k;

-- fewer rows than clumps, should return all rows
select "kseq" from bench1k tablesample system(50);

-- ~2-3 rows
select "kseq" from bench1k tablesample bernoulli(50) repeatable(2);

-- reduce bench1k to 0 rows
delete from bench1k;

-- only deleted rows, no output
select "kseq" from bench1k tablesample system(50);

select "kseq" from bench1k tablesample bernoulli(50) repeatable(3);

-- remove deleted rows
truncate table bench1k;
alter table bench1k rebuild;

-- no rows, no output
select "kseq" from bench1k tablesample system(50);

select "kseq" from bench1k tablesample bernoulli(50) repeatable(5);

-------------------------------------------------------------------------------
-- Manipulate catalog row count to simulate concurrent DML/TABLESAMPLE.
-- Only affects TABLESAMPLE SYSTEM, so ignore BERNOULLI.
-------------------------------------------------------------------------------

-- Baseline
select count(*) from concurrent_sim tablesample system(50);

-- Report zero rows where there are 100
-- Sampling returns all rows (rowcount == 0 => clumpSize = 1)
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'ANALYZETEST', 'CONCURRENT_SIM', 0);
select count(*) from concurrent_sim tablesample system(50);

-- Report 5 rows where there are 100
-- Sampling returns all rows (fewer rows than clumps)
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'ANALYZETEST', 'CONCURRENT_SIM', 5);

select count(*) from concurrent_sim tablesample system(50);

-- Report 50 rows where there are 100
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'ANALYZETEST', 'CONCURRENT_SIM', 50);
select count(*) from concurrent_sim tablesample system(50);

-- Report 200 rows where there are 100
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'ANALYZETEST', 'CONCURRENT_SIM', 200);
select count(*) from concurrent_sim tablesample system(10);

-- reset rowcount = 100
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'ANALYZETEST', 'CONCURRENT_SIM', 100);

-- Test with deleted rows -----------------------------------------------------

-- Delete all but 4 rows
delete from concurrent_sim where "kseq" > 4;

-- Report 8 rows when there are 4
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'ANALYZETEST', 'CONCURRENT_SIM', 8);
select count(*) from concurrent_sim tablesample system(50);

-- Report zero rows when there are 4
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'ANALYZETEST', 'CONCURRENT_SIM', 0);
select count(*) from concurrent_sim tablesample system(50);

-- Report 100 rows when there are 4
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'ANALYZETEST', 'CONCURRENT_SIM', 100);
select count(*) from concurrent_sim tablesample system(10);


-- Test with small, rebuilt table ---------------------------------------------

alter table concurrent_sim rebuild;

-- Report 8 rows when there are 4
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'ANALYZETEST', 'CONCURRENT_SIM', 8);
select count(*) from concurrent_sim tablesample system(50);

-- Report zero rows when there are 4
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'ANALYZETEST', 'CONCURRENT_SIM', 0);
select count(*) from concurrent_sim tablesample system(50);

-- Report 100 rows when there are 4
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'ANALYZETEST', 'CONCURRENT_SIM', 100);
select count(*) from concurrent_sim tablesample system(10);


-- Test empty table with deleted rows -----------------------------------------

-- Delete remaining rows in table
delete from concurrent_sim;

-- Report 100 rows when there are 0
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'ANALYZETEST', 'CONCURRENT_SIM', 100);
select count(*) from concurrent_sim tablesample system(10);

-- Report 5 rows when there are 0
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'ANALYZETEST', 'CONCURRENT_SIM', 5);
select count(*) from concurrent_sim tablesample system(10);

-- Test rebuilt empty table ---------------------------------------------------

alter table concurrent_sim rebuild;

-- Report 100 rows when there are 0
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'ANALYZETEST', 'CONCURRENT_SIM', 100);
select count(*) from concurrent_sim tablesample system(10);

-- Report 5 rows when there are 0
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'ANALYZETEST', 'CONCURRENT_SIM', 5);
select count(*) from concurrent_sim tablesample system(10);

-- Reset row count
call sys_boot.mgmt.stat_set_row_count('LOCALDB', 'ANALYZETEST', 'CONCURRENT_SIM', 0);


-- Drop tables destroyed by this test
drop table concurrent_sim;
drop table bench1k;

