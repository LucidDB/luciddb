set schema 's';

-- Should work after the integration
SELECT KSEQ, K4 FROM BENCH10K WHERE K4 = 1 OR K4 = 2
order by 1;
