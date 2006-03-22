set schema 's';

-- should work after integration
SELECT KSEQ, K4 FROM BENCH10K WHERE K4 = 1 OR K4 = 2 or K4 = 5
order by 1;
