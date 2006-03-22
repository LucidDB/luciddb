set schema 's';

SELECT KSEQ from BENCH10K where K2 = 0 OR K2 = 3
order by 1;
