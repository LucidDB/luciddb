set schema 's';

select KSEQ, K2, K4 from BENCH10K 
where K2 = 2 and K4 = 2 and K5 = 2 and K10 = 2
order by 1;

SELECT KSEQ FROM BENCH10K 
WHERE (k2 = 2 or k4 = 2 or k5 = 2) and k100 = 2
order by 1;

SELECT KSEQ, K2, K4 FROM BENCH10K WHERE K2 = 2 AND K4 = 2
order by 1;

SELECT KSEQ, K2, K5 FROM BENCH10K WHERE K2 = 2 AND K5 = 2
order by 1;
