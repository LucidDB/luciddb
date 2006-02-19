-- set echo=off
set schema 's';

select  KSEQ,K2,K4,K5,K10,K25,K100,K1K,K10K,K40K,K100K,K250K,K500K  
FROM BENCH10K where
k100 between 1 and 100
order by KSEQ
;
