-- set echo=off
set schema 's';

select  KSEQ,K2,K4,K5,K10,K25,K100,K1K,K10K,K40K,K100K,K250K,K500K  
FROM BENCH10K where
k2 between 1 and 2 and
k4 between 1 and 4 and
k5 between 1 and 5 and
k10 between 1 and 10 and
k25 between 1 and 25 and
k100 between 1 and 100 and
k1K between 1 and 1000 and
k10K between 1 and 10000 and
k40K between 1 and 40000 and
k100K between 1 and 100000 and
k250K between 1 and 250000 and
k500K between 1 and 500000 and
kSEQ between 1 and 10000
order by KSEQ
;
