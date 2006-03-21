set schema 's';

!set incremental on

select  KSEQ,K2,K4,K5,K10,K25,K100,K1K,K10K,K40K,K100K,K250K,K500K  
FROM BENCH100K
order by 1;

!set incremental off