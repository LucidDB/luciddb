set schema 's';

select KSEQ, K2, K4, K10K from BENCH1M where K2 = 2 and K10K = 3 and K4 = 1
order by KSEQ
;
select KSEQ, K2, K4, K25, K1K from BENCH1M where K2 = 2 and K4 = 1 and 
K1K = 10 and K25 = 7
order by KSEQ
;
select KSEQ, K2, K250K, K500K from BENCH1M WHERE
KSEQ = 2 and K2 = 1 and K250K = 183196 and K500K = 192382
order by KSEQ
;
select KSEQ, K2, K250K, K500K from BENCH1M WHERE
KSEQ = 8 and K2 = 1 and K4 = 4 and K5 = 2 and K100 = 17
order by KSEQ
;
select KSEQ, K250K, K500K from BENCH1M WHERE
KSEQ = 14 and K250K = 54997 and K500K = 430462
order by KSEQ
;
select KSEQ from BENCH1M where KSEQ > 999999
order by KSEQ
;
select KSEQ, K100K from BENCH1M where K100K between 95000 and 95110
order by KSEQ
;
select KSEQ, K100K from BENCH1M where K100K between 95200 and 95310
order by KSEQ
;
select KSEQ, K100K from BENCH1M where K100K between 95400 and 95510
order by KSEQ
;
select KSEQ, K100K from BENCH1M where K100K between 95400 and 95510
order by KSEQ
;
select KSEQ, K100K from BENCH1M where K100K between 95600 and 95710
order by KSEQ
;
select KSEQ, K10K from BENCH1M where K10K between 9500 and 9501
order by KSEQ
;
select KSEQ, K10K from BENCH1M where K10K between 9520 and 9521
order by KSEQ
;
select KSEQ, K10K from BENCH1M where K10K between 9540 and 9541
order by KSEQ
;
select KSEQ, K10K from BENCH1M where K10K between 9560 and 9561
order by KSEQ
;
select KSEQ, K10K from BENCH1M where K10K between 9580 and 9581
order by KSEQ
;
