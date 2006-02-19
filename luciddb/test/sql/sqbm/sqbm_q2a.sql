-- SET ECHO=ON
select count(*) from BENCH1M where K2 = 2 and KSEQ = 3
;
select count(*) from BENCH1M where K2 = 2 and K100K = 3
;
select count(*) from BENCH1M where K2 = 2 and K10K = 3
;
select count(*) from BENCH1M where K2 = 2 and K1K = 3
;
select count(*) from BENCH1M where K2 = 2 and K100 = 3
;
select count(*) from BENCH1M where K2 = 2 and K25 = 3
;
select count(*) from BENCH1M where K2 = 2 and K10 = 3
;
select count(*) from BENCH1M where K2 = 2 and K5 = 3
;
select count(*) from BENCH1M where K2 = 2 and K4 = 3
;
