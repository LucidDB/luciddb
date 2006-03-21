set schema 's';

select KSEQ, K2, K4 from bench100 where K2 = 2 and K4 = 2
order by 1;

select KSEQ, K2, K4 from bench100 where K2 = 2 and K4 = 3
order by 1;

select KSEQ, K2 from bench100 where K2 = 2 and KSEQ = 3
order by 1;

select KSEQ, K2, K100K from bench100 where K2 = 2 and K100K = 3
order by 1;

select KSEQ, K2, K40K from bench100 where K2 = 2 and K40K = 3
order by 1;

select KSEQ, K2, K10K from bench100 where K2 = 2 and K10K = 3
order by 1;

select KSEQ, K2, K1K from bench100 where K2 = 2 and K1K = 3
order by 1;

select KSEQ, K2, K100 from bench100 where K2 = 2 and K100 = 3
order by 1;

select KSEQ, K2, K25 from bench100 where K2 = 2 and K25 = 3
order by 1;

select KSEQ, K2, K10 from bench100 where K2 = 2 and K10 = 3
order by 1;

select KSEQ, K2, K5 from bench100 where K2 = 2 and K5 = 3
order by 1;
