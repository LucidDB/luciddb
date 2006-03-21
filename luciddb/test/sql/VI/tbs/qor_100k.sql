set schema 's';

select KSEQ, K100K from bench100K
where K2 = 2 OR K100K = 3
order by 1;

select KSEQ, K40K from bench100K
where K2 = 2 OR K40K = 3
order by 1;

select KSEQ, K10K from bench100K
where K2 = 2 OR K10K = 3
order by 1;

select KSEQ, K1K from bench100K
where K2 = 2 OR K1K = 3
order by 1;

select KSEQ, K100 from bench100K
where K2 = 2 OR K100 = 3
order by 1;

select KSEQ, K25 from bench100K
where K2 = 2 OR K25 = 3
order by 1;

select KSEQ, K10 from bench100K
where K2 = 2 OR K10 = 3
order by 1;

select KSEQ, K5 from bench100K
where K2 = 2 OR K5 = 3
order by 1;

select KSEQ, K4 from bench100K
where K2 = 2 OR K4 = 3
order by 1;
