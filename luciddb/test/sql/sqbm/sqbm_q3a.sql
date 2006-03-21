set schema 's';

select SUM(K1K) from bench1M
where
Kseq between 400000 and 500000
and K100K = 3
;
select SUM(K1K) from bench1M
where
Kseq between 400000 and 500000
and K10K = 3
;
select SUM(K1K) from bench1M
where
Kseq between 400000 and 500000
and K100 = 3
;
select SUM(K1K) from bench1M
where
Kseq between 400000 and 500000
and K25 = 3
;
select SUM(K1K) from bench1M
where
Kseq between 400000 and 500000
and K10 = 3
;
select SUM(K1K) from bench1M
where
Kseq between 400000 and 500000
and K5 = 3
;
select SUM(K1K) from bench1M
where
Kseq between 400000 and 500000
and K4 = 3
;
