-- HHJ Swing testing special conditions

set schema 's';

-- order by 
select a.kseq, max(a.kseq) from bench1m a, bench1m b where a.kseq =
b.k10 and a.kseq < 10001 group by a.kseq order by a.kseq;

-- no matches in join
select min(a.kseq), sum(a.kseq), max(a.kseq) from bench1m a, bench1m b where a.kseq =
b.kseq and b.kseq > 900000 and a.kseq < 10001;


