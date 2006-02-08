-- HHJ Swing testing GROUP BY conditions

-- group by without agg
select a.k10
from bench1m b, bench1m a
where a.kseq = b.kseq and a.kseq < 10001
group by a.k10
order by a.k10;

-- group by same as join key
select max(a.kseq)
from bench1m b, bench1m a
where a.kseq = b.k10 and a.kseq < 10001
group by a.kseq
order by a.kseq;

-- group by subset of join key
select b.kseq, sum(b.kseq)
from bench1m a,bench1m b
where a.kseq = b.kseq and a.k10 = b.k100 and b.kseq < 1001 and a.kseq < 10001
group by b.kseq
order by b.kseq;

-- group by with functions on join keys
select b.kseq, sum(b.k100)
from bench1m a,bench1m b
where (a.kseq * 100) = b.kseq and b.kseq < 1001 and a.kseq < 10001
group by b.kseq
order by b.kseq;

-- group by superset of join key
select b.kseq, sum(b.k100)
from bench1m a,bench1m b
where a.kseq = b.kseq and b.kseq < 1001 and a.kseq < 10001
group by b.kseq, a.k10
order by b.kseq, a.k10;

-- end
