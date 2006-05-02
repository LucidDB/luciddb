-- general HHJ swing tests for coverage

set schema 's';

-- AGG on one side, join big on small table
select max(b.k500k) 
from bench1m a, bench1m b 
where a.kseq = b.k10 and b.kseq < 10001 
group by a.kseq
order by a.kseq
;

-- select AGG from both sides, and key fields, join big on small
select a.k10, b.kseq, count(b.k1k), sum(b.k1k), avg(b.k1k), max(a.k10)
from bench1m a, bench1m b
where a.k10 = b.kseq
group by b.kseq, a.k10
order by b.kseq, a.k10
;

-- AGG on one side, join big on small table, no group by
select sum(b.k2) 
from bench1m a, bench1m b 
where a.kseq = b.kseq and b.kseq < 10001
;

-- end
