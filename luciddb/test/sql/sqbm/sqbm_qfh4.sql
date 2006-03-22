-- 
-- test from hell: group by with lots of results
--
set schema 's';

select b1.k10, b2.k25, count(*), sum(b1.k10+b2.k25)
from bench1m b1, bench1m b2
where b1.kseq=b2.kseq
group by b1.k10, b2.k25
order by 1,2
;

