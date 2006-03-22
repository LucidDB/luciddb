-- 
-- test from hell: ultra joins with large group by
--
set schema 's';

select b1.k100k, count(b1.k100k), sum(b1.k10), sum(b2.k25)
from bench1m b1, bench1m b2
where b1.kseq=b2.kseq
group by b1.k100k
order by 1
;

