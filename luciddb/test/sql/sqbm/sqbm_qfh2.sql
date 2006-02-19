-- 
-- test from hell: ultra joins
--

select count(*), sum(b1.k10 - b2.k25)
from bench1m b1, bench1m b2
where b1.k1k=b2.k10k
;
