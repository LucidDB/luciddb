-- 
-- test from hell: ultra joins
--

select count(*), sum(b1.k10), sum(b2.k25)
from bench1m b1, bench1m b2
where b1.k500k = b2.k100k;
;
