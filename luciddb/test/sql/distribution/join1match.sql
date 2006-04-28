--
-- eqjoin with returning only one row
--
set schema 's';

select b1.k100, b2.kseq
from distribution_100 b1, distribution_100 b2
where (b1.k100 - 99) = b2.kseq
--order by b1.k100, b2.kseq;
order by 1,2;

-- select b1.k500k, b2.kseq
-- from distribution_1m b2, distribution_1m b1
-- where (b1.k500k - 499999) = b2.kseq
-- --order by b1.k500k, b2.kseq;
-- order by 1,2;
