--
-- eqjoin with returning no row
--
set schema 's';

select b1.k100, b2.kseq
from distribution_100 b1, distribution_100 b2
where (b1.k100 - 100) = b2.kseq
order by b1.k100, b2.kseq;

-- select b1.k10k, b2.kseq
-- from distribution_10k b1, distribution_10k b2
-- where (b1.k10k - 10000) = b2.kseq
-- order by b1.k10k, b2.kseq;

-- select b1.k100k, b2.kseq
-- from distribution_100k b1, distribution_100k b2
-- where (b1.k100k - 100000) = b2.kseq
-- order by b1.k100k, b2.kseq;

-- select b1.k500k, b2.kseq
-- from distribution_1m b2, distribution_1m b1
-- where (b1.k500k - 500000) = b2.kseq
-- order by b1.k500k, b2.kseq;

