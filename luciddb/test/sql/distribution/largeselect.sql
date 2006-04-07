--
-- equijoins with large select list
--
set schema 's';

select b1.k100, b1.kseq, b1.k100square, b1.kseqsquare,
       b2.k100, b2.kseq, b2.k100square, b2.kseqsquare
from distribution_100 b1, distribution_100 b2
where b1.k100square = b2.kseqsquare
order by b1.k100, b1.kseq, b2.kseq, b2.k100;

-- select b1.k10k, b1.kseq, b1.k10ksquare, b1.kseqsquare,
--        b2.k10k, b2.kseq, b2.k10ksquare, b2.kseqsquare
-- from distribution_10k b1, distribution_10k b2
-- where b1.k10ksquare = b2.kseqsquare
-- order by b1.k10k, b1.kseq, b2.kseq, b2.k10k
-- /
-- select b1.k100k, b1.kseq, b1.k100ksquare, b1.kseqsquare,
--        b2.k100k, b2.kseq, b2.k100ksquare, b2.kseqsquare
-- from distribution_100k b1, distribution_100k b2
-- where b1.k100ksquare = b2.kseqsquare
-- order by b1.k100k, b1.kseq, b2.kseq, b2.k100k
-- /
-- select b1.k500k, b1.kseq, b1.k500ksquare, b1.kseqsquare,
--        b2.k500k, b2.kseq, b2.k500ksquare, b2.kseqsquare
-- from distribution_1m b2, distribution_1m b1
-- where b1.k500ksquare = b2.kseqsquare
-- order by b1.k500k, b1.kseq, b2.kseq, b2.k500k
-- /
