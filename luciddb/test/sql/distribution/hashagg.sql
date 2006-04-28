--
-- equijoins with aggregation
--
set schema 's';

select min(b2.kseq), max(b2.kseq), sum(b2.kseq),
       min(b2.k500k), max(b2.k500k), count(b2.k500k)
from distribution_1m b1, distribution_1m b2
where b1.k500ksquare = b2.kseqsquare;
