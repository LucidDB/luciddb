--
-- equijoins with the filtering condition
-- equal on 2 columns of the same table
--
set schema 's';

select b1.k500k, b1.kseq, b2.kseq, b2.k500k
from distribution_1m b2, distribution_1m b1
where b1.k500ksquare = b2.kseqsquare
  and b1.k500k+125836 = b1.kseq
--order by b1.k500k, b1.kseq, b2.kseq, b2.k500k
order by 1,2,3,4;
