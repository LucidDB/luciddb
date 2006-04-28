--
-- equijoins
--
set schema 's';

select k100, kseq
from distribution_100
where k100 = 53 or k100 = 47
   or kseq = 53 or kseq = 47
order by k100, kseq;

select b1.k100, b1.kseq, b2.kseq, b2.k100
from distribution_100 b1, distribution_100 b2
where b1.k100square = b2.kseqsquare
--order by b1.k100, b1.kseq, b2.kseq, b2.k100;
order by 1,2,3,4;

select k10k, kseq
from distribution_10k
where k10k = 5003 or k10k = 4997
   or kseq = 5003 or kseq = 4997
--order by k10k, kseq
order by 1,2;

select b1.k10k, b1.kseq, b2.kseq, b2.k10k
from distribution_10k b1, distribution_10k b2
where b1.k10ksquare = b2.kseqsquare
--order by b1.k10k, b1.kseq, b2.kseq, b2.k10k
order by 1,2,3,4;

select k100k, kseq
from distribution_100k
where k100k = 50003 or k100k = 49997
   or kseq = 50003 or kseq = 49997
--order by k100k, kseq
order by 1,2;

select b1.k100k, b1.kseq, b2.kseq, b2.k100k
from distribution_100k b1, distribution_100k b2
where b1.k100ksquare = b2.kseqsquare
--order by b1.k100k, b1.kseq, b2.kseq, b2.k100k
order by 1,2,3,4;

select k500k, kseq
from distribution_1m
where k500k between 249961 and 249970
   or k500k between 250030 and 250039
   or kseq between 249961 and 249970
   or kseq between 250030 and 250039
--order by k500k, kseq
order by 1,2;

select b1.k500k, b1.kseq, b2.kseq, b2.k500k
from distribution_1m b2, distribution_1m b1
where b1.k500ksquare = b2.kseqsquare
--order by b1.k500k, b1.kseq, b2.kseq, b2.k500k
order by 1,2,3,4;
