-- $Id$
-- Multiset related regression tests

select*from unnest(multiset[1,2,3]);

select*from unnest(multiset[1+2,3-4,5*6,7/8,9+10*11*log(13)]);

select*from unnest(multiset['a','b'||'c']);

select*from unnest(multiset[row(1,2,3,4), row(11,22,33,44)]);

select*from unnest(multiset(select*from sales.depts));
