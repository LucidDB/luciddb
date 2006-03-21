-- Tests for inlined expressions

-- Queries to test the inlining

set schema 's';

select n,n+1 from bill order by 1;

select n,n+1 from bill2 order by 1;
