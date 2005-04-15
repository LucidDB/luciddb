-- $Id$
-- Test multiset related queries

set schema 'sales';

-- force usage of Fennel calculator
--alter system set "calcVirtualMachine" = 'CALCVM_FENNEL';

explain plan for select*from unnest(multiset[1,2]);
explain plan for select*from unnest(multiset[1+2,3*4/5]);

-- NOTE: at the time of adding this, aggregation was not fully supported
-- so this test will break once we get that support.
explain plan for select * from sales.emps where cardinality(multiset['abc'])=3;

explain plan for values element(multiset[5]);

explain plan for values multiset[1] multiset union multiset[2];
explain plan for values multiset[1] multiset union all multiset[2];
explain plan for values multiset[1] multiset union distinct multiset[2];

-- shouldnt fail but does due to current non-intersect support
explain plan for values multiset[1] multiset intersect multiset[2];
explain plan for values multiset[1] multiset intersect all multiset[2];
explain plan for values multiset[1] multiset intersect distinct multiset[2];

-- shouldnt fail but does due to current non-except support
explain plan for values multiset[1] multiset except multiset[2];
explain plan for values multiset[1] multiset except all multiset[2];
explain plan for values multiset[1] multiset except distinct multiset[2];

-- test cast from multiset to multiset
explain plan for select * from unnest(cast(multiset['1'] as double multiset));

-- test IS A SET
explain plan for values multiset[1] is a set;

-- test MEMBER OF
explain plan for values 632 MEMBER OF multiset[2];


-- explain plan for select fusion(multiset[3]) from emps;
-- explain plan for select collect(deptno) from emps;
-- explain plan for select collect(deptno), fusion(multiset[3]) from emps;



