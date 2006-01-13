-- $Id$
-- Multiset related regression tests

select*from unnest(multiset[1,2,3]);

select*from unnest(multiset[1.34,1.563,2.0]);

select*from unnest(multiset[1.23e1, -23.e0]);

select*from unnest(multiset[1+2,3-4,5*6,7/8,9+10*11*log10(13)]);

select*from unnest(multiset['a','b'||'c']);

select*from unnest(multiset[row(1,2,3,4), row(11,22,33,44)]);

select*from unnest(multiset[row(43.2, 421e-2), row(32.22, 43e-1)]);

select*from unnest(multiset(select*from sales.depts));

select*from unnest(values(multiset[1]));

select*from unnest(values(multiset[43e-1]));

select*from unnest(values(multiset[32.342]));

select*from unnest(values(multiset[43, 32.2, 34e-2]));

select*from unnest(values(multiset(select*from sales.emps)));

--- TODO: Enable tests when multiset support is more complete

-- FIXME: Need to implement RelStructuredTypeFlattener.visit(RexFieldAccess)
--        for RexCorrelVariable
-- select*from unnest(select multiset[empno] from sales.emps);

-- FIXME: Need to implement RelStructuredTypeFlattener.visit(RexFieldAccess)
--        for RexCorrelVariable
-- select*from unnest(select multiset[(empno, name)] from sales.emps);

-- Test values
-- values(multiset[]);
-- values(multiset[1]);
-- values(multiset[multiset[1]]);

-- Test Union
-- values(multiset[1]) union values(multiset[2]);
-- values(multiset[1]) union all values(multiset[1]);
-- values(multiset[1]) union distinct values(multiset[1]);

-- select * from unnest(values(multiset[1]) union values(multiset[2]));

-- values(multiset[1] multiset union multiset[1,2]);
-- values(multiset[1] multiset union all multiset[1,2]);
-- values(multiset[1] multiset union distinct multiset[1,2]);

-- select * from unnest((multiset[1] multiset union multiset[1,2]));

-- Test Intersect
-- values(multiset[1]) intersect values(multiset[1]);
-- values(multiset[1]) intersect all values(multiset[1]);
-- values(multiset[1]) intersect distinct values(multiset[1]);

-- values(multiset[1,3,5] multiset intersect multiset[1,2]);
-- values(multiset[1,3,5] multiset intersect all multiset[1,2]);
-- values(multiset[1,3,5] multiset intersect distinct multiset[1,2]);

-- Test Except
-- values(multiset[1]) except values(multiset[1]);
-- values(multiset[1]) except all values(multiset[1]);
-- values(multiset[1]) except distinct values(multiset[1]);

-- values(multiset[1,1,2,3,5] multiset except multiset[1,5]);
-- values(multiset[1,1,2,3,5] multiset except all multiset[1,5]);
-- values(multiset[1,1,2,3,5] multiset except distinct multiset[1,5]);

-- Test Element
-- values(element(multiset[1]));

-- Should give error, multiset has more than one elements
-- values(element(multiset[1,2]));

-- Should give error, multiset has 0 elements
-- (at least when intersect works)
-- values(element(multiset[1] multiset intersect multiset[2]));

-- Test Cardinality
-- values(cardinality(multiset[1]));
-- values(cardinality(multiset[(1,2,3,4)]));
-- values(cardinality(multiset[1,2,3,4]));

-- Test other multiset operators
-- values(multiset[1] is a set);
-- values(1 member of multiset[1]);
-- values(2 member of multiset[1]);
-- values(fusion(multiset[1]));
-- select collect(empno) from sales.emps;

-- TODO: Test Lateral, submultiset, cast

-- Test tables with multisets
-- create schema multiset_test;
-- set schema 'multiset_test';
-- set path 'multiset_test';

-- Simple table with multiset
-- create table multiset_table1(
--     id integer primary key,
--     items integer multiset);

-- insert into multiset_table1 values(1, multiset[1,2,3]);

-- select * from multiset_table1;
-- select * from unnest(multiset_table1.items);

-- select * from multiset_table1 as t, unnest(t.items);
-- select * from unnest(t.items), multiset_table1 as t;

-- select * from unnest(t1.x) as t2, unnest(t2.y) as t1;

-- Test table with nested multiset
-- create type nested_row AS (
--    i integer,
--    names varchar(10) multiset ) final;

-- create table multiset_table2(
--    id integer primary key,
--    nested_items nested_row  multiset);

-- insert into multiset_table2 
--    values(1, 
--           multiset[ ( 2, multiset['a', 'b'] ) ] );

-- select * from multiset_table2;
-- select * from unnest(multiset_table2.nested_items);

