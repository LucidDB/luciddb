-- $Id$
-- Test multiset related queries

set schema 'sales';

-- force usage of Fennel calculator
alter system set "calcVirtualMachine" = 'CALCVM_FENNEL';

explain plan for select*from unnest(multiset[1,2]);
explain plan for select*from unnest(multiset[1+2,3*4/5]);
