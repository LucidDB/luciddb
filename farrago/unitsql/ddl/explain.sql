-- $Id$
-- Test EXPLAIN command.

set schema 'sales';

-- Explain logical plan brief
explain plan excluding attributes without implementation for 
select 1 from depts;

-- Explain logical plan normal
explain plan including attributes without implementation for 
select 1 from depts;

-- Explain logical plan verbose
explain plan including all attributes without implementation for
select 1 from depts;

-- Explain physical plan brief
explain plan excluding attributes for
select 1 from depts;

-- Explain physical plan normal
explain plan including attributes for
select 1 from depts;

-- Explain physical plan verbose
explain plan including all attributes for
select 1 from depts;

-- Explain physical plan in XML format.
explain plan as xml for 
select 1 from depts;
