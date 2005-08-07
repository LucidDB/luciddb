-- $Id$
-- Throws unsupported types at the system, to make sure the errors are
-- civilized.

set schema 'sales';

-- should give error 'decimal is not supported'
create table td(n integer not null primary key, d decimal);

-- should give error 'decimal is not supported'
create table td5(n integer not null primary key, d decimal(5));

-- should give error 'decimal is not supported'
create table td52(n integer not null primary key, d decimal(5, 2));

values (cast(null as decimal));

values (cast(null as decimal(5)));

values (cast(null as decimal(5, 2)));

-- End types.sql
