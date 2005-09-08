-- $Id$
-- Miscellaneous DDL

set schema 'sales';

-- Table with description
create table foo (i int primary key) description 'this is a table';

drop table foo;

-- End misc.sql
