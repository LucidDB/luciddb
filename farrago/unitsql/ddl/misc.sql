-- $Id$
-- Miscellaneous DDL

set schema 'sales';

-- Table with description
create table foo (i int primary key) description 'this is a table';

drop table foo;

-- should fail:  a table may not have multiple primary keys
create table dup_pk(i int not null primary key,j int not null primary key);

-- should fail:  duplicate constraint names
create table dup_constraints(
    i int not null constraint charlie primary key,
    j int not null constraint charlie unique);

-- FTRS-specific table validation rules

-- should fail:  FTRS tables cannot have multiple clustered indexes
create table dup_clustered(i int not null primary key,j int not null)
create clustered index ix on dup_clustered(i)
create clustered index ix on dup_clustered(j)
;

-- should fail:  FTRS tables must have a primary key
create table missing_pk(i int not null,j int not null);

-- End misc.sql
