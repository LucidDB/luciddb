-- $Id$
-- Miscellaneous DDL

create schema misc;
set schema 'misc';
set path 'misc';

-- Table with description
create table table_with_desc (i int primary key) description 'this is a table';

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
create clustered index ix2 on dup_clustered(j)
;

-- should fail:  FTRS tables must have a primary key
create table missing_pk(i int not null,j int not null);


-- LCS-specific table validation rules

-- LCS tables don't require primary keys
create table lcs_table(i int not null)
server sys_column_store_data_server
;

-- verify creation of system-defined clustered index
!indexes LCS_TABLE

-- LCS tables may have multiple clustered indexes
create table lcs_table_explicit(i int not null,j int not null,k int not null)
server sys_column_store_data_server
create clustered index explicit_i on lcs_table_explicit(i)
create clustered index explicit_jk1 on lcs_table_explicit(j,k)
;

-- verify creation of user-defined clustered indexes
!indexes LCS_TABLE_EXPLICIT

-- should fail: LCS clustered indexes may not overlap
create table lcs_table_overlap(i int not null,j int not null,k int not null)
server sys_column_store_data_server
create clustered index explicit_ij on lcs_table_overlap(i,j)
create clustered index explicit_jk2 on lcs_table_overlap(j,k)
;

-- test usage of UDT's for column type
create type square as (
    side_length double
) final;

create table lcs_table_udt(i int not null,s square)
server sys_column_store_data_server
;

-- test LCS drop/truncate
truncate table lcs_table;
drop table lcs_table_explicit;

-- test some features which aren't yet implemented for LCS

-- doesn't fail, but has no effect,
-- because for now unclustered indexes are stubbed out
create table lcs_table_unclustered(i int not null)
server sys_column_store_data_server
create index unclustered_i on lcs_table_unclustered(i)
;

-- should fail:  can't handle collections yet
create table lcs_table_multiset(i int not null,im integer multiset)
server sys_column_store_data_server
;

-- End misc.sql
