-- $Id$
-- Test flatfile namespace plugin

create foreign data wrapper flatfile_foreign_wrapper
library 'class com.lucidera.farrago.namespace.flatfile.FlatFileDataWrapper'
language java;

create server flatfile_server
foreign data wrapper flatfile_foreign_wrapper
options (with_header 'yes', log_directory 'testlog');

create schema flatfile_schema;

set schema 'flatfile_schema';

create foreign table flatfile_explicit_table(
    id int not null,
    name varchar(50) not null,
    extra_field char(1) not null)
server flatfile_server
options (filename 'unitsql/med/example.csv');

select * from flatfile_explicit_table order by 3;

create foreign table flatfile_rowTypeTooBig(
    a varchar(2000),
    b varchar(2000),
    c varchar(2000))
server flatfile_server
options (filename 'unitsql/med/example.csv');

select * from flatfile_rowTypeTooBig;

create foreign table flatfile_missing(
    id int not null,
    name varchar(50) not null,
    extra_field char(1) not null)
server flatfile_server
options (filename 'unitsql/med/missing.csv');

select * from flatfile_missing;

-- note: this column description is invalid, and should cause errors
create foreign table flatfile_locked(
    id int not null,
    name varchar(50) not null)
server flatfile_server
options (
    filename 'unitsql/med/example.csv',
    error_filename 'unitsql/med/locked');

select * from flatfile_locked;

create server flatfile_server_badLineDelim
foreign data wrapper flatfile_foreign_wrapper
options (
    with_header 'yes',
    line_delimiter '\t', 
    log_directory 'testlog/');

create foreign table flatfile_badLineDelim(
    id int not null,
    name varchar(50) not null,
    extra_field char(1) not null)
server flatfile_server_badLineDelim
options (filename 'unitsql/med/example.csv');

select * from flatfile_schema.flatfile_badLineDelim;

create server flatfile_server_badFieldDelim
foreign data wrapper flatfile_foreign_wrapper
options (
    with_header 'yes',
    field_delimiter '\t', 
    log_directory 'testlog');

create foreign table flatfile_badFieldDelim(
    id int not null,
    name varchar(50) not null,
    extra_field char(1) not null)
server flatfile_server_badFieldDelim
options (filename 'unitsql/med/example.csv');

select * from flatfile_schema.flatfile_badFieldDelim;

-- data file is assumed to have at least one 'G'
create server flatfile_server_incompleteColumn
foreign data wrapper flatfile_foreign_wrapper
options (
    with_header 'yes',
    line_delimiter 'G', 
    log_directory 'testlog');

create foreign table flatfile_incompleteColumn(
    id int not null,
    name varchar(50) not null,
    extra_field char(1) not null)
server flatfile_server_incompleteColumn
options (filename 'unitsql/med/example.csv');

select * from flatfile_schema.flatfile_incompleteColumn;

create foreign table flatfile_tooManyColumns(
    id int not null,
    name varchar(50) not null)
server flatfile_server
options (filename 'unitsql/med/example.csv');

select * from flatfile_schema.flatfile_tooManyColumns;

create foreign table flatfile_tooFewColumns(
    id int not null,
    name varchar(50) not null,
    extra_field char(1) not null,
    extra_field2 char(1) not null)
server flatfile_server
options (filename 'unitsql/med/example.csv');

select * from flatfile_schema.flatfile_tooFewColumns;

-- we cannot cause the rowTextTooLong error because a row is 
-- guaranteed to fit within the buffers used for flat files.
-- however, if we decide to relax this restriction, throw in 
-- a test case.

-- should fail:  required metadata support not available
import foreign schema testdata
from server flatfile_server
into flatfile_schema;
