-- $Id$
-- Test flatfile namespace plugin

create schema flatfile_schema;

set schema 'flatfile_schema';

-- create wrapper for access to flatfile data
-- sys_file_wrapper has alread been allocated, but depending on a 
-- local data wrapper helps the test cleanup scripts
create foreign data wrapper local_file_wrapper
library 'class com.lucidera.farrago.namespace.flatfile.FlatFileDataWrapper'
language java;

-- create a server for general use
create server flatfile_server
foreign data wrapper local_file_wrapper
options (
    directory 'unitsql/med',
    file_extension 'csv',
    with_header 'yes', 
    log_directory 'testlog');

-- test a table with explicit column definitions
create foreign table flatfile_explicit_table(
    id int not null,
    name varchar(50) not null,
    extra_field char(1) not null)
server flatfile_server
options (filename 'example');

select * from flatfile_explicit_table order by 3;

-- test a table whose row type is too large
create foreign table flatfile_rowTypeTooBig(
    a varchar(2000),
    b varchar(2000),
    c varchar(2000))
server flatfile_server
options (filename 'example');

select * from flatfile_rowTypeTooBig;

-- test a table with a missing data file
create foreign table flatfile_missing(
    id int not null,
    name varchar(50) not null,
    extra_field char(1) not null)
server flatfile_server
options (filename 'missing');

select * from flatfile_missing;

-- test whether an attempt is made to log errors
-- note that this column description is invalid
create server flatfile_server_locked
foreign data wrapper local_file_wrapper
options (
    directory 'unitsql/med',
    file_extension 'csv',
    with_header 'yes', 
    log_directory 'unitsql/med');

create foreign table flatfile_locked(
    id int not null,
    name varchar(50) not null)
server flatfile_server_locked
options (
    filename 'example',
    log_filename 'locked');

select * from flatfile_locked;

-- test: bad line delimiter is given
-- note that the delimiter does not occur in the file
-- note that you can also choose an empty file extension
-- as a trick to pass the full path to foreign tables
create server flatfile_server_badLineDelim
foreign data wrapper local_file_wrapper
options (
    file_extension '',
    with_header 'yes',
    line_delimiter '\t', 
    log_directory 'testlog/');

create foreign table flatfile_badLineDelim(
    id int not null,
    name varchar(50) not null,
    extra_field char(1) not null)
server flatfile_server_badLineDelim
options (filename 'unitsql/med/example.csv');

select * from flatfile_badLineDelim;

-- test: bad field delimiter is given
-- note that the delimiter does not occur in the file
create server flatfile_server_badFieldDelim
foreign data wrapper local_file_wrapper
options (
    file_extension 'csv',
    with_header 'yes',
    field_delimiter '\t', 
    log_directory 'testlog');

create foreign table flatfile_badFieldDelim(
    id int not null,
    name varchar(50) not null,
    extra_field char(1) not null)
server flatfile_server_badFieldDelim
options (filename 'unitsql/med/example');

select * from flatfile_badFieldDelim;

-- test: bad line delimiter is specified
-- (but it does occur in the file)
-- note: data file is assumed to have at least one 'G'
create server flatfile_server_incompleteColumn
foreign data wrapper local_file_wrapper
options (
    file_extension 'csv',
    with_header 'yes',
    line_delimiter 'G', 
    log_directory 'testlog');

create foreign table flatfile_incompleteColumn(
    id int not null,
    name varchar(50) not null,
    extra_field char(1) not null)
server flatfile_server_incompleteColumn
options (filename 'unitsql/med/example');

select * from flatfile_incompleteColumn;

-- test: data file has too many columns
create foreign table flatfile_tooManyColumns(
    id int not null,
    name varchar(50) not null)
server flatfile_server
options (filename 'example');

select * from flatfile_tooManyColumns;

-- test: data file has too few columns
create foreign table flatfile_tooFewColumns(
    id int not null,
    name varchar(50) not null,
    extra_field char(1) not null,
    extra_field2 char(1) not null)
server flatfile_server
options (filename 'example');

select * from flatfile_tooFewColumns;

-- test: row text is too long in data file
-- the parser should give up when it's reached the max size
-- and should interpret this row as multiple rows

-- test a describe query. this type of query returns the width of a 
-- table's fields. it is an internal query, and should not appear in
-- user level documentation.
select * from flatfile_server.SAMPLE_DESC."example2";

-- test a sampling queries (which become important in the absence of 
-- bcp control files). there is no need to order their results, because 
-- the results appear as they do in the source file. in this case, the 
-- first line is the "header line". this feature is also undocumented.
select * from flatfile_server.SAMPLE."example2";

-- test sampling errors
select * from flatfile_server.SAMPLE."missing";

select * from flatfile_server.SAMPLE."empty";

-- test: read metadata from bcp files

-- test import foreign schema using wrong schema name
import foreign schema testdata
from server flatfile_server
into flatfile_schema;

-- test: import foreign schema
-- the directory contains an empty data file with no control file
-- fails all imports
import foreign schema bcp
from server flatfile_server
into flatfile_schema;

-- test: should fail; entire import failed
select * from flatfile_schema."example" order by 3;

-- test a table using implicit column definitions
create foreign table flatfile_implicit_table
server flatfile_server
options (filename 'example');

select * from flatfile_implicit_table order by 3;

-- test import schemas with/without bcp files
create schema flatfiledir_schema;

set schema 'flatfiledir_schema';

create server flatfiledir_server
foreign data wrapper local_file_wrapper
options (
    directory 'unitsql/med/flatfiles',
    file_extension 'csv',
    with_header 'yes', 
    log_directory 'testlog');

import foreign schema bcp
from server flatfiledir_server
into flatfiledir_schema;

select * from flatfiledir_schema."example2" order by 3;

drop schema flatfiledir_schema cascade;
create schema flatfiledir_schema;
set schema 'flatfiledir_schema';

import foreign schema bcp EXCEPT TABLE_NAME LIKE 'E%'
from server flatfiledir_server
into flatfiledir_schema;

select * from flatfiledir_schema."example2" order by 3;
drop table flatfiledir_schema."example2";
drop table flatfiledir_schema."example";

-- test files with null values
select * from flatfiledir_schema."withnulls" order by 3;
drop table flatfiledir_schema."withnulls";

-- test badly qualified import foreign schema
import foreign schema bcp LIMIT TO ("no_table")
from server flatfiledir_server
into flatfiledir_schema;

-- test qualified import foreign schema
import foreign schema bcp LIMIT TO ("example")
from server flatfiledir_server
into flatfiledir_schema;

select * from flatfiledir_schema."example" order by 3;

-- test: not imported. fail
select * from flatfiledir_schema."example2" order by 3;

drop table flatfiledir_schema."example";

import foreign schema bcp LIMIT TO TABLE_NAME LIKE 'e%'
from server flatfiledir_server
into flatfiledir_schema;

select * from flatfiledir_schema."example" order by 3;
drop table flatfiledir_schema."example";

-- test: uses generated bcp
select * from flatfiledir_server.bcp."example2" order by 3;
