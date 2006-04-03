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
    directory 'unitsql/med/flatfiles',
    file_extension 'csv',
    with_header 'yes', 
    log_directory 'testlog');

-- create a server without headers (for detecting other errors)
create server flatfile_server_noheader
foreign data wrapper local_file_wrapper
options (
    directory 'unitsql/med/flatfiles',
    file_extension 'csv',
    with_header 'no', 
    log_directory 'testlog');


---------------------------------------------------------------------------
-- Part 1. Parser tests
---------------------------------------------------------------------------

--
-- 1.1 Test a table with explicit column definitions
-- 
create foreign table flatfile_explicit_table(
    id int not null,
    name varchar(50) not null,
    extra_field char(1) not null)
server flatfile_server
options (filename 'example');

select * from flatfile_explicit_table order by 3;

--
-- 1.2 Test a table whose row type is very large
--     (this should not throw an error, because we want to allow a large 
--         row type as long as the data is of manageable size)
--
create foreign table flatfile_rowTypeTooBig(
    a varchar(2000),
    b varchar(2000),
    c varchar(2000))
server flatfile_server
options (filename 'example');

select * from flatfile_rowTypeTooBig;

--
-- 1.3 Test a table with a missing data file
--
create foreign table flatfile_missing(
    id int not null,
    name varchar(50) not null,
    extra_field char(1) not null)
server flatfile_server
options (filename 'missing');

select * from flatfile_missing;

--
-- 1.4 Test whether an attempt is made to log errors
--     (the following column description is invalid)
--
create server flatfile_server_locked
foreign data wrapper local_file_wrapper
options (
    directory 'unitsql/med/flatfiles',
    file_extension 'csv',
    with_header 'no', 
    log_directory 'unitsql/med/flatfiles');

create foreign table flatfile_locked(
    id int not null,
    name varchar(50) not null)
server flatfile_server_locked
options (
    filename 'noheader',
    log_filename 'locked');

select * from flatfile_locked;

--
-- 1.5 Test bad line delimiter
--      (note that the delimiter does not occur in the file)
--      (note that you can also choose an empty file extension
--          as a trick to pass the full path to foreign tables)
--
create server flatfile_server_badLineDelim
foreign data wrapper local_file_wrapper
options (
    file_extension '',
    with_header 'no',
    line_delimiter '\t', 
    log_directory 'testlog/');

create foreign table flatfile_badLineDelim(
    id int not null,
    name varchar(50) not null,
    extra_field char(1) not null)
server flatfile_server_badLineDelim
options (filename 'unitsql/med/flatfiles/noheader.csv');

select * from flatfile_badLineDelim;

--
-- 1.6 Test bad field delimiter
--      (note that the delimiter does not occur in the file)
--
create server flatfile_server_badFieldDelim
foreign data wrapper local_file_wrapper
options (
    file_extension 'csv',
    with_header 'no',
    field_delimiter '\t', 
    log_directory 'testlog');

create foreign table flatfile_badFieldDelim(
    id int not null,
    name varchar(50) not null,
    extra_field char(1) not null)
server flatfile_server_badFieldDelim
options (filename 'unitsql/med/flatfiles/noheader');

select * from flatfile_badFieldDelim;

--
-- 1.7 Test bad line delimiter
--     (when it occurs in the file)
--     (incomplete column is detected, because the file doesn't end 
--         with a delimiter)
--     (note data file is assumed to have at least one 'G')
--
create server flatfile_server_incompleteColumn
foreign data wrapper local_file_wrapper
options (
    file_extension 'csv',
    with_header 'no',
    line_delimiter 'G', 
    log_directory 'testlog');

create foreign table flatfile_incompleteColumn(
    id int not null,
    name varchar(50) not null,
    extra_field char(1) not null)
server flatfile_server_incompleteColumn
options (filename 'unitsql/med/flatfiles/noheader');

select * from flatfile_incompleteColumn;

--
-- 1.8 Test data file with too many columns
--
create foreign table flatfile_tooManyColumns(
    id int not null,
    name varchar(50) not null)
server flatfile_server_noheader
options (filename 'noheader');

select * from flatfile_tooManyColumns;

--
-- 1.9 Test data file with too few columns
--
create foreign table flatfile_tooFewColumns(
    id int not null,
    name varchar(50) not null,
    extra_field char(1) not null,
    extra_field2 char(1) not null)
server flatfile_server_noheader
options (filename 'example');

select * from flatfile_tooFewColumns;

--
-- 1.10 Test row which is too long (the text is larger than one page)
--      (parser recovers and returns another error, but expected error
--          can be viewed in error log)
--
create server flatfile_server_rowTooLong
foreign data wrapper local_file_wrapper
options (
    directory 'unitsql/med/flatfiles',
    file_extension 'txt',
    with_header 'yes', 
    log_directory 'testlog');

select * from flatfile_server_rowTooLong.BCP."longrow";

-- long column quietly truncates
select * from flatfile_server_rowTooLong.BCP."longcol";

--
-- 1.11 different escape and quote characters
--
create server flatfile_server_esc
foreign data wrapper local_file_wrapper
options (
    directory 'unitsql/med/flatfiles',
    file_extension 'esc',
    control_file_extension 'ctl',
    with_header 'yes', 
    escape_char '\',
    log_directory 'testlog');

select * from flatfile_server_esc.BCP."example" order by 3;


---------------------------------------------------------------------------
-- Part 2. Test fixed position file parsing                              --
---------------------------------------------------------------------------

--
-- 2.1 invalid parameters
--
create server flatfile_server_fixed
foreign data wrapper local_file_wrapper
options (
    directory 'unitsql/med/flatfiles',
    file_extension 'dat',
    with_header 'no',
    field_delimiter '',
    line_delimiter '\r\n', 
    log_directory 'testlog');

--
-- 2.2 valid definition
--
create server flatfile_server_fixed
foreign data wrapper local_file_wrapper
options (
    directory 'unitsql/med/flatfiles',
    file_extension 'dat',
    with_header 'no',
    field_delimiter '',
    escape_char '',
    quote_char '',
    line_delimiter '\r\n', 
    log_directory 'testlog');

select * from flatfile_server_fixed.BCP."fixed" order by 3;


---------------------------------------------------------------------------
-- Part 3. Sampling queries                                              --
---------------------------------------------------------------------------

--
-- 3.1 Test a describe query. 
--     This type of query returns the width of a table's fields. it is an 
--     internal query, and should not appear in user level documentation.
--
select * from flatfile_server.SAMPLE_DESC."example";

--
-- 3.2 Test a sampling queries 
--     (which become important in the absence of bcp control files)
--     (there is no need to order their results, because the results appear 
--         as they do in the source file. in this case, the first line is 
--         the "header line". this feature is also undocumented.)
--
select * from flatfile_server.SAMPLE."example";

--
-- 3.3 Test sampling of missing file
--
select * from flatfile_server.SAMPLE."missing";

--
-- 3.4 Test sampling of an empty file (no header)
--
create server flatfile_server_empty
foreign data wrapper local_file_wrapper
options (
    directory 'unitsql/med/flatfiles',
    file_extension 'txt',
    log_directory 'testlog');

select * from flatfile_server_empty.SAMPLE."empty";

-- Missing header error is returned when control file is supplied
select * from flatfile_server_empty.BCP."empty";

--
-- 3.5 Describe a fixed format file (illegal)
--
select * from flatfile_server_fixed.SAMPLE_DESC."fixed";

--
-- 3.6 Test sampling of a file perhaps with header, but with no data
--
select * from flatfile_server_empty.BCP."emptydata";

--
-- 3.7 Test sampling of a file with nulls in header
--
select * from flatfile_server_empty.BCP."nullheader";

--
-- 3.8 Test sampling of a file with nulls in data
--
select * from flatfile_server_empty.BCP."nulldata";


---------------------------------------------------------------------------
-- Part 4. Reading metadata from bcp files                               --
---------------------------------------------------------------------------

-- test import foreign schema using wrong schema name
import foreign schema testdata
from server flatfile_server
into flatfile_schema;

-- test: import foreign schema
-- the directory contains an empty data file with no control file
-- fails all imports
create server flatfile_server_fail
foreign data wrapper local_file_wrapper
options (
    directory 'unitsql/med/flatfiles',
    file_extension 'fail',
    control_file_extension 'failbcp',
    log_directory 'testlog');

import foreign schema bcp
from server flatfile_server_fail
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
