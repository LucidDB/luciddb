-- $Id$
-- Test flatfile namespace plugin

create foreign data wrapper flatfile_foreign_wrapper
library 'class com.lucidera.farrago.namespace.flatfile.FlatFileDataWrapper'
language java;

create server flatfile_server
foreign data wrapper flatfile_foreign_wrapper
options (with_header 'no');

create schema flatfile_schema;

set schema 'flatfile_schema';

create foreign table flatfile_explicit_table(
    id int not null,
    name varchar(50) not null,
    extra_field char(1) not null)
server flatfile_server
options (filename 'unitsql/med/example.csv');

select * from flatfile_explicit_table;

-- should fail:  required metadata support not available
import foreign schema testdata
from server flatfile_server
into flatfile_schema;
