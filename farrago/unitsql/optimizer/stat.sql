-- $Id$
-- This script tests statistics generation procedures

!set verbose true

--
-- Setup a link to an empty tpcd schema
--
create schema tpcd;

set schema 'tpcd';

create foreign data wrapper local_file_wrapper
library 'class com.lucidera.farrago.namespace.flatfile.FlatFileDataWrapper'
language java;

create server tpcd_server
foreign data wrapper local_file_wrapper
options (
    directory 'unitsql/optimizer/data/tpcd',
    file_extension 'tbl',
    with_header 'no', 
    log_directory 'testlog');

import foreign schema bcp 
limit to (
    "CUSTOMER","LINEITEM","NATION","ORDERS",
    "PART","PARTSUPP","REGION","SUPPLIER")
from server tpcd_server
into tpcd;


---------------------------------------------------------------------------
-- Fake statistics generation                                            --
--                                                                       --
-- Here we just generate a few data; would need some adjustment          --
-- for usage with actual tpcd queries (column names, generated values)   --
---------------------------------------------------------------------------

--
-- 1.1 Row counts, fine for foreign tables
--
call sys_boot.mgmt.stat_set_row_count('LOCALDB','TPCD','CUSTOMER',15000);
call sys_boot.mgmt.stat_set_row_count('LOCALDB','TPCD','LINEITEM',600572);
call sys_boot.mgmt.stat_set_row_count('LOCALDB','TPCD','NATION',25);
call sys_boot.mgmt.stat_set_row_count('LOCALDB','TPCD','ORDERS',150000);
call sys_boot.mgmt.stat_set_row_count('LOCALDB','TPCD','PART',20000);
call sys_boot.mgmt.stat_set_row_count('LOCALDB','TPCD','PARTSUPP',80000);
call sys_boot.mgmt.stat_set_row_count('LOCALDB','TPCD','REGION',5);
call sys_boot.mgmt.stat_set_row_count('LOCALDB','TPCD','SUPPLIER',1000);

select "name", "rowCount" from sys_fem.med."BaseColumnSet" order by 1;

--
-- 1.2 Page counts. We can neither index foreign tables or retrieve the 
--     page counts of foreign indexes, so make a dummy table
--
create table dummy_region (
    R_REGIONKEY  INTEGER PRIMARY KEY,
    R_NAME       VARCHAR(25) NOT NULL,
    R_COMMENT    VARCHAR(152));

call sys_boot.mgmt.stat_set_page_count(
    'LOCALDB','TPCD','SYS$CONSTRAINT_INDEX$DUMMY_REGION$SYS$PRIMARY_KEY',1);

select "name", "pageCount" from sys_fem.med."LocalIndex" order by 1;

--
-- 1.3 Column histograms, also fine for foreign tables
--

-- C_CUSTKEY
call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB','TPCD','CUSTOMER','F1',15000,10,1500,0,'0123456789');

-- C_NATIONKEY
call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB','TPCD','CUSTOMER','F4',25,10,25,0,'ABCDEFGHIJKLMNOPQRSTUVWXYZ');

select 
    c."name",
    "distinctValueCount" "values","percentageSampled" "percent",
    "barCount","rowsPerBar","rowsLastBar"
from 
    sys_fem.med."ColumnHistogram" h,
    sys_fem.sql2003."AbstractColumn" c
where c."Histogram" = h."mofId" order by 1;

-- query the distinct column C_CUSTKEY
select "ordinal","startingValue","valueCount"
from sys_fem.med."ColumnHistogramBar" where "startingValue" < 'A' order by 1;

-- query the low cardinality column C_NATIONKEY
select "ordinal","startingValue","valueCount"
from sys_fem.med."ColumnHistogramBar" where "startingValue" > '99' order by 1;

--
-- 1.4 Update a histogram with more data
--
call sys_boot.mgmt.stat_set_column_histogram(
    'LOCALDB','TPCD','CUSTOMER','F1',15000,100,15000,0,'0123456789');

select 
    c."name",
    "distinctValueCount" "values","percentageSampled" "percent",
    "barCount","rowsPerBar","rowsLastBar"
from 
    sys_fem.med."ColumnHistogram" h,
    sys_fem.sql2003."AbstractColumn" c
where c."Histogram" = h."mofId" order by 1;

select "ordinal","startingValue","valueCount"
from sys_fem.med."ColumnHistogramBar" where "startingValue" < 'A' order by 1;

-- FIXME: this query fails
-- select * from 
--     sys_fem.sql2003."AbstractColumn" c,
--     sys_fem.med."ColumnHistogramBar" b
-- where b."Histogram" = c."Histogram";
