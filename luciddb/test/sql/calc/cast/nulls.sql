-- create null table

create schema ncast;

set schema 'ncast';

create table alltypes_null (
c_bigint bigint,
c_boolean boolean,
c_char char(12),
c_date date,
c_decimal decimal,
c_double double,
c_float float,
c_integer integer,
c_numeric_1 numeric(8,4),
c_numeric_2 numeric(6,2),
c_real real,
c_smallint smallint,
c_time time,
c_timestamp timestamp,
c_tinyint tinyint,
c_varbinary varbinary(13),
c_varchar varchar(23)
);

insert into alltypes_null values
(null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null);

-- test castable types according to SQL2003 spec,
-- negative tests already exist in basic 

-- bigint
select cast(c_bigint as bigint) from alltypes_null;
select cast(c_bigint as char(12)) from alltypes_null;
select cast(c_bigint as decimal) from alltypes_null;
select cast(c_bigint as double) from alltypes_null;
select cast(c_bigint as float) from alltypes_null;
select cast(c_bigint as integer) from alltypes_null;
select cast(c_bigint as numeric(8,4)) from alltypes_null;
select cast(c_bigint as numeric(6,2)) from alltypes_null;
select cast(c_bigint as real) from alltypes_null;
select cast(c_bigint as smallint) from alltypes_null;
select cast(c_bigint as tinyint) from alltypes_null;
select cast(c_bigint as varchar(23)) from alltypes_null;

-- boolean
select cast(c_boolean as boolean) from alltypes_null;
select cast(c_boolean as char(12)) from alltypes_null;
select cast(c_boolean as varchar(23)) from alltypes_null;

-- char(12)
select cast(c_char as bigint) from alltypes_null;
select cast(c_char as boolean) from alltypes_null;
select cast(c_char as char(12)) from alltypes_null;
select cast(c_char as date) from alltypes_null;
select cast(c_char as decimal) from alltypes_null;
select cast(c_char as double) from alltypes_null;
select cast(c_char as float) from alltypes_null;
select cast(c_char as integer) from alltypes_null;
select cast(c_char as numeric(8,4)) from alltypes_null;
select cast(c_char as numeric(6,2)) from alltypes_null;
select cast(c_char as real) from alltypes_null;
select cast(c_char as smallint) from alltypes_null;
select cast(c_char as time) from alltypes_null;
select cast(c_char as timestamp) from alltypes_null;
select cast(c_char as tinyint) from alltypes_null;
select cast(c_char as varchar(23)) from alltypes_null;

-- decimal
select cast(c_decimal as bigint) from alltypes_null;
select cast(c_decimal as char(12)) from alltypes_null;
select cast(c_decimal as decimal) from alltypes_null;
select cast(c_decimal as double) from alltypes_null;
select cast(c_decimal as float) from alltypes_null;
select cast(c_decimal as integer) from alltypes_null;
select cast(c_decimal as numeric(8,4)) from alltypes_null;
select cast(c_decimal as numeric(6,2)) from alltypes_null;
select cast(c_decimal as real) from alltypes_null;
select cast(c_decimal as smallint) from alltypes_null;
select cast(c_decimal as tinyint) from alltypes_null;
select cast(c_decimal as varchar(23)) from alltypes_null;

-- double
select cast(c_double as bigint) from alltypes_null;
select cast(c_double as char(12)) from alltypes_null;
select cast(c_double as decimal) from alltypes_null;
select cast(c_double as double) from alltypes_null;
select cast(c_double as float) from alltypes_null;
select cast(c_double as integer) from alltypes_null;
select cast(c_double as numeric(8,4)) from alltypes_null;
select cast(c_double as numeric(6,2)) from alltypes_null;
select cast(c_double as real) from alltypes_null;
select cast(c_double as smallint) from alltypes_null;
select cast(c_double as tinyint) from alltypes_null;
select cast(c_double as varchar(23)) from alltypes_null;

-- float
select cast(c_float as bigint) from alltypes_null;
select cast(c_float as char(12)) from alltypes_null;
select cast(c_float as decimal) from alltypes_null;
select cast(c_float as double) from alltypes_null;
select cast(c_float as float) from alltypes_null;
select cast(c_float as integer) from alltypes_null;
select cast(c_float as numeric(8,4)) from alltypes_null;
select cast(c_float as numeric(6,2)) from alltypes_null;
select cast(c_float as real) from alltypes_null;
select cast(c_float as smallint) from alltypes_null;
select cast(c_float as tinyint) from alltypes_null;
select cast(c_float as varchar(23)) from alltypes_null;

-- integer
select cast(c_integer as bigint) from alltypes_null;
select cast(c_integer as char(12)) from alltypes_null;
select cast(c_integer as decimal) from alltypes_null;
select cast(c_integer as double) from alltypes_null;
select cast(c_integer as float) from alltypes_null;
select cast(c_integer as integer) from alltypes_null;
select cast(c_integer as numeric(8,4)) from alltypes_null;
select cast(c_integer as numeric(6,2)) from alltypes_null;
select cast(c_integer as real) from alltypes_null;
select cast(c_integer as smallint) from alltypes_null;
select cast(c_integer as tinyint) from alltypes_null;
select cast(c_integer as varchar(23)) from alltypes_null;

-- numeric(8,4)
select cast(c_numeric_1 as bigint) from alltypes_null;
select cast(c_numeric_1 as char(12)) from alltypes_null;
select cast(c_numeric_1 as decimal) from alltypes_null;
select cast(c_numeric_1 as double) from alltypes_null;
select cast(c_numeric_1 as float) from alltypes_null;
select cast(c_numeric_1 as integer) from alltypes_null;
select cast(c_numeric_1 as numeric(8,4)) from alltypes_null;
select cast(c_numeric_1 as numeric(6,2)) from alltypes_null;
select cast(c_numeric_1 as real) from alltypes_null;
select cast(c_numeric_1 as smallint) from alltypes_null;
select cast(c_numeric_1 as tinyint) from alltypes_null;
select cast(c_numeric_1 as varchar(23)) from alltypes_null;

-- numeric(6,2)
select cast(c_numeric_2 as bigint) from alltypes_null;
select cast(c_numeric_2 as char(12)) from alltypes_null;
select cast(c_numeric_2 as decimal) from alltypes_null;
select cast(c_numeric_2 as double) from alltypes_null;
select cast(c_numeric_2 as float) from alltypes_null;
select cast(c_numeric_2 as integer) from alltypes_null;
select cast(c_numeric_2 as numeric(8,4)) from alltypes_null;
select cast(c_numeric_2 as numeric(6,2)) from alltypes_null;
select cast(c_numeric_2 as real) from alltypes_null;
select cast(c_numeric_2 as smallint) from alltypes_null;
select cast(c_numeric_2 as tinyint) from alltypes_null;
select cast(c_numeric_2 as varchar(23)) from alltypes_null;

-- real
select cast(c_real as bigint) from alltypes_null;
select cast(c_real as char(12)) from alltypes_null;
select cast(c_real as decimal) from alltypes_null;
select cast(c_real as double) from alltypes_null;
select cast(c_real as float) from alltypes_null;
select cast(c_real as integer) from alltypes_null;
select cast(c_real as numeric(8,4)) from alltypes_null;
select cast(c_real as numeric(6,2)) from alltypes_null;
select cast(c_real as real) from alltypes_null;
select cast(c_real as smallint) from alltypes_null;
select cast(c_real as tinyint) from alltypes_null;
select cast(c_real as varchar(23)) from alltypes_null;

-- smallint
select cast(c_smallint as bigint) from alltypes_null;
select cast(c_smallint as char(12)) from alltypes_null;
select cast(c_smallint as decimal) from alltypes_null;
select cast(c_smallint as double) from alltypes_null;
select cast(c_smallint as float) from alltypes_null;
select cast(c_smallint as integer) from alltypes_null;
select cast(c_smallint as numeric(8,4)) from alltypes_null;
select cast(c_smallint as numeric(6,2)) from alltypes_null;
select cast(c_smallint as real) from alltypes_null;
select cast(c_smallint as smallint) from alltypes_null;
select cast(c_smallint as tinyint) from alltypes_null;
select cast(c_smallint as varchar(23)) from alltypes_null;

-- tinyint
select cast(c_tinyint as bigint) from alltypes_null;
select cast(c_tinyint as char(12)) from alltypes_null;
select cast(c_tinyint as decimal) from alltypes_null;
select cast(c_tinyint as double) from alltypes_null;
select cast(c_tinyint as float) from alltypes_null;
select cast(c_tinyint as integer) from alltypes_null;
select cast(c_tinyint as numeric(8,4)) from alltypes_null;
select cast(c_tinyint as numeric(6,2)) from alltypes_null;
select cast(c_tinyint as real) from alltypes_null;
select cast(c_tinyint as smallint) from alltypes_null;
select cast(c_tinyint as tinyint) from alltypes_null;
select cast(c_tinyint as varchar(23)) from alltypes_null;

-- varbinary(13)
select cast(c_varbinary as varbinary(10)) from alltypes_null;

-- varchar(23)
select cast(c_varchar as bigint) from alltypes_null;
select cast(c_varchar as boolean) from alltypes_null;
select cast(c_varchar as char(12)) from alltypes_null;
select cast(c_varchar as date) from alltypes_null;
select cast(c_varchar as decimal) from alltypes_null;
select cast(c_varchar as double) from alltypes_null;
select cast(c_varchar as float) from alltypes_null;
select cast(c_varchar as integer) from alltypes_null;
select cast(c_varchar as numeric(8,4)) from alltypes_null;
select cast(c_varchar as numeric(6,2)) from alltypes_null;
select cast(c_varchar as real) from alltypes_null;
select cast(c_varchar as smallint) from alltypes_null;
select cast(c_varchar as time) from alltypes_null;
select cast(c_varchar as timestamp) from alltypes_null;
select cast(c_varchar as tinyint) from alltypes_null;
select cast(c_varchar as varchar(23)) from alltypes_null;

-- cleanup
drop schema ncast cascade;