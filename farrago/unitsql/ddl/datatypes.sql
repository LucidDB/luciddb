-- Test case illustrating all builtin datatypes.
CREATE OR REPLACE SCHEMA datatype;
SET SCHEMA 'datatype';
!set outputformat csv
CREATE TABLE datatype_table (
 "bigint" bigint,
 "binary" binary,
 "binary(0)" binary(0),
 "binary(2)" binary(2),
 "binary varying" binary varying,
 "binary varying(3)" binary varying(3),
 "boolean" boolean not null primary key,
 "char" char,
 "char(0)" char(0),
 "char(5)" char(5),
 "character" character,
 "character(6)" character(6),
 "character varying" character varying(7),
 "character varying(7)" character varying,
 "char varying" char varying(8),
 "char varying(8)" char varying,
 "date" date,
 "dec" dec,
 "decimal" decimal,
 "decimal(9)" decimal(9),
 "decimal(9,2)" decimal(9,2),
 "double" double,
 "double precision" double precision,
 "float" float,
 "int" int,
 "integer" integer,
 "numeric" numeric,
 "numeric(9)" numeric(9),
 "numeric(9,2)" numeric(9,2),
 "real" real,
 "smallint" smallint,
 "time" time,
 "time(0)" time(0),
 "timestamp" timestamp,
 "timestamp(0)" timestamp(0),
 "tinyint" tinyint,
 "varbinary" varbinary,
 "varbinary(9)" varbinary(9),
 "varchar" varchar,
 "varchar(10)" varchar(10));

-- Find out actual column types.
-- Note synonyms:
-- * CHARACTER becomes CHAR
-- * CHAR VARYING and CHARACTER VARYING become VARCHAR
-- * BINARY VARYING becomes VARBINARY
-- * FLOAT and DOUBLE PRECISION become DOUBLE
-- * DEC and NUMERIC become DECIMAL
-- * INT becomes INTEGER
--
-- Also note:
-- * CHAR, BINARY precision defaults to 0.
-- * FIXME: Per SQL:2008, VARCHAR, VARBINARY should require precision.
-- * FIXME: CHAR, BINARY, VARCHAR, VARBINARY should not accept precision 0.
!describe datatype.datatype_table

-- fails; BLOB not supported
CREATE TABLE blob_table (i int not null primary key, "blob" blob);

-- fails; BINARY LARGE OBJECT not supported
CREATE TABLE blob_table (i int not null primary key, "binary large object" binary large object);

-- fails; CLOB not supported
CREATE TABLE clob_table (i int not null primary key, "clob" clob);

-- fails; CLOB not supported
CREATE TABLE clob_table (i int not null primary key, "character large object" character large object);

-- fails; INTERVAL not supported as a column type
CREATE TABLE interval_table (i int not null primary key, "interval" interval day to second);

-- fails; TIME WITH TIME ZONE not supported as a column type
CREATE TABLE twtz_table (i int not null primary key, "time with time zone" time with time zone);

-- fails; TIME WITHOUT TIME ZONE not supported as a column type
CREATE TABLE twotz_table (i int not null primary key, "time without time zone" time without time zone);

-- fails; TIMESTAMP WITH TIME ZONE not supported as a column type
CREATE TABLE tswtz_table (i int not null primary key, "timestamp with time zone" timestamp with time zone);

-- fails; TIMESTAMP WITHOUT TIME ZONE not supported as a column type
CREATE TABLE tswotz_table (i int not null primary key, "timestamp without time zone" timestamp without time zone);

-- fails; TIMESTAMP(p) not supported as a column type
CREATE TABLE timestampp_table (i int not null primary key, "timestamp(3)" timestamp(3));

-- fails; TIME(p) not supported as a column type
CREATE TABLE timep_table (i int not null primary key, "time(3)" time(3));

-- fails; NUMERIC with neg scale
CREATE TABLE numericnegscale_table (i int not null primary key, "numeric(5,-3)" numeric(5,-3));

-- fails; DECIMAL with neg scale
CREATE TABLE decimalnegscale_table (i int not null primary key, "decimal(5,-3)" decimal(5,-3));

-- fails; DEC with neg scale
CREATE TABLE decnegscale_table (i int not null primary key, "dec(5,-3)" dec(5,-3));

-- fails; NUMBER not supported as a column type
CREATE TABLE number_table (i int not null primary key, "number" number);

-- fails; NUMBER(p) not supported as a column type
CREATE TABLE numberp_table (i int not null primary key, "number(5)" number(5));

-- fails; NUMBER(p,s) not supported as a column type
CREATE TABLE numberps_table (i int not null primary key, "number(5,2)" number(5,2));

-- end datatypes.sql

