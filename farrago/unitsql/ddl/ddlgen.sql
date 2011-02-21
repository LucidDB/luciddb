-- $Id$
-- Script which creates every kind of object, with all clauses.
-- Invoked from FarragoDdlGeneratorTest.testCustomSchema,
-- as well as as a standalone test.
--
-- Types not tested yet:
--  ...

!set force true
drop schema ddlgen cascade;
drop foreign data wrapper test_mdr cascade;

CREATE SCHEMA ddlgen DESCRIPTION 'schema with every object type';

-- Would prefer not to set default schema, but FRG-297 prevents
-- creation of UDT without a default schema.
SET SCHEMA 'ddlgen';
SET PATH 'ddlgen';

CREATE FOREIGN DATA WRAPPER test_mdr
LIBRARY 'class net.sf.farrago.namespace.mdr.MedMdrForeignDataWrapper'
LANGUAGE JAVA
DESCRIPTION 'private data wrapper for mdr';

CREATE SERVER mof_server_test
FOREIGN DATA WRAPPER test_mdr
OPTIONS (
    extent_name 'MOF',
    schema_name 'Model',
    "org.eigenbase.enki.implementationType" 'NETBEANS_MDR',
    "org.netbeans.mdr.persistence.Dir" 'unitsql/ddl/mdr')
DESCRIPTION 'a server';

CREATE FOREIGN TABLE ddlgen.mof_exception(
    name char(20),
    annotation varchar(128),
    container varchar(128),
    "SCOPE" varchar(128),
    visibility varchar(128),
    "mofId" varchar(128),
    "mofClassName" varchar(128))
SERVER mof_server_test
OPTIONS (class_name 'Exception')
DESCRIPTION 'a foreign table';

-- Disabled: sys_column_store_data_server not available in aspen.
-- Table with primary key, clustered and unclustered indexes
--CREATE TABLE ddlgen.t(i int not null primary key, j int)
--DESCRIPTION 'table with clustered and unclustered indexes'
--SERVER sys_column_store_data_server
--CREATE INDEX unclustered_i on ddlgen.t(i)
--CREATE CLUSTERED INDEX clustered_j on ddlgen.t(i);
--
-- Index, not as part of table definition
--CREATE INDEX unclustered_i2 on ddlgen.t(i, j);

-- Scoped table
CREATE GLOBAL TEMPORARY TABLE ddlgen.temptab(x char(3) primary key, y varbinary(10))
DESCRIPTION 'a temp table'
ON COMMIT DELETE ROWS;

-- Function with no SQL body
CREATE FUNCTION decrypt_public_key(k varbinary(50))
RETURNS varchar(25)
LANGUAGE java
NO SQL
EXTERNAL NAME 'class net.sf.farrago.test.FarragoTestUDR.decryptPublicKey';

-- View
CREATE VIEW v
DESCRIPTION 'a view' AS
select * FROM ddlgen.temptab WHERE x IS NOT NULL;

-- View depends on v and temptab
CREATE VIEW v2 AS
select * FROM v, temptab;

-- View depends on v and v2
CREATE VIEW v3 AS
select * FROM v, v2;

-- View depends on v and v3
CREATE VIEW aView AS
select decrypt_public_key(v3.y) FROM v3, v;

-- distinct type
CREATE TYPE simolean_currency AS DOUBLE FINAL DESCRIPTION 'a distinct type';

CREATE TYPE ddlgen.playing_card AS (
    card_suit char(1),
    card_rank integer
) FINAL
DESCRIPTION 'an object type'
CONSTRUCTOR METHOD playing_card (
    card_suit_init char(1),card_rank_init integer)
RETURNS ddlgen.playing_card
SELF AS RESULT
CONTAINS SQL
SPECIFIC ddlgen.playing_card;

CREATE SPECIFIC METHOD ddlgen.playing_card
FOR ddlgen.playing_card
BEGIN
    set self.card_suit = card_suit_init; set
    self.card_rank = card_rank_init; return self
; END;

CREATE FUNCTION ddlgen.compare_cards_spades_trump(
  c1 ddlgen.playing_card,
  c2 ddlgen.playing_card)
RETURNS INTEGER
SPECIFIC compare_cards_spades_trump
CONTAINS SQL
DETERMINISTIC
RETURN case
when c1.card_suit='S' and c2.card_suit<>'S' then 1
when c2.card_suit='S' and c1.card_suit<>'S' then -1
when c1.card_rank > c2.card_rank then 1
when c1.card_rank < c2.card_rank then -1
else 0
end;

CREATE ORDERING FOR ddlgen.playing_card
ORDER FULL BY RELATIVE
WITH SPECIFIC FUNCTION ddlgen.compare_cards_spades_trump;

CREATE FUNCTION ddlgen.self(c CURSOR)
RETURNS TABLE(c.*)
LANGUAGE JAVA
PARAMETER STYLE SYSTEM DEFINED JAVA
NO SQL
EXTERNAL NAME 'class net.sf.farrago.test.FarragoTestUDR.self';

CREATE FUNCTION ddlgen.self_passthrough(c CURSOR)
RETURNS TABLE(c.* PASSTHROUGH)
LANGUAGE JAVA
PARAMETER STYLE SYSTEM DEFINED JAVA
NO SQL
EXTERNAL NAME 'class net.sf.farrago.test.FarragoTestUDR.self';

CREATE FUNCTION ddlgen.stringify_columns(
    c CURSOR,
    cl SELECT FROM c,
    delimiter VARCHAR(128))
RETURNS TABLE(v VARCHAR(65535))
LANGUAGE JAVA
PARAMETER STYLE SYSTEM DEFINED JAVA
NO SQL
EXTERNAL NAME 'class net.sf.farrago.test.FarragoTestUDR.stringifyColumns';

-- End ddlgen.sql
