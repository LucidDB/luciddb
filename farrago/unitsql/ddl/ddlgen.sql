-- $Id$
-- Script which creates every kind of object, with all clauses.
-- Invoked from AspenDdlGeneratorTest.testCustomSchema,
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

-- aspen-specific objects follow this point

create driver node d1
 identified by '44444444-4444-4444-4444-444444444444'
 offline opening not updated peer link respawn holddown 5
 description 'bar';

create driver node D2 offline opening
 not updated peer link respawn holddown 5
 description 'with a multi-'
 'line description';

create processing node n2;

create processing node N1
 identified by 'deadbeef-cafe-abad-face-5ca1ed6e0de5'
 listener 'n1.domain.com' port 5410
 ipaddress '192.168.1.1' port 32000
 description 'a node';

create link between n1 and n2 unknown description 'node 1 to node 2';

create or replace foreign data wrapper pulse_w
    library 'class com.sqlstream.plugin.pulse.PulseControlPlugin'
    language java;

create or replace server pulse_s
 foreign data wrapper pulse_w
 description 'a foreign server';

create foreign stream pulse
    (seqno int not null,
     message varchar(32))
    server pulse_s
    options (
        nrows '10',
        rowtime_interval '100',
        default_string_output 'before')
   description 'a foreign stream';

CREATE STREAM x(x INTEGER, y VARCHAR(10) NOT NULL)
DESCRIPTION 'a stream';

CREATE VIEW vx
DESCRIPTION 'a stream view' AS
SELECT STREAM * FROM x WHERE x > 5;

CREATE STREAM y(x INTEGER, y VARCHAR(10) NOT NULL)
RESIDING ON n2;

create or replace pump pump1 stopped description 'pump1 is a trivial pump' as
    insert into ddlgen.y select stream * from ddlgen.vx where x < 4;

-- End ddlgen.sql
