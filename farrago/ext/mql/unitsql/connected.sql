-- $Id$
-- Test MQL foreign data wrapper, including the parts
-- that actually access the web service
-- (so this should not run as part of checkin acceptance tests).
-- This test may fail if the Freebase contents have been
-- updated since the last time the .ref file was updated.

create schema metaweb;

create or replace foreign data wrapper mql_wrapper
library '${FARRAGO_HOME}/ext/mql/plugin/farrago-mql.jar'
language java;

create or replace server mql_server
foreign data wrapper mql_wrapper;

create or replace jar metaweb.mql_jar
library 'file:${FARRAGO_HOME}/ext/mql/plugin/farrago-mql.jar'
options(0);

create or replace function metaweb.mql_query(
  url varchar(4096),
  mql varchar(65535), 
  row_type varchar(65535))
returns table(
  objects varchar(128))
language java
parameter style system defined java
dynamic_function
no sql
external name 'metaweb.mql_jar:net.sf.farrago.namespace.mql.MedMqlUdx.execute';

create or replace foreign table metaweb.artists(
    "name" varchar(128), "id" varchar(128))
server mql_server
options (metaweb_type '/music/artist');

select * from metaweb.artists order by "name";

select "id" from metaweb.artists order by "id";

select "name" from metaweb.artists 
where "id"='/en/gene_kelly';

select * from metaweb.artists 
where "id"='/en/gene_kelly';

select count(*) from metaweb.artists;
