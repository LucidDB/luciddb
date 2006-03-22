--
-- viload.sql - load and index 99 rows of a set query type table
--

create foreign data wrapper orcl_jdbc
library '/home/elin/open/luciddb/plugin/FarragoMedJdbc3p.jar'
language java;

create server orcl_server
foreign data wrapper orcl_jdbc
options(
    url 'jdbc:oracle:thin:@akela.lucidera.com:1521:XE',
    user_name 'schoi',
    password 'schoi',
    driver_class 'oracle.jdbc.driver.OracleDriver'
);

create schema orcl_schema;
set schema 'orcl_schema';

create foreign table BENCH_SOURCE
server orcl_server
options (
SCHEMA_NAME 'SCHOI',
table_name 'bench100'
)
;

create schema s;
set schema 's';

create table bench1M (kseq integer, k4 integer, k10 integer, k25 integer);

create index Kseq_idx on bench1M (kseq);

create index K4_idx on bench1M (k4);

create index K10_idx on bench1M (k10);

create index K25_idx on bench1M (k25);

insert into bench1m (kseq,k4,k10,k25) select "kseq","k4","k10","k25" from orcl_schema.bench_source;
