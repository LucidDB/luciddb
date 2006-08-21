-- create local tables from flatfile server
-- to be used for other tests

create schema s;
set schema 's';

create table BENCH100 (
"kseq" bigint primary key,
"k2" bigint,
"k4" bigint,
"k5" bigint,
"k10" bigint,
"k25" bigint,
"k100" bigint,
"k1k" bigint,
"k10k" bigint,
"k40k" bigint,
"k100k" bigint,
"k250k" bigint,
"k500k" bigint);

create table BENCH10K (
"kseq" bigint primary key,
"k2" bigint,
"k4" bigint,
"k5" bigint,
"k10" bigint,
"k25" bigint,
"k100" bigint,
"k1k" bigint,
"k10k" bigint,
"k40k" bigint,
"k100k" bigint,
"k250k" bigint,
"k500k" bigint);

create table BENCH100K (
"kseq" bigint primary key,
"k2" bigint,
"k4" bigint,
"k5" bigint,
"k10" bigint,
"k25" bigint,
"k100" bigint,
"k1k" bigint,
"k10k" bigint,
"k40k" bigint,
"k100k" bigint,
"k250k" bigint,
"k500k" bigint);

create table BENCH1M (
"kseq" bigint primary key,
"k2" bigint,
"k4" bigint,
"k5" bigint,
"k10" bigint,
"k25" bigint,
"k100" bigint,
"k1k" bigint,
"k10k" bigint,
"k40k" bigint,
"k100k" bigint,
"k250k" bigint,
"k500k" bigint);



INSERT INTO s.BENCH100
SELECT "kseq","k2","k4","k5","k10","k25","k100","k1k","k10k","k40k","k100k","k250k","k500k"
FROM ff_server.BCP."bench100";

INSERT INTO s.BENCH10K
SELECT "kseq","k2","k4","k5","k10","k25","k100","k1k","k10k","k40k","k100k","k250k","k500k"
FROM ff_server.BCP."bench10K";


INSERT INTO s.BENCH100K
SELECT "kseq","k2","k4","k5","k10","k25","k100","k1k","k10k","k40k","k100k","k250k","k500k"
FROM ff_server.BCP."bench100K";


INSERT INTO s.BENCH1M
SELECT "kseq","k2","k4","k5","k10","k25","k100","k1k","k10k","k40k","k100k","k250k","k500k"
FROM ff_server.BCP."bench1M";



analyze table bench100 compute statistics for all columns;
analyze table bench10k compute statistics for all columns;
analyze table bench100k compute statistics for all columns;
analyze table bench1m compute statistics for all columns;

