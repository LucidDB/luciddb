----------------------------------------------------------------------

-- large data set from bench tables

create schema analyzetest;
set schema 'analyzetest';

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

-- populated from BENCH10K
create table BENCH1K (
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

create table CONCURRENT_SIM (
"kseq" bigint primary key,
"k100" bigint);
