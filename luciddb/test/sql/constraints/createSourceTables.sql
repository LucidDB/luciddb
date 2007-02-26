create schema uc_src;
set schema 'uc_src';

create foreign table nation_ft(
  nationkey varchar(20),
  name varchar(25),
  regionkey varchar(20),
  comment varchar(152)
) server ff_tpch
options (
schema_name 'BCP',
filename 'nation'
);

create table nation_src(
  nationkey varchar(20),
  name varchar(25),
  regionkey varchar(20),
  comment varchar(152)
);

create table part_src(
  p_partkey integer,
  p_name varchar(55),
  p_mfgr varchar(25),
  p_brand varchar(10),
  p_type varchar(25),
  p_size integer,
  p_container varchar(10),
  p_retailprice decimal(15,2),
  p_comment varchar(23)
);

create table bench1m_src(
  kseq integer,
  k2 integer,
  k4 integer,
  k5 integer,
  k10 integer,
  k25 integer,
  k100 integer,
  k1k integer,
  k10k integer,
  k40k integer,
  k100k integer,
  k250k integer,
  k500k integer
);

-- create indexes
create index b1m_k100k_idx on bench1m_src(k10k);
create index b1m_k500k_idx on bench1m_src(k250k);