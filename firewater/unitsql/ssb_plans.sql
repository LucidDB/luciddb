-- $Id$
-- Tests for Firewater query optimization of the Star Schema Benchmark

create or replace server fakeremote_server
foreign data wrapper sys_firewater_fakeremote_wrapper
options (user_name 'sa');

create partition qp1 on (fakeremote_server);

create partition qp2 on (fakeremote_server);

create schema ssb;
set schema 'ssb';

create table customer(
c_custkey     integer primary key,
c_name        varchar(25) not null,
c_address     varchar(40) not null,
c_city        varchar(10) not null,
c_nation      varchar(15) not null,
c_region      varchar(12) not null,
c_phone       varchar(15) not null,
c_mktsegment  varchar(10) not null)
options (partitioning 'NONE');

create table dates(
d_datekey          integer primary key,
d_date             varchar(18) not null,
d_dayofweek        varchar(8) not null,
d_month            varchar(9) not null,
d_year             integer not null,
d_yearmonthnum     integer,
d_yearmonth        varchar(7) not null,
d_daynuminweek     integer,
d_daynuminmonth    integer,
d_daynuminyear     integer,
d_monthnuminyear   integer,
d_weeknuminyear    integer,
d_sellingseason    varchar(12) not null,
d_lastdayinweekfl  integer,
d_lastdayinmonthfl integer,
d_holidayfl        integer,
d_weekdayfl        integer)
options (partitioning 'NONE');

create table part(
p_partkey     integer primary key,
p_name        varchar(22) not null,
p_mfgr        varchar(6) not null,
p_category    varchar(7) not null,
p_brand       varchar(9) not null,
p_color       varchar(11) not null,
p_type        varchar(25) not null,
p_size        integer not null,
p_container   varchar(10) not null)
options (partitioning 'NONE');

create table supplier(
s_suppkey     integer primary key,
s_name        varchar(25) not null,
s_address     varchar(25) not null,
s_city        varchar(10) not null,
s_nation      varchar(15) not null,
s_region      varchar(12) not null,
s_phone       varchar(15) not null)
options (partitioning 'NONE');

create table lineorder(
lo_orderkey       integer,
lo_linenumber     integer,
lo_custkey        integer not null,
lo_partkey        integer not null,
lo_suppkey        integer not null,
lo_orderdate      integer not null,
lo_orderpriotity  varchar(15) not null,
lo_shippriotity   integer,
lo_quantity       integer,
lo_extendedprice  integer,
lo_ordtotalprice  integer,
lo_discount       integer,
lo_revenue        integer,
lo_supplycost     integer,
lo_tax            integer,
lo_commitdate     integer not null,
lo_shipmode       varchar(10) not null,
primary key(lo_orderkey, lo_linenumber))
options (partitioning 'HASH');

!set outputformat csv

-- 1.sql
explain plan for
select 
    sum(lo_extendedprice*lo_discount) as revenue
from 
     lineorder, dates
where 
    lo_orderdate = d_datekey
    and d_year = 1993
    and lo_discount between 1 and 3
    and lo_quantity < 25;

-- 2.sql
explain plan for
select 
    sum(lo_extendedprice*lo_discount) as revenue
from 
    lineorder, dates
where 
    lo_orderdate = d_datekey
    and d_yearmonthnum = 199401
    and lo_discount between 4 and 6
    and lo_quantity between 26 and 35;

-- 3.sql
explain plan for
select 
    sum(lo_extendedprice*lo_discount) as revenue
from 
    lineorder, dates
where 
    lo_orderdate = d_datekey
    and d_weeknuminyear = 6
    and d_year = 1994
    and lo_discount between 5 and 7
    and lo_quantity between 26 and 35;

-- 4.sql
explain plan for
select 
    sum(lo_revenue), d_year, p_brand
from 
    lineorder, dates, part, supplier
where 
    lo_orderdate = d_datekey
    and lo_partkey = p_partkey
    and lo_suppkey = s_suppkey
    and p_category = 'MFGR#12'
    and s_region = 'AMERICA'
group by 
    d_year, p_brand
order by 
    d_year, p_brand;

-- 5.sql
explain plan for
select 
    sum(lo_revenue), d_year, p_brand
from 
    lineorder, dates, part, supplier
where 
    lo_orderdate = d_datekey
    and lo_partkey = p_partkey
    and lo_suppkey = s_suppkey
    and p_brand between 'MFGR#2221' and 'MFGR#2228'
    and s_region = 'ASIA'
group by 
    d_year, p_brand
order by 
    d_year, p_brand;

-- 6.sql
explain plan for
select 
    sum(lo_revenue), d_year, p_brand
from 
    lineorder, dates, part, supplier
where 
    lo_orderdate = d_datekey
    and lo_partkey = p_partkey
    and lo_suppkey = s_suppkey
    and p_brand = 'MFGR#2239'
    and s_region = 'EUROPE'
group by 
    d_year, p_brand
order by 
    d_year, p_brand;

--7.sql
explain plan for
select 
    c_nation, s_nation, d_year,
    sum(lo_revenue) as revenue
from 
    customer, lineorder, supplier, dates
where 
    lo_custkey = c_custkey
    and lo_suppkey = s_suppkey
    and lo_orderdate = d_datekey
    and c_region = 'ASIA'
    and s_region = 'ASIA'
    and d_year >= 1992 and d_year <= 1997
group by 
    c_nation, s_nation, d_year
order by 
    d_year asc, revenue desc;

-- 8.sql
explain plan for
select 
    c_city, s_city, d_year, sum(lo_revenue) as revenue
from 
    customer, lineorder, supplier, dates
where 
    lo_custkey = c_custkey
    and lo_suppkey = s_suppkey
    and lo_orderdate = d_datekey
    and c_nation = 'UNITED STATES'
    and s_nation = 'UNITED STATES'
    and d_year >= 1992 and d_year <= 1997
group by 
    c_city, s_city, d_year
order by 
    d_year asc, revenue desc;

-- 9.sql
explain plan for
select 
    c_city, s_city, d_year, sum(lo_revenue) as revenue
from 
    customer, lineorder, supplier, dates
where 
    lo_custkey = c_custkey
    and lo_suppkey = s_suppkey
    and lo_orderdate = d_datekey
    and (c_city='UNITED KI1' or c_city='UNITED KI5')
    and (s_city='UNITED KI1' or s_city='UNITED KI5')
    and d_year >= 1992 and d_year <= 1997
group by 
    c_city, s_city, d_year
order by 
    d_year asc, revenue desc;

-- 10.sql
explain plan for
select 
    c_city, s_city, d_year, sum(lo_revenue) as revenue
from 
    customer, lineorder, supplier, dates
where 
    lo_custkey = c_custkey
    and lo_suppkey = s_suppkey
    and lo_orderdate = d_datekey
    and (c_city='UNITED KI1' or c_city='UNITED KI5')
    and (s_city='UNITED KI1' or s_city='UNITED KI5')
    and d_yearmonth = 'Dec1997'
group by 
    c_city, s_city, d_year
order by 
    d_year asc, revenue desc;

-- 11.sql
explain plan for
select 
    d_year, c_nation,
    sum(lo_revenue - lo_supplycost) as profit
from 
    dates, customer, supplier, part, lineorder
where 
    lo_custkey = c_custkey
    and lo_suppkey = s_suppkey
    and lo_partkey = p_partkey
    and lo_orderdate = d_datekey
    and c_region = 'AMERICA'
    and s_region = 'AMERICA'
    and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2')
group by 
    d_year, c_nation
order by 
    d_year, c_nation;

-- 12.sql
explain plan for
select 
    d_year, s_nation, p_category,
    sum(lo_revenue - lo_supplycost) as profit
from 
    dates, customer, supplier, part, lineorder
where 
    lo_custkey = c_custkey
    and lo_suppkey = s_suppkey
    and lo_partkey = p_partkey
    and lo_orderdate = d_datekey
    and c_region = 'AMERICA'
    and s_region = 'AMERICA'
    and (d_year = 1997 or d_year = 1998)
    and (p_mfgr = 'MFGR#1' or p_mfgr = 'MFGR#2')
group by 
    d_year, s_nation, p_category
order by 
    d_year, s_nation, p_category;

-- 13.sql
explain plan for
select 
    d_year, s_city, p_brand,
    sum(lo_revenue - lo_supplycost) as profit
from 
    dates, customer, supplier, part, lineorder
where 
    lo_custkey = c_custkey
    and lo_suppkey = s_suppkey
    and lo_partkey = p_partkey
    and lo_orderdate = d_datekey
    and s_nation = 'UNITED STATES'
    and (d_year = 1997 or d_year = 1998)
    and p_category = 'MFGR#14'
group by 
    d_year, s_city, p_brand
order by 
    d_year, s_city, p_brand;
