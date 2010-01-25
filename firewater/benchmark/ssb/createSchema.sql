create schema ssb;

create table ssb.customer(
c_custkey     integer primary key,
c_name        varchar(25) not null,
c_address     varchar(40) not null,
c_city        varchar(10) not null,
c_nation      varchar(15) not null,
c_region      varchar(12) not null,
c_phone       varchar(15) not null,
c_mktsegment  varchar(10) not null);

create table ssb.dates(
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
d_weekdayfl        integer);

create table ssb.part(
p_partkey     integer primary key,
p_name        varchar(22) not null,
p_mfgr        varchar(6) not null,
p_category    varchar(7) not null,
p_brand       varchar(9) not null,
p_color       varchar(11) not null,
p_type        varchar(25) not null,
p_size        integer not null,
p_container   varchar(10) not null);

create table ssb.supplier(
s_suppkey     integer primary key,
s_name        varchar(25) not null,
s_address     varchar(25) not null,
s_city        varchar(10) not null,
s_nation      varchar(15) not null,
s_region      varchar(12) not null,
s_phone       varchar(15) not null);

create table ssb.lineorder(
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
primary key(lo_orderkey, lo_linenumber));

create index lo_custkey_idx on ssb.lineorder(lo_custkey);
create index lo_partkey_idx on ssb.lineorder(lo_partkey);
create index lo_suppkey_idx on ssb.lineorder(lo_suppkey);
create index lo_orderdate_idx on ssb.lineorder(lo_orderdate);
create index lo_commitdate_idx on ssb.lineorder(lo_commitdate);
create index c_region_idx on ssb.customer(c_region);
create index c_naiton_idx on ssb.customer(c_nation);
create index c_city_idx on ssb.customer(c_city);
create index d_year_idx on ssb.dates(d_year);
create index d_yearmonthnum_idx on ssb.dates(d_yearmonthnum);
create index d_weeknuminyear_idx on ssb.dates(d_weeknuminyear);
create index d_yearmonth_idx on ssb.dates(d_yearmonth);
create index p_category_idx on ssb.part(p_brand);
create index p_brand_idx on ssb.part(p_brand);
create index p_mfgr_idx on ssb.part(p_brand);
create index s_region_idx on ssb.supplier(s_region);
create index s_naiton_idx on ssb.supplier(s_nation);
create index s_city_idx on ssb.supplier(s_city);
create index lo_quantity_idx on ssb.lineorder(lo_quantity);
create index lo_discount_idx on ssb.lineorder(lo_discount);

create server ssb_file_server
foreign data wrapper sys_file_wrapper
options(
    directory 'benchmark/ssb/flatfiles',
    file_extension '.tbl',
    ctrl_file_extension '.bcp',
    field_delimiter '|',
    line_delimiter '\n',
    quote_char '"',
    escape_char '',
    with_header 'no',
    num_rows_scan '1'
);

create schema ssb_files;

import foreign schema "DEFAULT"
from server ssb_file_server
into ssb_files;
