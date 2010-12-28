-- test the replicate_mondrian UDP

-- create the source star schema

create schema mondrian_src;

create table mondrian_src.d1(
    d1_key int not null primary key,
    d1_attr varchar(200));

create table mondrian_src.d2(
    d2_key int not null primary key,
    d2_attr varchar(200));

create table mondrian_src.f(
    f_key int not null primary key,
    d1_key int not null,
    d2_key int not null,
    m1 numeric(18,3),
    m2 bigint);

-- create an extra table in the source table so we can verify that
-- it does NOT get replicated

create table mondrian_src.extra(
    k int not null primary key);

-- populate some data
insert into mondrian_src.d1 values (1, 'Parsley'), (2, 'Sage');
insert into mondrian_src.d2 values (1, 'Rosemary'), (2, 'Thyme');
insert into mondrian_src.f values (1, 1, 1, 5.2, 3), (2, 2, 1, 10.4, 7);

-- create a loopback link into the source schema
create server loopback_localdb
foreign data wrapper "LUCIDDB LOCAL"
options(
    url 'jdbc:luciddb:',
    user_name 'sa');

-- create the replication target schema
create schema mondrian_tgt;

call applib.replicate_mondrian(
'${FARRAGO_HOME}/../luciddb/test/sql/udr/udp/MondrianReplication.xml',
'LOOPBACK_LOCALDB',
'MONDRIAN_SRC',
'MONDRIAN_TGT',
'${FARRAGO_HOME}/../luciddb/testlog/mondrian_replication_script.sql',
true);

-- verify data copy

select * from mondrian_tgt.d1 order by d1_key;

select * from mondrian_tgt.d2 order by d2_key;

select * from mondrian_tgt.f order by f_key;

-- verify that unreferenced table did NOT get copied:  should fail
select * from mondrian_tgt.extra;

-- verify primary key creation:  should fail
insert into mondrian_tgt.d1 values (1, 'Fennel');

-- verify indexing

select index_name
from sys_boot.jdbc_metadata.index_info_view
where table_schem='MONDRIAN_TGT'
and table_name='D2'
order by index_name;

-- preemptive cleanup since loopback links are pesky
drop server loopback_localdb cascade;
call sys_root.flush_code_cache();
