set schema 's';

-- BUG3680
-- create big concat index.  This should cause some new node blocks to be 
-- created 
--set plan=BOTH

!set outputformat csv

create index B1M_KSEQ_K500K_K250K on bench1M(KSEQ, K500K, K250K)
;
drop index B1M_K500K_IDX
;
drop index B1M_K10_IDX
;

explain plan excluding attributes for
select count(*) from bench1M where K10 = 2;

select count(*) from bench1M where K10 = 2
;
create index B1M_K10_IDX on bench1M(K10)
;

explain plan excluding attributes for
select count(*) from bench1M where K10 = 2;

select count(*) from bench1M where K10 = 2
;

drop index B1M_K5_IDX
;

explain plan excluding attributes for
select count(*) from bench1M where K5 = 2;

select count(*) from bench1M where K5 = 2
;
create index B1M_K5_IDX on bench1M(K5)
;

explain plan excluding attributes for
select count(*) from bench1M where K5 = 2;

select count(*) from bench1M where K5 = 2
;

drop index B1M_K25_IDX
;

explain plan excluding attributes for
select count(*) from bench1M where K25 = 2;

select count(*) from bench1M where K25 = 2
;
create index B1M_K25_IDX on bench1M(K25)
;

explain plan excluding attributes for
select count(*) from bench1M where K25 = 2;

select count(*) from bench1M where K25 = 2
;

drop table bench1m
;
--select tablespace_name, total_blocks, free_blocks from dba_tablespaces
--where tablespace_name = 'TBB_BENCH_TS'
--;


!set outputformat table