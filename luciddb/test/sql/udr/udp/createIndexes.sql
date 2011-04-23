create schema idx_cand_test;
set schema 'idx_cand_test';
create table t1 (
  c1 int,
  c2 int,
  c3 int
);

insert into t1 values (1, 1, 1), (2, 2, 2), (3, 3, 3), (4, 4, 3), (5, 4, 3);

select * from table(applib.show_idx_candidates('IDX_CAND_TEST', 'T1', 90));
call applib.create_indexes('select * from table(applib.show_idx_candidates(''IDX_CAND_TEST'', ''T1'', 90))');
-- empty set test
call applib.create_indexes('select * from table(applib.show_idx_candidates(''IDX_CAND_TEST'', ''T1'', 90))');
select * from table(applib.show_idx_candidates('IDX_CAND_TEST', 'T1', 90));
select index_name from sys_root.dba_unclustered_indexes
  where schema_name = 'IDX_CAND_TEST' and table_name = 'T1';

drop schema idx_cand_test cascade;
