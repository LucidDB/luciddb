create schema hello;
set schema 'hello';
create table firing (step varchar(100), dirty_seq int, relative_seq int);
insert into firing values ('Ready', 0, 2), ('Aim', 0, 0), ('Fire', 0, 1),
('Drag', 1, 0), ('Bury', 2, 0), ('Kill', 0, 1);

-- Simple tests without partitioning
select * from table(
  applib.generate_sequence(
    cursor(select step, dirty_seq, relative_seq from firing),
    0, 1));

select * from table(
  applib.generate_sequence(
    cursor(select step, dirty_seq, relative_seq from firing),
    3, 2));
select * from table(
  applib.generate_sequence(
    cursor(select step, dirty_seq, relative_seq from firing),
    10, -1));

-- bigint overflow
select * from table(
  applib.generate_sequence(
    cursor(select step, dirty_seq, relative_seq from firing),
    9223372036854775805, 1));

-- zero increment
select * from table(
  applib.generate_sequence(
    cursor(select step, dirty_seq, relative_seq from firing),
    1, 0));

-- test null values
select * from table(
  applib.generate_sequence(
    cursor(select step, dirty_seq, relative_seq from firing),
    cast(null as bigint), 2));
select * from table(
  applib.generate_sequence(
    cursor(select step, dirty_seq, relative_seq from firing),
    3, cast(null as bigint)));

-- some basic partitioning now

-- out of order sorting:
select step, dirty_seq, seq_num as relative_seq from table(
  applib.generate_sequence_partitioned(
    cursor(select step, dirty_seq from firing),
    row(dirty_seq), 0, 1));

-- sorted

select * from table(applib.generate_sequence_partitioned(
    cursor(select * from firing order by dirty_seq),
    row(dirty_seq), 0, 1));

-- two keys
select * from table(applib.generate_sequence_partitioned(
    cursor(select * from firing order by dirty_seq, relative_seq),
    row(dirty_seq, relative_seq), 0, 1));


-- cleanup
drop table firing;
drop schema hello cascade;
