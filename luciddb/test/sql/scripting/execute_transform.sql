-- do our own generate_sequence
create schema hello;
set schema 'hello';
create table firing (step varchar(100), dirty_seq int, relative_seq int);
insert into firing values ('Ready', 0, 2), ('Aim', 0, 0), ('Fire', 0, 1),
('Drag', 1, 0), ('Bury', 2, 0), ('Kill', 0, 1);

select * from table(
  applib.generate_sequence(
    cursor(select step, dirty_seq, relative_seq from firing),
    0, 1));

select step, dirty_seq, relative_seq, expr$0 as seq_num
from table(applib.execute_transform('js',
    '${FARRAGO_HOME}/test/sql/scripting/gen_sequence.js',
    cursor(select * from firing, (values(0)))));
