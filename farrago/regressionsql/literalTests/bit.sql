-- $Id$ 

-- test bit literal
-- These test pass the system but are not displayed correctly in sqlline engine.
-- select B'10' as t1 from values ('true');
-- select B'00000000000' as t1 from values ('true');
-- select B'11011000000' as t1 from values ('true');
-- select B'01010101010' as t1 from values ('true');

create schema test;
set schema test;
create table t_bit(bit_col bit not null primary key);

-- bug fix me and uncomment the following:
--insert into t_bit values(B'10');

drop table t_bit;

