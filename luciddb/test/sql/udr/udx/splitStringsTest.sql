-- test SplitStringUdx
create schema SPLITTEST;
set schema 'SPLITTEST';
create table "T1" (COL1 integer, COL2 varchar(255), COL3 varchar(255));
insert into T1 (COL1, COL2, COL3) values (10, 'AA~BB~ ', 'b!ah');
insert into T1 (COL1, COL2, COL3) values (20, '~', 'bl~~h');
insert into T1 (COL1, COL2, COL3) values (30, '\~', 'bl~~h');
insert into T1 (COL1, COL2, COL3) values (40, 'CC~\~\~~\~DD', '~meh');
insert into T1 (COL1, COL2, COL3) values (50, 'EE ~ F\~F\F', '~heh');

-- don't strip whitespace
select * from table(
  applib.split_rows(
    cursor(select COL2 from T1),
    '~',
    '\',
    FALSE)
);

-- strip whitespace
select * from table(
  applib.split_rows(
    cursor(select COL2 from T1),
    '~',
    '\',
    TRUE)
);

create or replace view V1 as (select * from table(
  applib.split_rows(
    cursor(select * from T1),
    row(COL2),
    '~',
    '\',
    FALSE)
));

-- should have two rows with trailing space
select * from V1 where COL2 like '% ';

-- should have two rows with leading space
select * from V1 where COL2 like ' %';

-- following three should give the same output
select * from table(
  applib.split_string_to_rows(
    'AA~BB~CC',
    '~',
    '\',
    TRUE)
);

select * from table(
  applib.split_string_to_rows(
    'AA~BB~CC~ ~~ ',
    '~',
    '\',
    TRUE)
);

select * from table(
  applib.split_string_to_rows(
    '~~  ~ AA~BB~CC',
    '~',
    '\',
    TRUE)
);

-- test with whitespace-only string input
select * from table(
  applib.split_string_to_rows(
    '  ',
    '~',
    '\',
    TRUE)
);

select * from table(
  applib.split_string_to_rows(
    '  ',
    '~',
    '\',
    FALSE)
);

-- test with empty input
select * from table(
  applib.split_string_to_rows(
    '',
    '~',
    '\',
    FALSE)
);

select * from table(
  applib.split_string_to_rows(
    '',
    '~',
    '\',
    TRUE)
);

-- test with null input
call applib.create_var('splittest', null, 'xyz');
call applib.set_var('splittest', 'var', null);
select * from table(applib.split_string_to_rows(applib.get_var('splittest', 'var'), ',', '!',TRUE));


-- test exception when more than one escape char
-- enable when LER-4686 is fixed
--select * from table(
--  applib.split_string_to_rows(
--    '~~  ~ AA~BB~CC',
--    '~',
--    '\a',
--    TRUE)
--);

-- test exception when more than one separator char
-- enable when LER-4686 is fixed
--select * from table(
--  applib.split_string_to_rows(
--    '~~  ~ AA~BB~CC',
--    '~a',
--    '\',
--    TRUE)
--);

-- test exception when more than one col to split in multicol mode
select * from table(
  applib.split_rows(
    cursor(select * from T1),
    row(COL2,COL3),
    '~',
    '\',
    FALSE)
);

-- test exception when more than one col as input in singlecol mode
select * from table(
  applib.split_rows(
    cursor(select * from T1),
    '~',
    '\',
    FALSE)
);

drop schema SPLITTEST cascade;
