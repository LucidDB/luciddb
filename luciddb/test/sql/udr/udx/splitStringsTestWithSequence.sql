-- test methods from SplitStringUdx with START_NUM=NULL
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
    FALSE,
    CAST(NULL as bigint),
    CAST(NULL as bigint))
);

-- strip whitespace
select * from table(
  applib.split_rows(
    cursor(select COL2 from T1),
    '~',
    '\',
    TRUE,
    CAST(NULL as bigint),
    CAST(NULL as bigint))
);

create or replace view V1 as (select * from table(
  applib.split_rows(
    cursor(select * from T1),
    row(COL2),
    '~',
    '\',
    FALSE,
    CAST(NULL as bigint),
    CAST(NULL as bigint))
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
    TRUE,
    CAST(NULL as bigint),
    CAST(NULL as bigint))
);

select * from table(
  applib.split_string_to_rows(
    'AA~BB~CC~ ~~ ',
    '~',
    '\',
    TRUE,
    CAST(NULL as bigint),
    CAST(NULL as bigint))
);

select * from table(
  applib.split_string_to_rows(
    '~~  ~ AA~BB~CC',
    '~',
    '\',
    TRUE,
    CAST(NULL as bigint),
    CAST(NULL as bigint))
);

-- test with whitespace-only string input
select * from table(
  applib.split_string_to_rows(
    '  ',
    '~',
    '\',
    TRUE,
    CAST(NULL as bigint),
    CAST(NULL as bigint))
);

select * from table(
  applib.split_string_to_rows(
    '  ',
    '~',
    '\',
    FALSE,
    CAST(NULL as bigint),
    CAST(NULL as bigint))
);

-- test with empty input
select * from table(
  applib.split_string_to_rows(
    '',
    '~',
    '\',
    FALSE,
    CAST(NULL as bigint),
    CAST(NULL as bigint))
);

select * from table(
  applib.split_string_to_rows(
    '',
    '~',
    '\',
    TRUE,
    CAST(NULL as bigint),
    CAST(NULL as bigint))
);

-- test with null input to split_string_to_rows
call applib.create_var('splittest', null, 'xyz');
call applib.set_var('splittest', 'var', null);
select * from table(applib.split_string_to_rows(applib.get_var('splittest', 'var'), ',', '!',TRUE, CAST(NULL as bigint),
CAST(NULL as bigint)));
-- test with null input to split_rows
create table status (c1 varchar(10));
insert into status values (null);
select * from status;
select * from table(
  applib.split_rows(cursor(select c1 from status),'~','!',TRUE,0,1));
select count(*) from table(
  applib.split_rows(cursor(select c1 from status),'~','!',TRUE,0,1)) where c1 is null;
insert into status values (null);
select * from status;
select * from table(
  applib.split_rows(cursor(select c1 from status),'~','!',TRUE,0,1));
select count(*) from table(
  applib.split_rows(cursor(select c1 from status),'~','!',TRUE,0,1)) where c1 is null;
insert into status values (null);
select * from status;
select * from table(
  applib.split_rows(cursor(select c1 from status),'~','!',TRUE,0,1));
select count(*) from table(
  applib.split_rows(cursor(select c1 from status),'~','!',TRUE,0,1)) where c1 is null;
insert into status values ('open');
select * from status;
select * from table(
  applib.split_rows(cursor(select c1 from status),'~','!',TRUE,0,1));
select count(*) from table(
  applib.split_rows(cursor(select c1 from status),'~','!',TRUE,0,1)) where c1 is null;
insert into status values ('won~lost');
select * from status;
select * from table(
  applib.split_rows(cursor(select c1 from status),'~','!',TRUE,0,1));
select count(*) from table(
  applib.split_rows(cursor(select c1 from status),'~','!',TRUE,0,1)) where c1 is null;
insert into status values (null);
select * from status;
select * from table(
  applib.split_rows(cursor(select c1 from status),'~','!',TRUE,0,1));
select count(*) from table(
  applib.split_rows(cursor(select c1 from status),'~','!',TRUE,0,1)) where c1 is null;
delete from status where c1 is null;
insert into status values (null),(null);
select * from status;
select * from table(
  applib.split_rows(cursor(select c1 from status),'~','!',TRUE,0,1));
select count(*) from table(
  applib.split_rows(cursor(select c1 from status),'~','!',TRUE,0,1)) where c1 is null;

create table status1 (c1 varchar(10), c2 varchar(10));
insert into status1 values (null,null);
select * from table(
  applib.split_rows(cursor(select c1,c2 from status1),row(c1),'~','!',TRUE,1,1));
select count(*) from table(
  applib.split_rows(cursor(select c1,c2 from status1),row(c1),'~','!',TRUE,1,1)) where c1 is null;

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
    FALSE,
    CAST(NULL as bigint),
    CAST(NULL as bigint))
);

-- test exception when more than one col as input in singlecol mode
select * from table(
  applib.split_rows(
    cursor(select * from T1),
    '~',
    '\',
    FALSE,
    CAST(NULL as bigint),
    CAST(NULL as bigint))
);

drop schema SPLITTEST cascade;

-- test methods from SplitStringUdx with START_NUM=1337
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
    FALSE,
    1337, 2)
);

-- strip whitespace
select * from table(
  applib.split_rows(
    cursor(select COL2 from T1),
    '~',
    '\',
    TRUE,
    1337, 2)
);

create or replace view V1 as (select * from table(
  applib.split_rows(
    cursor(select * from T1),
    row(COL2),
    '~',
    '\',
    FALSE,
    1337, 2)
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
    TRUE,
    1337, 2)
);

select * from table(
  applib.split_string_to_rows(
    'AA~BB~CC~ ~~ ',
    '~',
    '\',
    TRUE,
    1337, 2)
);

select * from table(
  applib.split_string_to_rows(
    '~~  ~ AA~BB~CC',
    '~',
    '\',
    TRUE,
    1337, 2)
);

-- test with whitespace-only string input
select * from table(
  applib.split_string_to_rows(
    '  ',
    '~',
    '\',
    TRUE,
    1337, 2)
);

select * from table(
  applib.split_string_to_rows(
    '  ',
    '~',
    '\',
    FALSE,
    1337, 2)
);

-- test with empty input
select * from table(
  applib.split_string_to_rows(
    '',
    '~',
    '\',
    FALSE,
    1337, 2)
);

select * from table(
  applib.split_string_to_rows(
    '',
    '~',
    '\',
    TRUE,
    1337, 2)
);

-- test with null input
call applib.create_var('splittest', null, 'xyz');
call applib.set_var('splittest', 'var', null);
select * from table(applib.split_string_to_rows(applib.get_var('splittest', 'var'), ',', '!',TRUE, 1337, 2));


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
    FALSE,
    1337, 2)
);

-- test exception when more than one col as input in singlecol mode
select * from table(
  applib.split_rows(
    cursor(select * from T1),
    '~',
    '\',
    FALSE,
    1337, 2)
);

-- test overflowing numbers, should wrap around
select * from table(
  applib.split_rows(
    cursor(select COL2 from T1),
    '~',
    '\',
    FALSE,
    9223372036854775805, 1)
);

-- test missing increment, should say "no match found for function signature..."
select * from table(
  applib.split_rows(
    cursor(select COL2 from T1),
    '~',
    '\',
    FALSE,
    1)
);

-- test negative start_num, should be -3, -2, -1, etc.
select * from table(
  applib.split_rows(
    cursor(select COL2 from T1),
    '~',
    '\',
    FALSE,
    -3, 1)
);

-- test negative start_num with negative increment, should be -3, -4, -5, etc.
select * from table(
  applib.split_rows(
    cursor(select COL2 from T1),
    '~',
    '\',
    FALSE,
    -3, -1)
);

-- test single string, NULL separator (should return empty)
select * from table(
  applib.split_string_to_rows(
    'abc~def',
    CAST(NULL as CHAR(1)),
    '\',
    TRUE,
    1, 1)
);

-- test single string, NULL escape (should return empty)
select * from table(
  applib.split_string_to_rows(
    'abc~def',
    '~',
    CAST(NULL as CHAR(1)),
    TRUE,
    1, 1)
);

-- test single column, NULL separator (should return empty)
select * from table(
  applib.split_rows(
    cursor(select COL2 from T1),
    CAST(NULL as CHAR(1)),
    '\',
    TRUE,
    1, 1)
);

-- test single column, NULL escape (should return empty)
select * from table(
  applib.split_rows(
    cursor(select COL2 from T1),
    '~',
    CAST(NULL as CHAR(1)),
    TRUE,
    1, 1)
);

-- test exception for 0 increment, single string
select * from table(
  applib.split_string_to_rows(
    'abc~def',
    '~',
    '\',
    TRUE,
    5000, 0)
);

-- test exception for 0 increment, single column
select * from table(
  applib.split_rows(
    cursor(select COL2 from T1),
    '~',
    '\',
    TRUE,
    -5000, 0)
);

-- test scalar subquery for START_NUM (should be two rows: 60, 70)
select * from table(
  applib.split_string_to_rows(
    'abc~gg',
    '~',
    '\',
    TRUE,
    (select MAX(COL1)+10 from T1),
    10)
);

-- test error scalar query returned more than one row
select * from table(
  applib.split_string_to_rows(
    'abc~gg',
    '~',
    '\',
    TRUE,
    (select COL1 from T1),
    10)
);

-- test empty subquery for START_NUM (should be the same as NULL, i.e. 1)
select * from table(
  applib.split_rows(
    cursor(select COL2 from T1),
    '~',
    '\',
    TRUE,
    (select 10 from table(applib.split_string_to_rows('','~','\',TRUE))),
    12)
);

-- test same table for input columns and subquery
select * from table(
  applib.split_rows(
    cursor(select * from T1),
    row(COL2),
    '~',
    '\',
    TRUE,
    (select max(COL1)+10 from T1),
    10)
);


drop schema SPLITTEST cascade;
