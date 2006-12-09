-- set up a schema and a table
create schema DERIVEEFFECTIVE;
set schema 'DERIVEEFFECTIVE';
create table T1(id varchar(255), effective_from_timestamp timestamp);

-- try with an empty input query. should still behave civilized.
select * from table(
    applib.derive_effective_to_timestamp(
        cursor(SELECT id, effective_from_timestamp FROM T1 ORDER BY id, effective_from_timestamp),
        365,
        'YEAR'
    )
);

-- insert a row, try with only one input row. should have "to" date as NULL
insert into T1 values ('USD~GBP~Corporate', CAST('2006-12-01 12:00:00' AS TIMESTAMP));
select * from table(
    applib.derive_effective_to_timestamp(
        cursor(SELECT id, effective_from_timestamp FROM T1 ORDER BY id, effective_from_timestamp),
        365,
        'MONTH'
    )
);

-- insert another row but with different ID. both rows should get "to" date as NULL
insert into T1 values ('USD~JPY~Corporate', CAST('2006-01-01 12:00:00' AS TIMESTAMP));
select * from table(
    applib.derive_effective_to_timestamp(
        cursor(SELECT id, effective_from_timestamp FROM T1 ORDER BY id, effective_from_timestamp),
        365,
        'WEEK'
    )
);

-- insert a bunch of rows, try different time units and counts
insert into T1 values ('USD~GBP~Corporate', CAST('2005-12-31 12:00:00' AS TIMESTAMP));
insert into T1 values ('USD~GBP~Corporate', CAST('2006-01-01 12:00:00' AS TIMESTAMP));
insert into T1 values ('USD~GBP~Corporate', CAST('2006-12-30 12:00:00' AS TIMESTAMP));
insert into T1 values ('USD~GBP~Corporate', CAST('2006-12-12 12:00:00' AS TIMESTAMP));
insert into T1 values ('USD~KRW~Corporate', CAST('2005-12-31 12:00:00' AS TIMESTAMP));
insert into T1 values ('USD~KRW~Corporate', CAST('2005-12-31 13:00:00' AS TIMESTAMP));

select * from table(
    applib.derive_effective_to_timestamp(
        cursor(SELECT id, effective_from_timestamp FROM T1 ORDER BY id, effective_from_timestamp),
        60,
        'MINUTE'
    )
);

select * from table(
    applib.derive_effective_to_timestamp(
        cursor(SELECT id, effective_from_timestamp FROM T1 ORDER BY id, effective_from_timestamp),
        1,
        'SECOND'
    )
);


select * from table(
    applib.derive_effective_to_timestamp(
        cursor(SELECT id, effective_from_timestamp FROM T1 ORDER BY id, effective_from_timestamp),
        1,
        'MILLISECOND'
    )
);

-- try with more than two input columns. should give a nice error.
select * from table(
    applib.derive_effective_to_timestamp(
        cursor(SELECT id, effective_from_timestamp, id FROM T1 ORDER BY id, effective_from_timestamp),
        365,
        'YEAR'
    )
);

-- try use a bad time unit string. should give a nice error.
select * from table(
    applib.derive_effective_to_timestamp(
        cursor(SELECT id, effective_from_timestamp FROM T1 ORDER BY id, effective_from_timestamp),
        365,
        'BLAH'
    )
);

-- try with either input column out of order. should give nice errors.
select * from table(
    applib.derive_effective_to_timestamp(
        cursor(SELECT id, effective_from_timestamp FROM T1 ORDER BY id),
        365,
        'SECOND'
    )
);

select * from table(
    applib.derive_effective_to_timestamp(
        cursor(SELECT id, effective_from_timestamp FROM T1 ORDER BY effective_from_timestamp),
        365,
        'SECOND'
    )
);

drop schema DERIVEEFFECTIVE cascade;
