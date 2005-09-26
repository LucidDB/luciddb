-- $Id$
-- Test casts between string types

-- REVIEW: SZ: 8/5/2004: This test wants to be in regressionsql, but
-- it only works under the Java Calc and regressionsql currently only
-- executes tests against the Fennel Calc.  See dtbug 228.

create schema s;
set schema 's';
create table t (
        i int not null primary key,
        short_c char(3) not null,
        med_c char(10) not null,
        long_c char(64) not null,
        short_vc varchar(3) not null,
        med_vc varchar(10) not null,
        long_vc varchar(64) not null);
insert into t values(1,
        'a',
        'a',
        'a',
        'a',
        'a',
        'a');
insert into t values(2,
        'ab',
        'ab',
        'ab',
        'ab',
        'ab',
        'ab');
insert into t values(3,
        'abc',
        'abc',
        'abc',
        'abc',
        'abc',
        'abc');
insert into t values(4,
        'abc',
        'abcd',
        'abcd',
        'abcd',
        'abcd',
        'abcd');
insert into t values(9,
        'abc',
        'abcdefghi',
        'abcdefghi',
        'abcdefghi',
        'abcdefghi',
        'abcdefghi');
insert into t values(10,
        'abc',
        'abcdefghij',
        'abcdefghij',
        'abcdefghij',
        'abcdefghij',
        'abcdefghij');
insert into t values(11,
        'abc',
        'abcdefghij',
        'abcdefghijk',
        'abcdefghijk',
        'abcdefghijk',
        'abcdefghijk');
insert into t values(63,
        'abc!',
        'abcdefghij',
        'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!',
        'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!',
        'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!',
        'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!');
insert into t values(64,
        'abc',
        'abcdefghij',
        'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@',
        'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@',
        'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@',
        'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@');

select i, char_length(long_vc) from t;

!set outputformat csv

-- cast smaller, same type
select cast(short_c as char(1)) from t;
select cast(med_c as char(5)) from t;
select cast(long_c as char(32)) from t;

select cast(short_vc as varchar(1)) from t;
select cast(med_vc as varchar(5)) from t;
select cast(long_vc as varchar(32)) from t;


-- cast larger, same type
-- Fails in Fennel Calc (pads with NULLs)
select cast(short_c as char(5)) from t;
-- Fails in Fennel Calc (pads with NULLs)
select cast(med_c as char(15)) from t;
-- Fails in Fennel Calc (pads with NULLs)
select cast(long_c as char(70)) from t;

select cast(short_vc as varchar(5)) from t;
select cast(med_vc as varchar(15)) from t;
select cast(long_vc as varchar(70)) from t;


-- cast same size, same type
select cast(short_c as char(3)) from t;
select cast(med_c as char(10)) from t;
select cast(long_c as char(64)) from t;

select cast(short_vc as varchar(3)) from t;
select cast(med_vc as varchar(10)) from t;
select cast(long_vc as varchar(64)) from t;


-- cast smaller, other type
select cast(short_c as varchar(1)) from t;
select cast(med_c as varchar(5)) from t;
select cast(long_c as varchar(32)) from t;

select cast(short_vc as char(1)) from t;
select cast(med_vc as char(5)) from t;
select cast(long_vc as char(32)) from t;


-- cast larger, other type
select cast(short_c as varchar(5)) from t;
select cast(med_c as varchar(15)) from t;
select cast(long_c as varchar(70)) from t;

select cast(short_vc as char(5)) from t;
select cast(med_vc as char(15)) from t;
select cast(long_vc as char(70)) from t;


-- cast same size, other type
select cast(short_c as varchar(3)) from t;
select cast(med_c as varchar(10)) from t;
select cast(long_c as varchar(64)) from t;

select cast(short_vc as char(3)) from t;
select cast(med_vc as char(10)) from t;
select cast(long_vc as char(64)) from t;
drop table t;
drop schema s;
