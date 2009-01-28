-- $Id$
-- Test Unicode data

create schema uni;

-- should fail:  unknown character set
create table uni.t1(
i int not null primary key, v varchar(10) character set "SANSKRIT");

-- should fail:  valid SQL character set, but not supported by Farrago
create table uni.t2(
i int not null primary key, v varchar(10) character set "UTF32");

-- should fail:  valid Java character set, but not supported by Farrago
create table uni.t3(
i int not null primary key, v varchar(10) character set "UTF-8");

-- should succeed:  standard singlebyte
create table uni.t4(
i int not null primary key, v varchar(10) character set "ISO-8859-1");

-- should succeed:  alias for ISO-8859-1
create table uni.t5(
i int not null primary key, v varchar(10) character set "LATIN1");

insert into uni.t5 values (1, 'Hi');

select cast(v as varchar(1) character set "LATIN1") from uni.t5;

-- should succeed:  2-byte Unicode
create table uni.t6(
i int not null primary key, v varchar(10) character set "UTF16");

insert into uni.t6 values (1, _UTF16'Hi');

select * from uni.t6;

select cast(v as varchar(1) character set "UTF16") from uni.t6;

-- should fail:  unknown character set
select cast(v as varchar(1) character set "SANSKRIT") from uni.t6;

select cast(v as varchar(1) character set "UTF16") from uni.t5;

select cast(v as varchar(1) character set "LATIN1") from uni.t6;

select cast(v as char(40) character set "LATIN1") from uni.t5;

select cast(v as char(40) character set "UTF16") from uni.t6;

select cast(v as char(40) character set "UTF16") from uni.t5;

select cast(v as char(40) character set "LATIN1") from uni.t6;

select char_length(v) from uni.t5;

select char_length(v) from uni.t6;

select v||v from uni.t5;

select v||v from uni.t6;

select substring(v from 1 for 1) from uni.t5;

select substring(v from 1 for 1) from uni.t6;

select substring(v from 2 for 1) from uni.t5;

select substring(v from 2 for 1) from uni.t6;

select substring(v from 2) from uni.t5;

select substring(v from 2) from uni.t6;

select overlay(v placing 'a' from 2 for 1) from uni.t5;

-- should fail:  character set mismatch
select overlay(v placing _UTF16'a' from 2 for 1) from uni.t5;

select overlay(v placing _UTF16'a' from 2 for 1) from uni.t6;

select overlay(v placing 'ya' from 3 for 0) from uni.t5;

select overlay(v placing _UTF16'ya' from 3 for 0) from uni.t6;

select position('i' in v) from uni.t5;

-- FIXME:  should fail:  character set mismatch
-- select position(_UTF16'i' in v) from uni.t5;

select position(_UTF16'i' in v) from uni.t6;

select trim(both from '  a  ') from uni.t5;

-- FIXME:  implicit trim char should match character set automatically
-- select trim(both from _UTF16'  a  ') from uni.t6;

select trim(both _UTF16' ' from _UTF16'  a  ') from uni.t6;

select upper(v) from uni.t5;

select upper(v) from uni.t6;

select lower(v) from uni.t5;

select lower(v) from uni.t6;

select initcap(v||v) from uni.t5;

select initcap(v||v) from uni.t6;
