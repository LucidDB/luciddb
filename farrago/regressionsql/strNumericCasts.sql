-- $Id$
-- Test casts between string types

--
-- test basic integer conversions
--
select cast('1' as smallint) from values(true);
select cast('1' as integer) from values(true);
select cast('1' as bigint) from values(true);

select cast('-1' as smallint) from values(true);
select cast('-1' as integer) from values(true);
select cast('-1' as bigint) from values(true);

select cast('0' as smallint) from values(true);
select cast('0' as integer) from values(true);
select cast('0' as bigint) from values(true);

--
-- test smallint limits (signed 16-bit ints: -32768 to 32767)
--
select cast('32767' as smallint) from values(true);
select cast('-32768' as smallint) from values(true);

-- these should fail (numeric out of range)
select cast('32768' as smallint) from values(true);
select cast('65535' as smallint) from values(true);
select cast('-32769' as smallint) from values(true);

--
-- test integer/int limits (signed 32-bit ints: -2147483648 to 2147483647)
--
select cast('2147483647' as integer) from values(true);
select cast('-2147483648' as integer) from values(true);

-- these should fail (numeric out of range)
select cast('2147483648' as integer) from values(true);
select cast('4294967295' as integer) from values(true);
select cast('-2147483649' as integer) from values(true);

--
-- test bigint limits (signed 64-bit int: -9223372036854775808 to 9223372036854775807)
--
select cast('9223372036854775807' as bigint) from values(true);
select cast('-9223372036854775808' as bigint) from values(true);

-- these should fail (numeric out of range)
select cast('9223372036854775808' as bigint) from values(true);
select cast('18446744073709551615' as bigint) from values(true);
select cast('-9223372036854775809' as bigint) from values(true);

--
-- test decimal(N, 0) limits (signed N-digit integers)
--
--select cast('99999' as decimal(5, 0)) from values(true);
--select cast('-99999' as decimal(5, 0)) from values(true);

-- these should fail (numeric out of range)
--select cast('100000' as decimal(5, 0)) from values(true);
--select cast('-100000' as decimal(5, 0)) from values(true);


--
-- test decimal(N, M) (exact numeric) conversions
--
--select cast('1.0' as decimal(2,1)) from values(true);
--select cast('-1.0' as decimal(2,1)) from values(true);
--select cast('0.0' as decimal(2,1)) from values(true);
--select cast('12.34' as decimal(4, 2)) from values(true);
--select cast('-43.21' as decimal(4, 2)) from values(true);
--select cast('5.67' as decimal(4, 2)) from values(true);

--select cast('1' as decimal(2,1)) from values(true);
--select cast('-1' as decimal(2,1)) from values(true);
--select cast('0' as decimal(2,1)) from values(true);
--select cast('12' as decimal(4, 2)) from values(true);
--select cast('-43' as decimal(4, 2)) from values(true);

-- REVIEW: spec says that numeric out of range error only occurs when
-- the most significant digits are lost: these are okay
--select cast('12.345' as decimal(4, 2)) from values(true);
--select cast('-12.345' as decimal(4, 2)) from values(true);

-- these should fail (numeric out of range)
--select cast('123.45' as decimal(4, 2)) from values(true);
--select cast('-123.45' as decimal(4, 2)) from values(true);



--
-- test double (approximate numeric) conversions
--
select cast('1.0' as double) from values(true);
select cast('-1.0' as double) from values(true);
select cast('0.0' as double) from values(true);

-- REVIEW: I presume these should work as well.
select cast('1' as double) from values(true);
select cast('-1' as double) from values(true);
select cast('0' as double) from values(true);

-- TODO: determine limits of DOUBLE and test them


--
-- test real (approximate numeric) conversions
--
select cast('1.0' as real) from values(true);
select cast('-1.0' as real) from values(true);
select cast('0.0' as real) from values(true);

-- REVIEW: I presume these should work as well.
select cast('1' as real) from values(true);
select cast('-1' as real) from values(true);
select cast('0' as real) from values(true);

-- TODO: determine limits of REAL and test them
