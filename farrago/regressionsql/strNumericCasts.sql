-- $Id$
-- Test casts between string types

--
-- test basic integer conversions
--
values cast('1' as smallint);
values cast('1' as integer);
values cast('1' as bigint);

values cast('-1' as smallint);
values cast('-1' as integer);
values cast('-1' as bigint);

values cast('0' as smallint);
values cast('0' as integer);
values cast('0' as bigint);

--
-- test smallint limits (signed 16-bit ints: -32768 to 32767)
--
values cast('32767' as smallint);
values cast('-32768' as smallint);

-- these should fail (numeric out of range)
values cast('32768' as smallint);
values cast('65535' as smallint);
values cast('-32769' as smallint);

--
-- test integer/int limits (signed 32-bit ints: -2147483648 to 2147483647)
--
values cast('2147483647' as integer);
values cast('-2147483648' as integer);

-- these should fail (numeric out of range)
values cast('2147483648' as integer);
values cast('4294967295' as integer);
values cast('-2147483649' as integer);

--
-- test bigint limits (signed 64-bit int: -9223372036854775808 to 9223372036854775807)
--
values cast('9223372036854775807' as bigint);
values cast('-9223372036854775808' as bigint);

--
-- test decimal(N, 0) limits (signed N-digit integers)
--
--values cast('99999' as decimal(5, 0));
--values cast('-99999' as decimal(5, 0));

-- these should fail (numeric out of range)
--values cast('100000' as decimal(5, 0));
--values cast('-100000' as decimal(5, 0));


--
-- test decimal(N, M) (exact numeric) conversions
--
--values cast('1.0' as decimal(2,1));
--values cast('-1.0' as decimal(2,1));
--values cast('0.0' as decimal(2,1));
--values cast('12.34' as decimal(4, 2));
--values cast('-43.21' as decimal(4, 2));
--values cast('5.67' as decimal(4, 2));

--values cast('1' as decimal(2,1));
--values cast('-1' as decimal(2,1));
--values cast('0' as decimal(2,1));
--values cast('12' as decimal(4, 2));
--values cast('-43' as decimal(4, 2));

-- REVIEW: spec says that numeric out of range error only occurs when
-- the most significant digits are lost: these are okay
--values cast('12.345' as decimal(4, 2));
--values cast('-12.345' as decimal(4, 2));

-- these should fail (numeric out of range)
--values cast('123.45' as decimal(4, 2));
--values cast('-123.45' as decimal(4, 2));



--
-- test double (approximate numeric) conversions
--
values cast('1.0' as double);
values cast('-1.0' as double);
values cast('0.0' as double);

-- REVIEW: I presume these should work as well.
values cast('1' as double);
values cast('-1' as double);
values cast('0' as double);

-- TODO: determine limits of DOUBLE and test them


--
-- test real (approximate numeric) conversions
--
values cast('1.0' as real);
values cast('-1.0' as real);
values cast('0.0' as real);

-- REVIEW: I presume these should work as well.
values cast('1' as real);
values cast('-1' as real);
values cast('0' as real);

-- TODO: determine limits of REAL and test them

-- these should fail (numeric out of range)

alter system set "calcVirtualMachine" = 'CALCVM_JAVA';
values cast('9223372036854775808' as bigint);
values cast('18446744073709551615' as bigint);
values cast('-9223372036854775809' as bigint);

alter system set "calcVirtualMachine" = 'CALCVM_FENNEL';
values cast('9223372036854775808' as bigint);
values cast('18446744073709551615' as bigint);
values cast('-9223372036854775809' as bigint);

