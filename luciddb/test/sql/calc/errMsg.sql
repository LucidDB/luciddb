-- test err msgs
-- Was calc8.sql

-- FRG-46
VALUES (1000 * 1000 * 1000 * 1000 * 1000 * 1000 * 1000);
VALUES (1 / sin(0));
VALUES (tan(2,0));
-- VALUES (power(0));
VALUES (pow(0));
VALUES (tan(date '1995-11-12'));
VALUES (100 / 0);
VALUES (100 / sin(0));
-- VALUES (to_char(power(10,200) * power(10,200)));
VALUES (pow(10,200) * pow(10,200));
VALUES (exp(1000));

-- Should get an error for a floating point out of range for some tests.
VALUES (LN(-1.0));
VALUES (LOG(-1.0));
VALUES (ACOS(5.0));
VALUES (ASIN(5.0));
VALUES (TAN(3.14159265 / 2.00000000));
VALUES (MOD(-5.0, 0));
VALUES (SQRT(-1.0));
VALUES (VALUERANGE(22.34323, 5.443));
VALUES (CAST (1000000000000000000 AS DECIMAL ( 19, 0)) / CAST (.001  AS DECIMAL (10, 5)));

-- End test
