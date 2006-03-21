--
-- bug 12903 - QUARTER function does not work on local time, but it should
--

values (QUARTER(CAST ('1999-03-31 15:59:59' as TIMESTAMP)));

values (QUARTER(CAST ('1999-03-31 16:00:00' as TIMESTAMP)));
