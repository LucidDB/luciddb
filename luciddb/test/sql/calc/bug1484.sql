-- bug1484.sql

-- Positive testing
values applib.convert_date('03-12-1998', 'MM-DD-YYYY');
-- functions not supported
--values applib.convert_date('03-12-1998', 'M-D-Y');
values applib.convert_date('*03-12-1998', '*MM-DD-YYYY');
values applib.convert_date('*03-12-1998*', '*MM-DD-YYYY*');

-- JIRA FRG-145
-- FIXME: this line breaks Java code gen because */ looks like the end of a Javadoc comment
-- values applib.convert_date('03*/12*/1998', 'MM*/DD*/YYYY');
values applib.convert_date('03121998', 'MMDDYYYY');
values applib.convert_date('0321998', 'MMDYYYY');
values applib.convert_date('32-1998', 'MD-YYYY');
-- leap years feb 29
values applib.convert_date('02-29-1996', 'MM-DD-YYYY');
values applib.convert_date('02-29-2000', 'MM-DD-YYYY');

-- month names not supported JIRA LDB-105
--values applib.convert_date('JAN-23-1998', 'MM-DD-YYYY');
--values applib.convert_date('JANUARY-23-1998', 'MM-DD-YYYY');

--values applib.convert_date('FEB-23-1998', 'MM-DD-YYYY');
--values applib.convert_date('FEBRUARY-23-1998', 'MM-DD-YYYY');

--values applib.convert_date('MAR-23-1998', 'MM-DD-YYYY');
--values applib.convert_date('MARCH-23-1998', 'MM-DD-YYYY');

--values applib.convert_date('APR-23-1998', 'MM-DD-YYYY');
--values applib.convert_date('APRIL-23-1998', 'MM-DD-YYYY');

--values applib.convert_date('MAY-23-1998', 'MM-DD-YYYY');
--values applib.convert_date('MAY-23-1998', 'MM-DD-YYYY');

--values applib.convert_date('JUN-23-1998', 'MM-DD-YYYY');
--values applib.convert_date('JUNE-23-1998', 'MM-DD-YYYY');

--values applib.convert_date('JUL-23-1998', 'MM-DD-YYYY');
--values applib.convert_date('JULY-23-1998','MM-DD-YYYY');

--values applib.convert_date('AUG-23-1998', 'MM-DD-YYYY');
--values applib.convert_date('AUGUST-23-1998', 'MM-DD-YYYY');

--values applib.convert_date('SEPT-23-1998', 'MM-DD-YYYY');           -- should the month be SEP?
--values applib.convert_date('SEPTEMBER-23-1998', 'MM-DD-YYYY');

--values applib.convert_date('OCT-23-1998', 'MM-DD-YYYY');
--values applib.convert_date('OCTOBER-23-1998', 'MM-DD-YYYY');

--values applib.convert_date('NOV-23-1998', 'MM-DD-YYYY');
--values applib.convert_date('NOVEMBER-23-1998', 'MM-DD-YYYY');

--values applib.convert_date('DEC-23-1998', 'MM-DD-YYYY');
--values applib.convert_date('DECEMBER-23-1998', 'MM-DD-YYYY');

--values applib.convert_date('APRIL231998', 'MMMMMDDYYYY');
--values applib.convert_date('23SEPT1998', 'DDMMMMYYYY');            -- month is SEPT?
-- end of LDB-105 part 1

-- functions not supported
--values applib.convert_date('11-3-98', 'M-D-Y');
--values applib.convert_date('11-3-23', 'M-D-Y');
--values applib.convert_date('11-3-00', 'M-D-Y');
--values applib.convert_date('11-3-99', 'M-D-Y');

--values applib.convert_date('Sep 12 95', 'M D Y');
--values applib.convert_date('aP 12 95', 'M D Y');
--values applib.convert_date('OctOb 12 95', 'M D Y');
--values applib.convert_date('Januar 12 95', 'M D Y');
--values applib.convert_date('august 12 95', 'M D Y');
--values applib.convert_date('febr 12 95', 'M D Y');
--values applib.convert_date('DECem 12 95', 'M D Y');



-- negative testing
-- unmatched mask
values applib.convert_date('*03-12-1998', 'MM-DD-YYYY');

-- still not work yet, JIRA LDB-105
-- non leap years feb 29
--values applib.convert_date('02-29-1900', 'MM-DD-YYYY');
--values applib.convert_date('02-29-1901', 'MM-DD-YYYY');
-- other invalid dates
--values applib.convert_date('01-32-1998', 'MM-DD-YYYY');
--values applib.convert_date('02-30-1998', 'MM-DD-YYYY');
--values applib.convert_date('04-31-1998', 'MM-DD-YYYY');
-- invalid months
--values applib.convert_date('00-29-1900', 'MM-DD-YYYY');
--values applib.convert_date('13-29-1900', 'MM-DD-YYYY');
-- invalid years
--values applib.convert_date('01-23-0000', 'MM-DD-YYYY');

--values applib.convert_date('00-00-0000', 'MM-DD-YYYY');
--values applib.convert_date('00-00-00', 'MM-DD-YY');
-- end of LDB-105 part 2




-- all are not supported ???
-- for performace reasons we don't disallow the following
--values applib.convert_date('03-12-1998*', 'M-D-Y');
--values applib.convert_date('1998-03-12', 'M-D-Y');
--values applib.convert_date('03-12-1998', '*MM-DD-YYYY');
--values applib.convert_date('*03-12-1998', '*MM-DD-YYYY*');
--values applib.convert_date('03-12-1998', 'MM*/DD*/YYYY');
--values applib.convert_date('0321998', 'MMDDYYYY');
--values applib.convert_date('031998', 'MMDYYYY');
--values applib.convert_date('0312-1998', 'MD-YYYY');
