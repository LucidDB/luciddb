-- $Id$
-- Full vertical negative system testing

-- NOTE: This script is run twice. Once with the "calcVirtualMachine" set to use fennel
-- and another time to use java. The caller of this script is setting the flag so no need
-- to do it directly unless you need to do acrobatics.

-- =============================================
-- |                                            |
-- |   EVERY QUERY IN THIS SCRIPT SHOULD FAIL   |
-- |                                            |
-- =============================================

-- empty escape string forbidden
values 'x' LIKE 'x' escape '';
values 'x' similar to 'x' escape '';
-- escape string with char length > 1 forbidden
values 'x' like 'x' escape 'ab';
values 'x' similar to 'x' escape 'ab';

values true and 1;
values false and '';
values 1.2 and unknown;
values unknown or 1;
values '' or x'';
values not 1;
values not '';

values 'a'||1;
values false||'a';

values 1/0;
values 1.1/0.0;
values MOD(1, 0);
values power(0.0, -1.0);
values ln(0.0);
values log10(0.0);

values 1='';
values false='';
values x'101f'=0.001;
values 1<>'';
values false<>'';
values x''<>0.2;
values 1>true;
values x''>'';
values 1<false;
values ''<0.1;
values 1>=true;
select x''>=name from sales.emps;
values 1<=true;
values ''<=0.0;

--should throw overflow error?
--values 2147483647+1
--values 1073741824*2;
--how about underflow?

values 1 is true;
values '' is not true;
values 1 is false;
values 0.01 is not false;
values x'' is unknown; 
values x'' is not unknown;

--prefix
values -x'';
values -'2';
values +x'';
values +cast(null as date);

values x'' between x'' and 1;
values x'' not between '' and 3;
values '' between '' and 1.0;
--all then's and else return null forbidden in SQL99
values CASE 1 WHEN 1 THEN NULL WHEN 2 THEN NULL END;

--according to the standard any trim character value that is not of length 1 should return a runtime error
values trim('ab' from 'ab');
values trim('' from 'ab');

values position(1 in 'superman');
values character_length(1);
values char_length(x'');
values upper(0.02);
values lower(x'');

--doesnt return the correct error message but keeping it active until its fixed
--values initcap(cast(null as date));
values initcap(1);

values abs('');

values nullif('',1);
values coalesce('a','b',1);

values localtime();
values localtime(1,2);
values localtime(-1);
values localtimestamp();
values localtimestamp(1,2);
values localtimestamp(-3);
values current_time(2,3);
values current_timestamp(-1);
values current_time(-20);
values current_date(2);
values log10(1,2,3);

values (1),(2,3);
values (1),('1');
values (1,'1'),(2,3);


