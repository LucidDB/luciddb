--
-- calc/funcMath.sql
--
-- test MATH builtin functions
-- Was calc3.sql

set schema 's';

create table boris1
(
nnull numeric(8,0),
n1 numeric(8,0),
n2 numeric(8,0),
n3 numeric(8,0),
snull varchar(40),
s1 varchar(40),
s2 varchar(40),
s3 varchar(40)
);

insert into boris1 values (null,1,2,3,null,'a','ab','abc');

-- select asin(sin(.5)), acos(cos(.8)), atan(tan(.2))
-- from boris1
-- ;
-- select sign(n1), sign(nnull), sign(2 - 3) from boris1
--;
select abs(-25), abs(25), abs(325.23523), abs(n1), abs(nnull) from boris1
;
select ceil(.235), ceil(n1), ceil(nnull) from boris1
;
select exp(nnull), exp(0), exp(10) from boris1
;
-- FRG-52
select floor(2.353425), floor(0), floor(nnull), floor(n2 / 10) from boris1
;
select ln(nnull), ln(0), ln(exp(1)) from boris1
;
select ln(nnull), ln(exp(1)) from boris1
;
select log10(nnull), log10(1000), log10(n2) from boris1
;
select ln(nnull), ln(1000), ln(n2) from boris1
;
-- test mod()
select mod(nnull, 2), mod(2, nnull) from boris1
;
select mod(nnull,0) from boris1
;
select mod(2,0) from boris1
;
select mod(-2,3), mod(2,-3) from boris1
;
-- JIRA FRG-150
select mod(2.256,5) from boris1
;
select mod(n2,n3) from boris1
;
select mod(0,3) from boris1
;


select pow(10,2), pow(0,0), pow(nnull, 0), pow(0, nnull),
       pow(1000,5) from boris1
;
select pow(2,0.5), pow(3,0.5), pow(-2,1), pow(-2,2) from boris1
;
select pow(-2.0,-1.0), pow(-2,-2) from boris1
;
select pow(-1,-1/3), pow(-1,-1/5) from boris1
;
select pow(-1,-0.5) from boris1
;
select pow(-1,-0.25) from boris1
;


-- select round(cos(.2), 1), round(.2383, 2), round(.2313,0), round(nnull, 3) from boris1
-- ;
-- select trunc(cos(.2), 1), trunc(.2313,2), trunc(.2313,0), trunc(nnull, 3) from boris1
-- ;
-- select trunc(cos(.2)), trunc(.2313), trunc(.2313), trunc(nnull) from boris1
-- ;
-- select round((-1*cos(.2)), 1), round(-.2383, 2), round(-.2313,0) from boris1
-- ;
-- select trunc((-1 * cos(.2)), 1), trunc(-.2313,2), trunc(-.2313,0) from boris1
-- ;
-- select sign(-34), sign(23), sign(nnull), sign(n2) from boris1
-- ;
-- select sqrt(n2-n1-n3), sqrt(100), sqrt(nnull) from boris1
-- ;

drop table boris1;

-- Test truncation on real values
--DROP TABLE bug6813;
--CREATE TABLE bug6813
--   (
--    rv REAL,
--    dv DOUBLE
--    );
--INSERT INTO bug6813
--values (0.987, 0.987)
--UNION ALL
--values (9.987, 9.87)
--UNION ALL
--values (99.987, 99.987)
--UNION ALL
--values (1, 1)
--UNION ALL
--values (0.9999999, 0.9999999)
--UNION ALL
--values (44444, 44444)
--;
--SELECT rv,
--   TRUNC(rv, 0) T0,
--   TRUNC(rv, 1) T1,
--   TRUNC(rv, 2) T2,
--   TRUNC(rv, 3) T3,
--   TRUNC(rv, 4) T4
--FROM bug6813
--ORDER BY rv;
--SELECT dv,
--   TRUNC(dv, 0) T0,
--   TRUNC(dv, 1) T1,
--   TRUNC(dv, 2) T2,
--   TRUNC(dv, 3) T3,
--   TRUNC(dv, 4) T4
--FROM bug6813
--ORDER BY dv;

-- bug 6268:  overflow detection
-- bunch of separate statements to keep the warning output deterministic
--select  n,to_number(to_Char(power(n,n))) from system.hundred 
--where n = 10;

--select  n,to_number(to_Char(power(n,n))) from system.hundred 
--where n = 11;

--select  n,to_number(to_Char(power(n,n))) from system.hundred 
--where n = 12;

--select  n,to_number(to_Char(power(n,n))) from system.hundred 
--where n = 13;

--select  n,to_number(to_Char(power(n,n))) from system.hundred 
--where n = 14;

--select  n,to_number(to_Char(power(n,n))) from system.hundred 
--where n = 15;

--select  n,to_number(to_Char(power(n,n))) from system.hundred 
--where n = 16;

--select  n,to_number(to_Char(power(n,n))) from system.hundred 
--where n = 18;

--select  n,to_number(to_Char(power(n,n))) from system.hundred 
--where n = 19;

-- End test calc/funcMath.sql

