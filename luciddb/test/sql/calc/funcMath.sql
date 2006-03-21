--
-- calc/funcMath.sql
--
-- test MATH builtin functions
-- Was calc3.sql

set schema 's';

-- select asin(sin(.5)), acos(cos(.8)), atan(tan(.2))
-- from boris
-- ;
-- select sign(n1), sign(nnull), sign(2 - 3) from boris
--;
-- select abs(-25), abs(25), abs(325.23523), abs(n1), abs(nnull) from boris
-- ;
select ceil(.235), ceil(n1), ceil(nnull) from boris
;
select exp(nnull), exp(0), exp(10) from boris
;
-- FRG-52
select floor(2.353425), floor(0), floor(nnull), floor(n2 / 10) from boris
;
select ln(nnull), ln(0), ln(exp(1)) from boris
;
select ln(nnull), ln(exp(1)) from boris
;
-- select log(nnull), log(1000), log(n2) from boris
-- ;
select ln(nnull), ln(1000), ln(n2) from boris
;
select mod(n2,n3), mod(nnull, 0), mod(2, 0), mod(2.256, 2) from boris
;
select pow(10,2), pow(0,0), pow(nnull, 0),
       pow(1000,5) from boris
;
-- select round(cos(.2), 1), round(.2383, 2), round(.2313,0), round(nnull, 3) from boris
-- ;
-- select trunc(cos(.2), 1), trunc(.2313,2), trunc(.2313,0), trunc(nnull, 3) from boris
-- ;
-- select trunc(cos(.2)), trunc(.2313), trunc(.2313), trunc(nnull) from boris
-- ;
-- select round((-1*cos(.2)), 1), round(-.2383, 2), round(-.2313,0) from boris
-- ;
-- select trunc((-1 * cos(.2)), 1), trunc(-.2313,2), trunc(-.2313,0) from boris
-- ;
-- select sign(-34), sign(23), sign(nnull), sign(n2) from boris
-- ;
-- select sqrt(n2-n1-n3), sqrt(100), sqrt(nnull) from boris
-- ;

-- Test truncation on real values
DROP TABLE bug6813;
CREATE TABLE bug6813
   (
    rv REAL,
    dv DOUBLE
    );
INSERT INTO bug6813
SELECT 0.987, 0.987 FROM onerow
UNION ALL
SELECT 9.987, 9.87 FROM onerow
UNION ALL
SELECT 99.987, 99.987 FROM onerow
UNION ALL
SELECT 1, 1 FROM onerow
UNION ALL
SELECT 0.9999999, 0.9999999 FROM onerow
UNION ALL
SELECT 44444, 44444 FROM onerow
;
SELECT rv,
   TRUNC(rv, 0) T0,
   TRUNC(rv, 1) T1,
   TRUNC(rv, 2) T2,
   TRUNC(rv, 3) T3,
   TRUNC(rv, 4) T4
FROM bug6813
ORDER BY rv;
SELECT dv,
   TRUNC(dv, 0) T0,
   TRUNC(dv, 1) T1,
   TRUNC(dv, 2) T2,
   TRUNC(dv, 3) T3,
   TRUNC(dv, 4) T4
FROM bug6813
ORDER BY dv;

-- bug 6268:  overflow detection
-- bunch of separate statements to keep the warning output deterministic
select  n,to_number(to_Char(power(n,n))) from system.hundred 
where n = 10;

select  n,to_number(to_Char(power(n,n))) from system.hundred 
where n = 11;

select  n,to_number(to_Char(power(n,n))) from system.hundred 
where n = 12;

select  n,to_number(to_Char(power(n,n))) from system.hundred 
where n = 13;

select  n,to_number(to_Char(power(n,n))) from system.hundred 
where n = 14;

select  n,to_number(to_Char(power(n,n))) from system.hundred 
where n = 15;

select  n,to_number(to_Char(power(n,n))) from system.hundred 
where n = 16;

select  n,to_number(to_Char(power(n,n))) from system.hundred 
where n = 18;

select  n,to_number(to_Char(power(n,n))) from system.hundred 
where n = 19;

-- End test calc/funcMath.sql

