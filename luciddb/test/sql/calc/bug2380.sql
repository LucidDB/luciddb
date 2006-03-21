--
-- Bug 2380
-- Owner: Boris
-- Abstract: incorrect evaluation of expression in HAVING clause
--

set schema 's';

create table qrysrcnum (
n1 numeric (2, 0),
n2 numeric (5, 2),
n3 numeric (10, 7),
n4 numeric (15, 6))
;

insert into qrysrcnum values ( 5, 20.00 , 300.00, 5000000.000000 )
;
insert into qrysrcnum values ( 7, 10.00 , 300.00, 50000.000000 )
;
insert into qrysrcnum values ( 7, 10.00 , 300.00, 50000.000000 )
;

-- FRG-49, FRG-50
-- SELECT  n1, n2, n3, n4, AVG(n1)   
SELECT  n1, n2, n3, n4
FROM  qrysrcnum  
GROUP BY n1, n2, n3, n4   
-- HAVING (n4 - n3) < (SELECT AVG(n1) FROM qrysrcnum qsn)  
HAVING (n4 - n3) < (SELECT SUM(n1) FROM qrysrcnum qsn)  
;
