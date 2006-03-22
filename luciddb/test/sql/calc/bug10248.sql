
--
-- bug10248: problem converting string to int
--

create schema bug10248
;

create table bug10248.bug10248_t(flight_num char(1), year_entered date)
;

insert into bug10248.bug10248_t values('1', date'1998-05-07')
;

insert into bug10248.bug10248_t values('2', date'1998-05-07')
;

insert into bug10248.bug10248_t values('3', date'1998-05-07')
;

insert into bug10248.bug10248_t values('4', date'1998-05-07')
;

insert into bug10248.bug10248_t values('5', date'1998-05-07')
;

insert into bug10248.bug10248_t values('6', date'1998-05-07')
;

insert into bug10248.bug10248_t values('7', date'1998-05-07')
;

insert into bug10248.bug10248_t values('8', date'1998-05-07')
;

insert into bug10248.bug10248_t values('9', date'1998-05-07')
;

CREATE VIEW BUG10248.BUG_10248_V AS
SELECT
cast (BUG10248_T.FLIGHT_NUM as integer) AS FLIGHT_NUM,
BUG10248_T.YEAR_ENTERED
FROM
BUG10248.BUG10248_T
;

-- TODO: no "IN" yet
SELECT
BUG_10248_V.YEAR_ENTERED
FROM
BUG10248.BUG_10248_V
WHERE
(BUG_10248_V.FLIGHT_NUM IN (1,2))
ORDER BY 1
;
