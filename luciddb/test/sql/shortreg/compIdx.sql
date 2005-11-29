--
-- composite index tests
--


-- leading edge searches

select LNAME from customers where LNAME='Dole' order by 1;
select LNAME from customers where LNAME='Peterson' order by 1;
select LNAME from customers where LNAME='Andrews' order by 1;

select LNAME from customers where LNAME<='Dole' order by 1;
select LNAME,FNAME from customers where LNAME>='Peterson' order by 1,2;
select LNAME,CUSTID from customers where LNAME<='Andrews' order by 1,2;

select LNAME from customers where LNAME<'Dole' order by 1;
select LNAME,FNAME from customers where LNAME>'Peterson' order by 1,2;
select LNAME,CUSTID from customers where LNAME<'Andrews' order by 1,2;

-- full searches

select * from customers where LNAME='Bush' and FNAME='Gerry' order by 1;
select LNAME from customers where LNAME='Frank' and FNAME='Victor' order by 1;


