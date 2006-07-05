set schema 'stkmkt';


--test EXCEPT
-- clients who traded in the first qtr of the year
select t1.account account, 
       t2.acct_name account_name
from cyqtr1 t1, accounts t2
where t1.account = t2.acct_no
order by 1,2
;
-- clients who traded in the second qtr of the year
select t1.account account, 
       t2.acct_name account_name
from cyqtr2 t1, accounts t2
where t1.account = t2.acct_no
order by 1,2
;
-- Find the clients who traded in the first qtr of the year but
-- didn't trade in the second qtr
select t1.account account, 
       t2.acct_name account_name
from cyqtr1 t1, accounts t2
where t1.account = t2.acct_no
EXCEPT
select t1.account account, 
       t2.acct_name account_name
from cyqtr2 t1, accounts t2
where t1.account = t2.acct_no
order by 1, 2
;
-- Find the clients who traded in the first qtr of the year but
-- didn't trade in the second qtr
select t1.account account, 
       t2.acct_name account_name
from accounts t2, (select * from jantran UNION ALL
                   select * from febtran UNION ALL
                   select * from martran ) t1
where t1.account = t2.acct_no
EXCEPT
select t1.account account, 
       t2.acct_name account_name
from accounts t2, (select * from aprtran UNION ALL
                   select * from maytran UNION ALL
                   select * from juntran) t1
where t1.account = t2.acct_no
order by 1, 2
;

-- clients who traded in the first half of the year
select t1.account account, 
       t2.acct_name account_name
from cy_firsthalf t1, accounts t2
where t1.account = t2.acct_no
order by 1,2
;
-- clients who traded in the second half of the year
select t1.account account, 
       t2.acct_name account_name
from cy_secondhalf t1, accounts t2
where t1.account = t2.acct_no
order by 1,2
;
-- Find the clients who traded in the first half of the year but
-- didn't trade in the second half
select t1.account account, 
       t2.acct_name account_name
from cy_firsthalf t1, accounts t2
where t1.account = t2.acct_no
EXCEPT
select t1.account account, 
       t2.acct_name account_name
from cy_secondhalf t1, accounts t2
where t1.account = t2.acct_no
order by 1, 2
;
