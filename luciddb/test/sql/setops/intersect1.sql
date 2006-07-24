set schema 'stkmkt';

-- test INTERSECT

-- clients who traded in first quarter
select t1.account account, 
       t2.acct_name account_name
from cyqtr1 t1, accounts t2
where t1.account = t2.acct_no
order by 1,2
;
-- clients who traded in second quarter
select t1.account account, 
       t2.acct_name account_name
from cyqtr2 t1, accounts t2
where t1.account = t2.acct_no
order by 1, 2
;
-- clients who traded in first and second quarter
select t1.account account, 
       t2.acct_name account_name
from cyqtr1 t1, accounts t2
where t1.account = t2.acct_no
INTERSECT
select t1.account account, 
       t2.acct_name account_name
from cyqtr2 t1, accounts t2
where t1.account = t2.acct_no
order by 1, 2
;
-- clients who traded in first and second quarter
select t1.account account, 
       t2.acct_name account_name
from accounts t2, (select * from jantran UNION ALL
                   select * from febtran UNION ALL
                   select * from martran ) t1
where t1.account = t2.acct_no
INTERSECT
select t1.account account, 
       t2.acct_name account_name
from accounts t2, (select * from aprtran UNION ALL
                   select * from maytran UNION ALL
                   select * from juntran) t1
where t1.account = t2.acct_no
order by 1, 2
;

-- clients who traded in jan
select t1.account account, 
       t2.acct_name account_name
from jantran t1, accounts t2
where t1.account = t2.acct_no
order by 1, 2
;
-- clients who traded in feb
select t1.account account, 
       t2.acct_name account_name
from febtran t1, accounts t2
where t1.account = t2.acct_no
order by 1, 2
;
-- clients who traded in mar
select t1.account account, 
       t2.acct_name account_name
from martran t1, accounts t2
where t1.account = t2.acct_no
order by 1, 2
;
-- clients who traded in apr
select t1.account account, 
       t2.acct_name account_name
from (select * from aprtran ) t1, accounts t2
where t1.account = t2.acct_no
order by 1, 2
;
-- clients who traded in jan, feb, mar but not in apr 
select t1.account account, 
       t2.acct_name account_name
from jantran t1, accounts t2
where t1.account = t2.acct_no
INTERSECT
select t1.account account, 
       t2.acct_name account_name
from febtran t1, accounts t2
where t1.account = t2.acct_no
INTERSECT
select t1.account account, 
       t2.acct_name account_name
from martran t1, accounts t2
where t1.account = t2.acct_no
EXCEPT
select t1.account account, 
       t2.acct_name account_name
from (select * from aprtran ) t1, accounts t2
where t1.account = t2.acct_no
order by 1, 2
;
