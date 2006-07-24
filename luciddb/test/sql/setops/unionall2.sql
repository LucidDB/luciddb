set schema 'stkmkt';


-- find the total commission paid by each customer for the whole year
select t1.account account, t2.acct_name account_name, sum(commission) 
from fyfull t1, accounts t2
where 
t1.account = t2.acct_no and
t1.purchase_time is not null
group by t1.account, t2.acct_name
order by t1.account, t2.acct_name
;

--
-- find the total commission paid by each customer for the whole year
-- by UNION ALLing over half-year transactions.
select account, account_name, sum(sumcomm) from
(
select t1.account account, t2.acct_name account_name, sum(commission) sumcomm
from cy_firsthalf t1, accounts t2
where 
t1.account = t2.acct_no and
t1.purchase_time is not null
group by t1.account, t2.acct_name
UNION ALL
select t1.account account, t2.acct_name account_name, sum(commission) sumcomm
from cy_secondhalf t1, accounts t2
where 
t1.account = t2.acct_no and
t1.purchase_time is not null
group by t1.account, t2.acct_name
)
group by account, account_name
order by account, account_name
;

--
--find the total commission paid by each customer for the whole year
--by UNION ALLing over qtrly transactions.
select account, account_name, sum(sumcomm) from
(
select t1.account account, t2.acct_name account_name, sum(commission) sumcomm
from cyqtr1 t1, accounts t2
where 
t1.account = t2.acct_no and
t1.purchase_time is not null
group by t1.account, t2.acct_name
UNION ALL
select t1.account account, t2.acct_name account_name, sum(commission) sumcomm
from cyqtr2 t1, accounts t2
where 
t1.account = t2.acct_no and
t1.purchase_time is not null
group by t1.account, t2.acct_name
UNION ALL
select t1.account account, t2.acct_name account_name, sum(commission) sumcomm
from cyqtr3 t1, accounts t2
where 
t1.account = t2.acct_no and
t1.purchase_time is not null
group by t1.account, t2.acct_name
UNION ALL
select t1.account account, t2.acct_name account_name, sum(commission) sumcomm
from cyqtr4 t1, accounts t2
where 
t1.account = t2.acct_no and
t1.purchase_time is not null
group by t1.account, t2.acct_name
)
group by account, account_name
order by account, account_name
;

--
--do the above queries using from-list subqueries
--
select t1.account account, t2.acct_name account_name, sum(commission) 
from accounts t2, (select * from fy_firsthalf UNION ALL select * from fy_secondhalf) t1
where 
t1.account = t2.acct_no and
t1.purchase_time is not null
group by account, account_name
order by account, account_name
;

select t1.account account, t2.acct_name account_name, sum(commission) 
from accounts t2, (select * from fyqtr1 UNION ALL select * from fyqtr2 UNION ALL                   select * from fyqtr3 UNION ALL select * from fyqtr4)  t1
where 
t1.account = t2.acct_no and
t1.purchase_time is not null
group by t1.account, t2.acct_name
order by t1.account, t2.acct_name
;

--find the total commission paid by each customer for the whole year
--by UNION ALLing over half-yearly transactions.
select account, account_name, sum(sumcomm) from
(
select t1.account account, t2.acct_name account_name, sum(commission) sumcomm
from accounts t2, (select * from fyqtr1 UNION ALL select * from fyqtr2 ) t1
where 
t1.account = t2.acct_no and
t1.purchase_time is not null
group by t1.account, t2.acct_name
UNION ALL
select t1.account account, t2.acct_name account_name, sum(commission) sumcomm
from accounts t2, (select * from fyqtr3 UNION ALL select * from fyqtr4) t1
where 
t1.account = t2.acct_no and
t1.purchase_time is not null
group by t1.account, t2.acct_name
)
group by account, account_name
order by account, account_name
;

--
--find the total commission paid by each customer for the whole year
--by UNION ALLing over qtrly transactions.
select account, account_name, sum(sumcomm) from
(
select t1.account account, t2.acct_name account_name, sum(commission) sumcomm
from accounts t2, (select * from jantran UNION ALL 
                   select * from febtran UNION ALL 
                   select * from martran) t1
where 
t1.account = t2.acct_no and
t1.purchase_time is not null
group by t1.account, t2.acct_name
UNION ALL
select t1.account account, t2.acct_name account_name, sum(commission) sumcomm
from accounts t2, (select * from aprtran UNION ALL 
                   select * from maytran UNION ALL 
                   select * from juntran) t1
where 
t1.account = t2.acct_no and
t1.purchase_time is not null
group by t1.account, t2.acct_name
UNION ALL
select t1.account account, t2.acct_name account_name, sum(commission) sumcomm
from accounts t2, (select * from jultran UNION ALL 
                   select * from augtran UNION ALL 
                   select * from septran) t1
where 
t1.account = t2.acct_no and
t1.purchase_time is not null
group by t1.account, t2.acct_name
UNION ALL
select t1.account account, t2.acct_name account_name, sum(commission) sumcomm
from accounts t2, (select * from octtran UNION ALL 
                   select * from novtran UNION ALL 
                   select * from dectran) t1
where 
t1.account = t2.acct_no and
t1.purchase_time is not null
group by t1.account, t2.acct_name
)
group by account, account_name
order by account, account_name
;
