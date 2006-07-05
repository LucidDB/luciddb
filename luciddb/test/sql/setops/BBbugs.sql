set schema 'stkmkt';


-- BB bug3306
select t1.security from jantran t1
except
select t1.security from febtran t1
UNION ALL
(select t1.security from febtran t1
 except
 select t1.security from martran t1)
UNION ALL
(select t1.security from martran t1
 except
 select t1.security from aprtran t1)
UNION ALL
(select t1.security from aprtran t1
 except
 select t1.security from maytran t1)
UNION ALL
(select t1.security from maytran t1
 except
 select t1.security from juntran t1)
UNION ALL
(select t1.security from juntran t1
 except
 select t1.security from jultran t1)
UNION ALL
(select t1.security from jultran t1
 except 
 select t1.security from augtran t1)
UNION ALL
(select t1.security from augtran t1
 except
 select t1.security from septran t1)
UNION ALL
(select t1.security from septran t1
 except 
 select t1.security from octtran t1)
UNION ALL
(select t1.security from octtran t1
 except 
 select t1.security from novtran t1)
UNION ALL
(select t1.security from novtran t1
 except
 select t1.security from dectran t1)
order by 1
;
--BBTEST: Find securities that traded in each month that
--        didn't trade in the following month in CY97.
--
select t1.symbol, t1.company
from tickers t1, jantran t2
where t1.symbol = t2.security
except
select t1.symbol, t1.company
from tickers t1, febtran t2
where t1.symbol = t2.security
UNION ALL
(select t1.symbol, t1.company
 from tickers t1, febtran t2
 where t1.symbol = t2.security
 except
 select t1.symbol, t1.company
 from tickers t1, martran t2
 where t1.symbol = t2.security)
UNION ALL
(select t1.symbol, t1.company
 from tickers t1, martran t2
 where t1.symbol = t2.security
 except
 select t1.symbol, t1.company
 from tickers t1, aprtran t2
 where t1.symbol = t2.security)
UNION ALL
(select t1.symbol, t1.company
 from tickers t1, aprtran t2
 where t1.symbol = t2.security
 except
 select t1.symbol, t1.company
 from tickers t1, maytran t2
 where t1.symbol = t2.security)
UNION ALL
(select t1.symbol, t1.company
 from tickers t1, maytran t2
 where t1.symbol = t2.security
 except
 select t1.symbol, t1.company
 from tickers t1, juntran t2
 where t1.symbol = t2.security)
UNION ALL
(select t1.symbol, t1.company
 from tickers t1, juntran t2
 where t1.symbol = t2.security
 except
 select t1.symbol, t1.company
 from tickers t1, jultran t2
 where t1.symbol = t2.security)
UNION ALL
(select t1.symbol, t1.company
 from tickers t1, jultran t2
 where t1.symbol = t2.security
 except
 select t1.symbol, t1.company
 from tickers t1, augtran t2
 where t1.symbol = t2.security)
UNION ALL
(select t1.symbol, t1.company
 from tickers t1, augtran t2
 where t1.symbol = t2.security
 except
 select t1.symbol, t1.company
 from tickers t1, septran t2
 where t1.symbol = t2.security)
UNION ALL
(select t1.symbol, t1.company
 from tickers t1, septran t2
 where t1.symbol = t2.security
 except
 select t1.symbol, t1.company
 from tickers t1, octtran t2
 where t1.symbol = t2.security)
UNION ALL
(select t1.symbol, t1.company
 from tickers t1, octtran t2
 where t1.symbol = t2.security
 except
 select t1.symbol, t1.company
 from tickers t1, novtran t2
 where t1.symbol = t2.security)
UNION ALL
(select t1.symbol, t1.company
 from tickers t1, novtran t2
 where t1.symbol = t2.security
 except
 select t1.symbol, t1.company
 from tickers t1, dectran t2
 where t1.symbol = t2.security)
order by 1, 2
;

-- BB bug3313
--BBTEST: Find clients who made > $100 in the 
--        1st and 2nd months of CY97.

select t1.account account,
       t2.acct_name account_name,
       sum ( (sale_price * numshares) - (purchase_price * numshares) - commission) profit
from jantran t1, accounts t2
where t1.account = t2.acct_no 
group by t1.account, t2.acct_name
having sum ( (sale_price * numshares) - (purchase_price * numshares) - commission) > 100
UNION ALL
select t1.account account,
       t2.acct_name account_name,
       sum ( (sale_price * numshares) - (purchase_price * numshares) - commission) profit
from febtran t1, accounts t2
where t1.account = t2.acct_no 
group by t1.account, t2.acct_name
having sum ( (sale_price * numshares) - (purchase_price * numshares) - commission) > 100
order by 1, 2
;
