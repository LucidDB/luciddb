set schema 'stkmkt';


--create views with UNION ALLS
create view cyqtr1 as
select * from jantran UNION ALL
select * from febtran UNION ALL
select * from martran
;
--
create view cyqtr2 as
select * from aprtran UNION ALL
select * from maytran UNION ALL
select * from juntran
;
create view cyqtr3 as
select * from jultran UNION ALL
select * from augtran UNION ALL
select * from septran
;
--
create view cyqtr4 as
select * from octtran UNION ALL
select * from novtran UNION ALL
select * from dectran
;

create view cy_firsthalf as
select * from cyqtr1 UNION ALL
select * from cyqtr2
;
create view cy_secondhalf as
select * from cyqtr3 UNION ALL
select * from cyqtr4
;
create view cyfull as
select * from cy_firsthalf UNION ALL
select * from cy_secondhalf
;
--
--
create view fyqtr3 as
select * from jantran UNION ALL
select * from febtran UNION ALL
select * from martran
;
--
create view fyqtr4 as
select * from aprtran UNION ALL
select * from maytran UNION ALL
select * from juntran
;
create view fyqtr1 as
select * from jultran UNION ALL
select * from augtran UNION ALL
select * from septran
;
--
create view fyqtr2 as
select * from octtran UNION ALL
select * from novtran UNION ALL
select * from dectran
;

create view fy_secondhalf as
select * from fyqtr1 UNION ALL
select * from fyqtr2
;
create view fy_firsthalf as
select * from fyqtr3 UNION ALL
select * from fyqtr4
;
create view fyfull as
select * from fy_firsthalf UNION ALL
select * from fy_secondhalf
;

--create views of transaction data for each customer
create view Investor1View as
select  a.acct_no account_number, 
        a.acct_name account_name,
        cyf.* 
from    accounts a, cyfull cyf
where   cyf.account = a.acct_no and
        a.acct_no = 1
;
create view Investor2View as
select  a.acct_no account_number, 
        a.acct_name account_name,
        cyf.* 
from    accounts a, cyfull cyf
where   cyf.account = a.acct_no and
        a.acct_no = 2
;
create view Investor3View as
select  a.acct_no account_number, 
        a.acct_name account_name,
        cyf.* 
from    accounts a, cyfull cyf
where   cyf.account = a.acct_no and
        a.acct_no = 3
;
create view Investor4View as
select  a.acct_no account_number, 
        a.acct_name account_name,
        cyf.* 
from    accounts a, cyfull cyf
where   cyf.account = a.acct_no and
        a.acct_no = 4
;
create view Investor5View as
select  a.acct_no account_number, 
        a.acct_name account_name,
        cyf.* 
from    accounts a, cyfull cyf
where   cyf.account = a.acct_no and
        a.acct_no = 5
;
create view Investor6View as
select  a.acct_no account_number, 
        a.acct_name account_name,
        cyf.* 
from    accounts a, cyfull cyf
where   cyf.account = a.acct_no and
        a.acct_no = 6
;
create view Investor7View as
select  a.acct_no account_number, 
        a.acct_name account_name,
        cyf.* 
from    accounts a, cyfull cyf
where   cyf.account = a.acct_no and
        a.acct_no = 7
;
create view Investor8View as
select  a.acct_no account_number, 
        a.acct_name account_name,
        cyf.* 
from    accounts a, cyfull cyf
where   cyf.account = a.acct_no and
        a.acct_no = 8
;
create view Investor9View as
select  a.acct_no account_number, 
        a.acct_name account_name,
        cyf.* 
from    accounts a, cyfull cyf
where   cyf.account = a.acct_no and
        a.acct_no = 9
;
create view Investor10View as
select  a.acct_no account_number, 
        a.acct_name account_name,
        cyf.* 
from    accounts a, cyfull cyf
where   cyf.account = a.acct_no and
        a.acct_no = 10
;
