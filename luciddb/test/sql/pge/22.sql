-- Query 22 -- Joy new, BB only
select cust_serv_acct_key, sum(rsd_svc_bilg_amt) topcust_amt
from pge.revn_dtl_rand
group by cust_serv_acct_key having rank( sum(rsd_svc_bilg_amt) ) <= 10
;
