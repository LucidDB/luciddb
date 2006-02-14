-- Query 21 -- Joy new
SELECT	c.cust_serv_acct_key,
c.cuin_lst_nm,
t.revn_yr_mo,
sum(f.RSD_SVC_BILG_AMT)
FROM (SELECT cust_serv_acct_key,
        max(rsd_svc_bilg_amt) maxamt
        FROM pge.revn_dtl_rand
        GROUP by cust_serv_acct_key
        HAVING max(rsd_svc_bilg_amt) > 500) keys,
        pge.cust_serv_acct c,
        pge.revn_dtl_rand f,
        pge.revn_prd t
WHERE t.revn_yr_mo = f.revn_yr_mo
and c.cust_serv_acct_key = f.cust_serv_acct_key
and t.revn_yr = 1995
and c.cust_serv_acct_key = keys.cust_serv_acct_key
GROUP BY c.cust_serv_acct_key, c.cuin_lst_nm, t.revn_yr_mo
ORDER BY 1, 2, 3, 4
;
