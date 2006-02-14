-- For Query 1, 8, 11, 13
create cube revn_dtl_rand_cube on pge.revn_dtl_rand
   dimensions geog_key, revn_yr_mo, srvpln_key
   groups (geog_key, revn_yr_mo, srvpln_key) g1, (revn_yr_mo, srvpln_key) g2
   measures sum(rsd_svc_bilg_amt), sum(rsd_st_slstx_amt),
   sum(rsd_trspt_chrg_amt), sum(rsd_balg_chrg_amt), sum(rsd_top_srchrg_amt),
   sum(rsd_ccoga_amt), sum(rsd_ccogc_amt), sum(rsd_cust_chrg_amt),
   sum(rsd_dmnd_chrg_amt), avg(usg_val), avg(usg_bill_therm), avg(usg_bill_kwh),
   avg(rsd_svc_bilg_amt)
tablespace PGE;
