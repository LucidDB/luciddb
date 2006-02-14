-- Indexes for the PGE schema
-- Index 1 
create index csa_i1 on pge.cust_serv_acct(cust_serv_acct_key);

-- Index 2 
create index csa_i2 on pge.cust_serv_acct(is_dwlgs_val);

-- Index 3 
create index csa_i3 on pge.cust_serv_acct(geog_key);

-- Index 4 
create index csa_i4 on pge.cust_serv_acct(srvpln_key);

-- Index 5 
create index r_d_r_i1 on pge.revn_dtl_rand(sas_fnl_bil_indcr);

-- Index 6 
create index r_d_r_i2 on pge.revn_dtl_rand(cust_serv_acct_key);

-- Index 7 
create index r_d_r_i3 on pge.revn_dtl_rand(srvpln_key);

-- Index 8 
create index r_d_r_i4 on pge.revn_dtl_rand(geog_key);

-- Index 9 
create index r_d_r_i5 on pge.revn_dtl_rand(revn_yr_mo);

-- Index 10 
create index r_d_r_i6 on pge.revn_dtl_rand(rsd_cu_cnt);

-- Index 11 
create index r_d_r_i7 on pge.revn_dtl_rand(rsd_svc_bilg_amt);

-- Index 12 
create index revn_prd_pkey on pge.revn_prd(revn_yr_mo);

-- Index 13 
create index revn_prd_i1 on pge.revn_prd(revn_yr);

-- Index 14 
create index revn_prd_i2 on pge.revn_prd(revn_qtr_yr_desc);

-- Index 15 
create index revn_prd_i3 on pge.revn_prd(revn_yr_mo_desc);

-- Index 16 
create index geog_pkey on pge.geog(geog_key);

-- Index 17 
create index geog_i1 on pge.geog(rvtwn_desc);

-- Index 18 
create index geog_i2 on pge.geog(st_cd);

-- Index 19 
create index geog_i3 on pge.geog(geog_cnty_desc);

-- Index 20 
create index geog_i4 on pge.geog(st_desc);

-- Index 21 
create index serv_plan_pkey on pge.serv_plan(srvpln_key);

-- Index 22 
create index serv_plan_i1 on pge.serv_plan(srvpln_id);

-- Index 23 
create index serv_plan_i2 on pge.serv_plan(bu_desc);

-- Index 24 
create index serv_plan_i3 on pge.serv_plan(srvpln_nm);

-- Index 25 
create index serv_plan_i4 on pge.serv_plan(rc_desc);

-- Index 26 
create index serv_plan_i5 on pge.serv_plan(spo_nm);

-- Index 27 
create index serv_plan_i6 on pge.serv_plan(spo_id);
