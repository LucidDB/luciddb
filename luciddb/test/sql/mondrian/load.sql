set schema 'foodmart_test';
create schema foodmart_foreign;
import foreign schema "dbo" from server mssql_server_foodmart into foodmart_foreign;
--insert into foodmart_test."sales_fact_1997" select "product_id","time_id","customer_id","promotion_id","store_id","store_sales","store_cost","unit_sales" from foodmart_foreign."sales_fact_1997";

insert into foodmart_test."sales_fact_1997" select * from foodmart_foreign."sales_fact_1997";
insert into foodmart_test."sales_fact_1998" select * from foodmart_foreign."sales_fact_1998";

insert into foodmart_test."sales_fact_dec_1998" select * from foodmart_foreign."sales_fact_dec_1998";



insert into foodmart_test."inventory_fact_1997" select * from foodmart_foreign."inventory_fact_1997";



insert into foodmart_test."inventory_fact_1998" select * from foodmart_foreign."inventory_fact_1998";



insert into foodmart_test."agg_pl_01_sales_fact_1997" select * from foodmart_foreign."agg_pl_01_sales_fact_1997";



insert into foodmart_test."agg_ll_01_sales_fact_1997" select * from foodmart_foreign."agg_ll_01_sales_fact_1997";



insert into foodmart_test."agg_l_03_sales_fact_1997" select * from foodmart_foreign."agg_l_03_sales_fact_1997";



insert into foodmart_test."agg_l_04_sales_fact_1997" select * from foodmart_foreign."agg_l_04_sales_fact_1997";



insert into foodmart_test."agg_l_05_sales_fact_1997" select * from foodmart_foreign."agg_l_05_sales_fact_1997";



insert into foodmart_test."agg_c_10_sales_fact_1997" select * from foodmart_foreign."agg_c_10_sales_fact_1997";



insert into foodmart_test."agg_c_14_sales_fact_1997" select * from foodmart_foreign."agg_c_14_sales_fact_1997";



insert into foodmart_test."agg_lc_100_sales_fact_1997" select * from foodmart_foreign."agg_lc_100_sales_fact_1997";



insert into foodmart_test."agg_c_special_sales_fact_1997" select * from foodmart_foreign."agg_c_special_sales_fact_1997";



insert into foodmart_test."agg_g_ms_pcat_sales_fact_1997" select * from foodmart_foreign."agg_g_ms_pcat_sales_fact_1997";



insert into foodmart_test."currency" select * from foodmart_foreign."currency";



insert into foodmart_test."account" select * from foodmart_foreign."account";



insert into foodmart_test."category" select * from foodmart_foreign."category";



insert into foodmart_test."customer" select * from foodmart_foreign."customer";



insert into foodmart_test."days" select * from foodmart_foreign."days";



insert into foodmart_test."department" select * from foodmart_foreign."department";



insert into foodmart_test."employee" select * from foodmart_foreign."employee";



insert into foodmart_test."employee_closure" select * from foodmart_foreign."employee_closure";



insert into foodmart_test."expense_fact" select * from foodmart_foreign."expense_fact";



insert into foodmart_test."position" select * from foodmart_foreign."position";



insert into foodmart_test."product" select * from foodmart_foreign."product";



insert into foodmart_test."product_class" select * from foodmart_foreign."product_class";



insert into foodmart_test."promotion" select * from foodmart_foreign."promotion";



insert into foodmart_test."region" select * from foodmart_foreign."region";



insert into foodmart_test."reserve_employee" select * from foodmart_foreign."reserve_employee";



insert into foodmart_test."salary" select * from foodmart_foreign."salary";



insert into foodmart_test."store" select * from foodmart_foreign."store";



insert into foodmart_test."store_ragged" select * from foodmart_foreign."store_ragged";



insert into foodmart_test."time_by_day" select * from foodmart_foreign."time_by_day";


insert into foodmart_test."warehouse" select * from foodmart_foreign."warehouse";



insert into foodmart_test."warehouse_class" select * from foodmart_foreign."warehouse_class";
