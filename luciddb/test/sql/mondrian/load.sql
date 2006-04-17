set schema 'foodmart';
create schema foodmart_foreign;
import foreign schema "dbo" from server mssql_server_foodmart into foodmart_foreign;
--insert into foodmart."sales_fact_1997" select "product_id","time_id","customer_id","promotion_id","store_id","store_sales","store_cost","unit_sales" from foodmart_foreign."sales_fact_1997";

insert into foodmart."sales_fact_1997" select * from foodmart_foreign."sales_fact_1997";
insert into foodmart."sales_fact_1998" select * from foodmart_foreign."sales_fact_1998";

insert into foodmart."sales_fact_dec_1998" select * from foodmart_foreign."sales_fact_dec_1998";



insert into foodmart."inventory_fact_1997" select * from foodmart_foreign."inventory_fact_1997";



insert into foodmart."inventory_fact_1998" select * from foodmart_foreign."inventory_fact_1998";



insert into foodmart."agg_pl_01_sales_fact_1997" select * from foodmart_foreign."agg_pl_01_sales_fact_1997";



insert into foodmart."agg_ll_01_sales_fact_1997" select * from foodmart_foreign."agg_ll_01_sales_fact_1997";



insert into foodmart."agg_l_03_sales_fact_1997" select * from foodmart_foreign."agg_l_03_sales_fact_1997";



insert into foodmart."agg_l_04_sales_fact_1997" select * from foodmart_foreign."agg_l_04_sales_fact_1997";



insert into foodmart."agg_l_05_sales_fact_1997" select * from foodmart_foreign."agg_l_05_sales_fact_1997";



insert into foodmart."agg_c_10_sales_fact_1997" select * from foodmart_foreign."agg_c_10_sales_fact_1997";



insert into foodmart."agg_c_14_sales_fact_1997" select * from foodmart_foreign."agg_c_14_sales_fact_1997";



insert into foodmart."agg_lc_100_sales_fact_1997" select * from foodmart_foreign."agg_lc_100_sales_fact_1997";



insert into foodmart."agg_c_special_sales_fact_1997" select * from foodmart_foreign."agg_c_special_sales_fact_1997";



insert into foodmart."agg_g_ms_pcat_sales_fact_1997" select * from foodmart_foreign."agg_g_ms_pcat_sales_fact_1997";



insert into foodmart."currency" select * from foodmart_foreign."currency";



insert into foodmart."account" select * from foodmart_foreign."account";



insert into foodmart."category" select * from foodmart_foreign."category";



insert into foodmart."customer" select * from foodmart_foreign."customer";



insert into foodmart."days" select * from foodmart_foreign."days";



insert into foodmart."department" select * from foodmart_foreign."department";



insert into foodmart."employee" select * from foodmart_foreign."employee";



insert into foodmart."employee_closure" select * from foodmart_foreign."employee_closure";



insert into foodmart."expense_fact" select * from foodmart_foreign."expense_fact";



insert into foodmart."position" select * from foodmart_foreign."position";



insert into foodmart."product" select * from foodmart_foreign."product";



insert into foodmart."product_class" select * from foodmart_foreign."product_class";



insert into foodmart."promotion" select * from foodmart_foreign."promotion";



insert into foodmart."region" select * from foodmart_foreign."region";



insert into foodmart."reserve_employee" select * from foodmart_foreign."reserve_employee";



insert into foodmart."salary" select * from foodmart_foreign."salary";



insert into foodmart."store" select * from foodmart_foreign."store";



insert into foodmart."store_ragged" select * from foodmart_foreign."store_ragged";



insert into foodmart."time_by_day" select * from foodmart_foreign."time_by_day";


insert into foodmart."warehouse" select * from foodmart_foreign."warehouse";



insert into foodmart."warehouse_class" select * from foodmart_foreign."warehouse_class";
