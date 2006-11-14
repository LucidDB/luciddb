set schema 'foodmart_test';
select "time_by_day"."the_year" from "time_by_day" as "time_by_day" group by "time_by_day"."the_year" order by "time_by_day"."the_year" ;
select "time_by_day"."the_year" from "time_by_day" as "time_by_day" group by "time_by_day"."the_year" order by "time_by_day"."the_year" ;
select "product_class"."product_family", "product_class"."product_department" from "product" as "product", "product_class" as "product_class" where "product"."product_class_id" = "product_class"."product_class_id" group by "product_class"."product_family", "product_class"."product_department" order by "product_class"."product_family" , "product_class"."product_department" ;
select "customer"."education" from "customer" as "customer" group by "customer"."education" order by "customer"."education" ;
