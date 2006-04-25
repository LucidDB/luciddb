set schema 'foodmart';
select count(distinct "product_class"."product_family") from "product_class" as "product_class";
