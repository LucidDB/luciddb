--
-- BBRange datatype conversion tests
--

-- list all values
select price from products order by price;

-- conversion from Q=0 to Q=2
select price from products where price > 7 order by price;
select price from products where price >= 7 order by price;

-- no conversion
select price from products where price > 7.81 order by price;
select price from products where price >= 7.81 order by price;

-- conversion from Q=3 to Q=2 (rounding occurs)
select price from products where price > 7.809 order by price;
select price from products where price >= 7.809 order by price;
select price from products where price > 7.811 order by price;
select price from products where price >= 7.811 order by price;

-- conversion from Q=0 to Q=2
select price from products where price < 2 order by price;
select price from products where price <= 2 order by price;

-- no conversion
select price from products where price < 2.95 order by price;
select price from products where price <= 2.95 order by price;

-- conversion from Q=3 to Q=2 (rounding occurs)
select price from products where price < 2.949 order by price;
select price from products where price <= 2.949 order by price;
select price from products where price < 2.951 order by price;
select price from products where price <= 2.951 order by price;
