CREATE foreign table csv_schema.SHORTREG_PRODUCTS_SRC (
PRODID INTEGER,
NAME VARCHAR(30),
PRICE DOUBLE
)
server csv_server
options (table_name 'products');

INSERT INTO s.PRODUCTS
SELECT PRODID,NAME,PRICE
FROM csv_schema.SHORTREG_PRODUCTS_SRC;