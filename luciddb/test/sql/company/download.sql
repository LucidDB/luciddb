-- $Id$

--
-- Download company data from SQL (some) Server
--

-- setup link to SQL server
-- CREATE SCHEMA SQLSERVER USING LINK ODBC_SQLSERVER
-- DEFINED BY 'BENCHMARK.dbo';

-- /creschema.sql

create foreign data wrapper orcl_jdbc
library '${FARRAGO_HOME}/plugin/FarragoMedJdbc3p.jar'
language java;

create server orcl_server
foreign data wrapper orcl_jdbc
options(
    url 'jdbc:oracle:thin:@akela.lucidera.com:1521:XE',
    user_name 'schoi',
    password 'schoi',
    driver_class 'oracle.jdbc.driver.OracleDriver'
);

set schema 's';

-- Download
INSERT INTO LOCATION SELECT * FROM orcl_server."SCHOI".LOCATION;
INSERT INTO DEPT SELECT * FROM orcl_server."SCHOI".DEPT;
INSERT INTO EMP SELECT * FROM orcl_server."SCHOI".EMP;
INSERT INTO CUSTOMERS SELECT * FROM orcl_server."SCHOI".CUSTOMERS;
INSERT INTO PRODUCTS SELECT * FROM orcl_server."SCHOI".PRODUCTS;
INSERT INTO SALES SELECT * FROM orcl_server."SCHOI".SALES;

-- Check the download
select count(*) from location;
select count(*) from dept;
select count(*) from emp;
select count(*) from customers;
select count(*) from products;
select count(*) from sales;

-- analyze the columns to get the statistics
-- analyze table EMP estimate statistics for all columns SAMPLE 100 PERCENT;
-- analyze table PRODUCTS estimate statistics for all columns SAMPLE 100 PERCENT;
-- analyze table SALES estimate statistics for all columns SAMPLE 100 PERCENT;
-- analyze table CUSTOMERS estimate statistics for all columns SAMPLE 100 PERCENT;
-- analyze table LOCATION estimate statistics for all columns SAMPLE 100 PERCENT;
-- analyze table DEPT estimate statistics for all columns SAMPLE 100 PERCENT;
