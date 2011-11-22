<?php

 // TODO: change your connection string here
 $connectionString = "host=localhost port=9999 dbname=LOCALDB user=sa password=";

 // open connection to the server
 $connection = pg_connect($connectionString) or die ("Unable to connect to PG2LucidDB: " . pg_last_error($connection)); 

 echo("Established connection to PG2LucidDB\n");

 // create schema:
 pg_query("CREATE SCHEMA PG2LUCIDDBTEST");
 // set current schema:
 pg_query("SET SCHEMA 'PG2LUCIDDBTEST'");

 echo("PG2LUCIDDBTEST schema has been successfully created\n");

 // create table Location:

 $SQL = "CREATE TABLE LOCATION ( " .
  "Id integer not null primary key, " . 
  "Name varchar(255) not null, " . 
  "Manager varchar(128), " . 
  "Square float not null, " .
  "IsFastFood boolean not null, " . 
  "IsClosed boolean not null, " . 
  "DateAdded timestamp not null, " . 
  "DateOpened date not null " .
 ")";

 pg_query($SQL);

 // append sample data:
 $SQL = "INSERT INTO LOCATION VALUES (1, 'Location #1', 'Manager for Location #1', 251.8, false, false, current_timestamp, APPLIB.CHAR_TO_DATE('yyyy-M-d', '2009-07-14'))";
 $count = 0;
 $result = pg_query($SQL);
 $count = $count + pg_affected_rows($result); 

 // unicode test:
 $SQL = "INSERT INTO LOCATION VALUES (2, 'Ресторан #2', NULL, 58.4, true, false, current_timestamp, APPLIB.CHAR_TO_DATE('yyyy-M-d', '2008-02-01'))";
 $result = pg_query($SQL);
 $count = $count + pg_affected_rows($result); 

 echo("Table LOCATION has been successfully created, $count rows added\n");

 // create table SKU:

 $SQL = "CREATE TABLE SKU ( " .
   "Id smallint not null primary key, " .
   "Name varchar(128) not null, " . 
   "Price numeric(6, 2) not null, " . 
   "Currency char(3) not null " . 
 ")";

 pg_query($SQL);

 // insert data using prepared statement:
 $SQL = "INSERT INTO SKU VALUES ($1, $2, $3, $4)";
 $result = pg_prepare($connection, "my_query", $SQL);

 // append data:
 $count = 0;
 $result = pg_execute($connection, "my_query", array(1, "Espresso Dopio", 185, "RUR"));
 $count = $count + pg_affected_rows($result); 

 $result = pg_execute($connection, "my_query", array(2, "Latte Medio", 175.50, "RUR"));
 $count = $count + pg_affected_rows($result); 

 $result = pg_execute($connection, "my_query", array(3, "Hamburger", 3.75, "USD"));
 $count = $count + pg_affected_rows($result); 

 $result = pg_execute($connection, "my_query", array(4, "Coca-cola", 0.8, "EUR"));
 $count = $count + pg_affected_rows($result); 
 
 echo("Table SKU has been successfully created, $count rows added\n");

 // create table SALES:

 $SQL = "CREATE TABLE SALES_FACT ( " . 
   "TransactionId bigint not null primary key, " . 
   "LocationId integer not null, " . 
   "SKUId smallint not null, " .
   "TransactionTime time not null, " . 
   "TransactionDate date not null, " . 
   "Comments varchar(128) " . 
 ")";

 pg_query($SQL);

 // append sample data:
 $count = 0;
 $result = pg_query("INSERT INTO SALES_FACT VALUES (1, 1, 1, APPLIB.CHAR_TO_TIME('kk:mm', '08:10'), APPLIB.CHAR_TO_DATE('yyyy-M-d', '2009-09-23'), NULL)");
 $count = $count + pg_affected_rows($result); 

 $result = pg_query("INSERT INTO SALES_FACT VALUES (2, 1, 3, APPLIB.CHAR_TO_TIME('kk:mm', '08:10'), APPLIB.CHAR_TO_DATE('yyyy-M-d', '2009-09-23'), NULL)");
 $count = $count + pg_affected_rows($result); 

 $result = pg_query("INSERT INTO SALES_FACT VALUES (3, 2, 4, APPLIB.CHAR_TO_TIME('kk:mm', '13:15'), APPLIB.CHAR_TO_DATE('yyyy-M-d', '2009-09-24'), 'Promo')");
 $count = $count + pg_affected_rows($result); 
 
 echo("Table SALES_FACT has been successfully created, $count rows added\n"); 

 // close connection:
 pg_close($connection);  

?>