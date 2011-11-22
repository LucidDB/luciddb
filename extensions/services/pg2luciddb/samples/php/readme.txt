To run the samples don't forget to change connection string with your own values and enable
php_pgsql extension in php.ini

On Windows platform additional dependency is libpq.dll which can be grabbed from PostgreSQL bin directory or
extracted from the archive with psql utility with the rest of required dll's (http://code.google.com/p/pg2luciddb/). 
All of them should be placed either in c:\windows\system32 directory or available in your PATH

Included scripts:

connect.php      - just connect to the server
createschema.php - create test schema (PG2LUCIDDBTEST with 3 tables)
dropschema.php   - drop test schema
fetchdata.php    - fetch data from the test schema

