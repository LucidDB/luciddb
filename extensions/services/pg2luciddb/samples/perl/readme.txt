To run the samples, please check first that you have module DBD::Pg (http://search.cpan.org/~turnstep/DBD-Pg-2.15.1/Pg.pm) 
installed. Also do not forget to change connection settings in sample scripts:

$host - host name with PG2LucidDB bridge installed
$port - port (9999 by default)
$dbuser - username
$dbpass - password

On Windows platform additional dependency is libpq.dll which can be grabbed from PostgreSQL bin directory or
extracted from the archive with psql utility with the rest of required dll's (http://code.google.com/p/pg2luciddb/). 
All of them should be placed either in c:\windows\system32 directory or available in your PATH

Included scripts:

connect.pl      - just connect to the server
createschema.pl - create test schema (PG2LUCIDDBTEST)
dropschema.pl   - drop test schema
fetchdata.pl    - fetch data from the test schema
prepared.pl     - prepared statements test

