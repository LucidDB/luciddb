<?php

 // TODO: change your connection string here
 $connectionString = "host=localhost port=9999 dbname=LOCALDB user=sa password=";

 // open connection to the server
 $connection = pg_connect($connectionString) or die ("Unable to connect to PG2LucidDB: " . pg_last_error($connection)); 

 echo("Established connection to PG2LucidDB\n");

 // drop schema:
 pg_query("DROP SCHEMA PG2LUCIDDBTEST CASCADE");

 echo("PG2LUCIDDBTEST schema has been successfully deleted\n");

 // close connection:
 pg_close($connection);  

?>