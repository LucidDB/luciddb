<?php

 // TODO: change your connection string here
 $connectionString = "host=localhost port=9999 dbname=LOCALDB user=sa password=";

 // open connection to the server
 $connection = pg_connect($connectionString) or die ("Unable to connect to PG2LucidDB: " . pg_last_error($connection)); 

 echo("Established connection to PG2LucidDB\n");

 // get server version:
 $result = pg_query("select version()");

 if ($result)
 {
     $row = pg_fetch_row($result);
     echo("Server version: $row[0]\n");
     pg_free_result($result);
 }

 // close connection:
 pg_close($connection);  

?>