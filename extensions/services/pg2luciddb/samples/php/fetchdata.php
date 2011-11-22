<?php

function print_table($result)
{
    $numrows = pg_num_rows($result);
    $fnum = pg_num_fields($result);

    for ($x = 0; $x < $fnum; $x++) {
        echo strtoupper(pg_field_name($result, $x));
        echo "\t";
    }

    echo "\n\n";

    for ($i = 0; $i < $numrows; $i++) 
    {
        $row = pg_fetch_object($result, $i);

        for ($x = 0; $x < $fnum; $x++) 
        {
            $fieldname = pg_field_name($result, $x);    
    	    echo $row->$fieldname . "\t";
        }
        echo"\n";
    }
}

 // TODO: change your connection string here
 $connectionString = "host=localhost port=9999 dbname=LOCALDB user=sa password=";

 // open connection to the server
 $connection = pg_connect($connectionString) or die ("Unable to connect to PG2LucidDB: " . pg_last_error($connection)); 

 echo("Established connection to PG2LucidDB\n");

 // get list of locations:
 echo("\nLOCATIONS table\n");
 $SQL = "SELECT * FROM PG2LUCIDDBTEST.LOCATION ORDER BY ID";

 $result = pg_query($connection, $SQL);
 print_table($result);
 pg_free_result($result);

 // get list of SKUs:
 echo("\n\nSKU table\n");  
 $SQL = "SELECT ID, NAME, PRICE, CURRENCY FROM PG2LUCIDDBTEST.SKU ORDER BY ID";

 $result = pg_query($connection, $SQL);
 print_table($result);
 pg_free_result($result);
  
 // get sales fact:
 echo("\n\nSALES_FACT for the date 2009-09-23\n");
 pg_query($connection, "SET SCHEMA 'PG2LUCIDDBTEST'");
 $SQL = "SELECT T1.TRANSACTIONID, T2.NAME, T3.NAME, T1.TRANSACTIONTIME, T1.TRANSACTIONDATE FROM SALES_FACT T1 INNER JOIN LOCATION T2 ON T2.ID = T1.LOCATIONID INNER JOIN SKU T3 ON T3.ID = T1.SKUID WHERE T1.TRANSACTIONDATE = APPLIB.CHAR_TO_DATE('yyyy-M-d', '2009-09-23') ORDER BY T1.TRANSACTIONID";
 
 $result = pg_query($connection, $SQL);
 print_table($result);
 pg_free_result($result);

 // close connection:
 pg_close($connection);  

?>