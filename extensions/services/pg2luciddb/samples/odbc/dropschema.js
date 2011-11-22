var conn = WScript.CreateObject("ADODB.Connection");
// replace with your ODBC datasource name:
conn.Open("Provider=MSDASQL; DSN=PGSQL");

// write status:
WScript.Echo("Established connection to PG2LucidDB");

// execute command:
conn.Execute("drop schema PG2LUCIDDBTEST cascade");

WScript.Echo("PG2LUCIDDBTEST schema has been successfully deleted");


// close connection:
conn.Close();
conn = null;