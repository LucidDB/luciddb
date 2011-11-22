// ----------------------------------------------------
// ADO constants
// ----------------------------------------------------
var adParamInput = 0x0001;
var adInteger = 3;
var adBigInt = 20;
var adDecimal = 14;
var adNumeric = 131;
var adVarChar = 200;
var adSingle = 4;
var adDouble = 5;
var adCurrency = 6;
var adBoolean = 11;
var adDate = 7;
var adDBDate = 133;
var adDBTime = 134;
var adDBTimeStamp = 135;

// ----------------------------------------------------

// create connection:
var conn = WScript.CreateObject("ADODB.Connection");
// replace with your ODBC datasource name:
conn.Open("Provider=MSDASQL; DSN=PGSQL");

// write status:
WScript.Echo("Established connection to PG2LucidDB");

// execute command:
conn.Execute("CREATE SCHEMA PG2LUCIDDBTEST");
conn.Execute("SET SCHEMA 'PG2LUCIDDBTEST'");

WScript.Echo("PG2LUCIDDBTEST schema has been successfully created");

// create LOCATION table
var SQL = "CREATE TABLE LOCATION ( " +
  "Id integer not null primary key, " +
  "Name varchar(255) not null, " +
  "Manager varchar(128), " +
  "Square float not null, " +
  "IsFastFood boolean not null, " +
  "IsClosed boolean not null, " +
  "DateAdded timestamp not null, " +
  "DateOpened date not null " + 
  ")";

conn.Execute(SQL);

// append sample data:
SQL = "INSERT INTO LOCATION VALUES (1, 'Location #1', 'Manager for Location #1', 251.8, false, false, current_timestamp, APPLIB.CHAR_TO_DATE('yyyy-M-d', '2009-07-14'))";
conn.Execute(SQL);

// unicode test:
SQL = "INSERT INTO LOCATION VALUES (2, 'Ресторан #2', NULL, 58.4, true, false, current_timestamp, APPLIB.CHAR_TO_DATE('yyyy-M-d', '2008-02-01'))";
conn.Execute(SQL);

WScript.Echo("Table LOCATION has been successfully created");

// create SKU table:

SQL = "CREATE TABLE SKU ( " +
   "Id smallint not null primary key, " +
   "Name varchar(128) not null, " +
   "Price numeric(6, 2) not null, " +
   "Currency char(3) not null " +
")";

conn.Execute(SQL);

// insert data using statement with parameters
// PLEASE NOTICE: ODBC doesn't use prepared statements
SQL = "INSERT INTO SKU VALUES (?, ?, ?, ?)";

var command2 = WScript.CreateObject("ADODB.Command");
command2.activeConnection = conn;
command2.commandText = SQL;

// simplified command execution
// using that method it's impossible to insert decimal values without being rounded, it seems
// to be the problem (or feature?) of ADO. Please use manual parameters definition instead
command2.Execute(null, [1, "Espresso Dopio", 185, "RUR"]); 
command2 = null;

// manually specify parameters:
command2 = WScript.CreateObject("ADODB.Command");    
command2.ActiveConnection = conn;
command2.CommandText = "INSERT INTO SKU (id, name, price, currency) VALUES (?, ?, ?, ?)";

command2.Parameters.Append(command2.CreateParameter("id", adInteger, adParamInput, 0, 3));
command2.Parameters.Append(command2.CreateParameter("name", adVarChar, adParamInput, 255, "Hamburger"));
// FIX: PostgreSQL ODBC tends to send decimal & numeric parameters as varchar,  that's why - use double datatype instead:
var pPrice = command2.CreateParameter("price", adDouble, adParamInput, 0, 3.75);
command2.Parameters.Append(pPrice);
command2.Parameters.Append(command2.CreateParameter("currency", adVarChar, adParamInput, 3, "USD"));      
command2.Execute();         

// modify already existing parameters:
command2.Parameters("id").Value = 4;
command2.Parameters("name").Value = "Coca-cola";
command2.Parameters("price").Value = 0.8;
command2.Parameters("currency").Value = "EUR";
command2.Execute(); 

command2.Parameters("id").Value = 2;
command2.Parameters("name").Value = "Latte Medio";
command2.Parameters("price").Value = 175.5;
command2.Parameters("currency").Value = "RUR";
command2.Execute(); 
command2 = null;

WScript.Echo("Table SKU has been successfully created");

// create table SALES_FACT:
SQL = "CREATE TABLE SALES_FACT ( " +
   " TransactionId bigint not null primary key, " +
   " LocationId integer not null, " +
   " SKUId smallint not null, " +
   " TransactionTime time not null, " +
   " TransactionDate date not null, " +
   " Comments varchar(128) " +
" ) ";

conn.Execute(SQL);

// append data:
conn.Execute("INSERT INTO SALES_FACT VALUES (1, 1, 1, APPLIB.CHAR_TO_TIME('kk:mm', '08:10'), APPLIB.CHAR_TO_DATE('yyyy-M-d', '2009-09-23'), NULL)");
conn.Execute("INSERT INTO SALES_FACT VALUES (2, 1, 3, APPLIB.CHAR_TO_TIME('kk:mm', '08:10'), APPLIB.CHAR_TO_DATE('yyyy-M-d', '2009-09-23'), NULL)");
conn.Execute("INSERT INTO SALES_FACT VALUES (3, 2, 4, APPLIB.CHAR_TO_TIME('kk:mm', '13:15'), APPLIB.CHAR_TO_DATE('yyyy-M-d', '2009-09-24'), 'Promo')");

WScript.Echo("Table SALES_FACT has been successfully created");

// close connection:
conn.Close();
conn = null;