// open connection:
var conn = WScript.CreateObject("ADODB.Connection");
// replace with your ODBC datasource name:
conn.Open("Provider=MSDASQL; DSN=PGSQL");

// write status:
WScript.Echo("Established connection to PG2LucidDB");

// get list of locations:
WScript.Echo("LOCATIONS table");
WScript.Echo("ID	NAME	MANAGER	SQUARE	ISFASTFOOD	ISCLOSED	DATEADDED	DATEOPENED");
WScript.Echo("");	

var SQL = "SELECT * FROM PG2LUCIDDBTEST.LOCATION ORDER BY ID";
var rs = conn.Execute(SQL);
while (!rs.EOF)
{
    WScript.Echo(rs.Fields(0).Value + "\t" + rs.Fields(1).Value + "\t" + rs.Fields(2).Value + "\t" + rs.Fields(3).Value + "\t" + rs.Fields(4).Value + "\t" + rs.Fields(5).Value + "\t" + rs.Fields(6).Value + "\t" + rs.Fields(7).Value);
    rs.MoveNext();
}
rs.Close();
rs = null;


// get list of SKUs:
WScript.Echo("");
WScript.Echo("SKU table");
WScript.Echo("ID	NAME	PRICE	CURRENCY");
WScript.Echo("");

SQL = "SELECT ID, NAME, PRICE, CURRENCY FROM PG2LUCIDDBTEST.SKU ORDER BY ID";
rs = conn.Execute(SQL);

while (!rs.EOF)
{
    WScript.Echo(rs.Fields(0).Value + "\t" + rs.Fields(1).Value + "\t" + rs.Fields(2).Value + "\t" + rs.Fields(3).Value);
    rs.MoveNext();
}
rs.Close();
rs = null;

// get sales fact:
WScript.Echo("");
WScript.Echo("SALES_FACT for the date 2009-09-23");
WScript.Echo("TRANSACTION_ID	LOCATION	SKU	TIME	DATE");
WScript.Echo("");

conn.Execute("SET SCHEMA 'PG2LUCIDDBTEST'");
SQL = "SELECT T1.TRANSACTIONID, T2.NAME, T3.NAME, T1.TRANSACTIONTIME, T1.TRANSACTIONDATE FROM SALES_FACT T1 INNER JOIN LOCATION T2 ON T2.ID = T1.LOCATIONID INNER JOIN SKU T3 ON T3.ID = T1.SKUID WHERE T1.TRANSACTIONDATE = APPLIB.CHAR_TO_DATE('yyyy-M-d', '2009-09-23') ORDER BY T1.TRANSACTIONID";

rs = conn.Execute(SQL);

while (!rs.EOF)
{
    WScript.Echo(rs.Fields(0).Value + "\t" + rs.Fields(1).Value + "\t" + rs.Fields(2).Value + "\t" + rs.Fields(3).Value + "\t" + rs.Fields(4).Value);
    rs.MoveNext();
}
rs.Close();
rs = null;

// close connection:
conn.Close();
conn = null;