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

// open connection:
var conn = WScript.CreateObject("ADODB.Connection");
// replace with your ODBC datasource name:
conn.Open("Provider=MSDASQL; DSN=PGSQL");

// write status:
WScript.Echo("Established connection to PG2LucidDB");

// get list of locations:
WScript.Echo("LOCATIONS table");

var cmd = WScript.CreateObject("ADODB.Command");
cmd.ActiveConnection = conn;
cmd.CommandText = "SELECT * FROM PG2LUCIDDBTEST.LOCATION WHERE ID = ? ORDER BY ID";
var rs = cmd.Execute(null, [2]);

// write column names:
var tmpHeader = "";
for (var i = 0; i < rs.Fields.Count; i++)
{
    tmpHeader += rs.Fields(i).Name + "\t";    
}
WScript.Echo(tmpHeader);
WScript.Echo("");

// write data:
while (!rs.EOF) 
{                                   
    var tmpLine = "";
    for (var i = 0; i < rs.Fields.Count; i++)
    {
        tmpLine += rs.Fields(i).Value + "\t";
    }
    WScript.Echo(tmpLine);
    rs.MoveNext();
}
rs.Close();
rs = null;

// execute second request:
var rs = cmd.Execute(null, [1]);

// write data:
while (!rs.EOF) 
{                                   
    var tmpLine = "";
    for (var i = 0; i < rs.Fields.Count; i++)
    {
        tmpLine += rs.Fields(i).Value + "\t";
    }
    WScript.Echo(tmpLine);
    rs.MoveNext();
}
rs.Close();
rs = null;

// sku table:
WScript.Echo("");
WScript.Echo("SKU table");

cmd = WScript.CreateObject("ADODB.Command");
cmd.ActiveConnection = conn;

// execute request with explicit parameters:
cmd.CommandText = "SELECT ID, NAME, PRICE, CURRENCY FROM PG2LUCIDDBTEST.SKU WHERE CURRENCY = ? AND PRICE >= ? ORDER BY NAME ASC";
cmd.Parameters.Append(cmd.CreateParameter("currency", adVarChar, adParamInput, 255, "RUR"));
cmd.Parameters.Append(cmd.CreateParameter("price", adDouble, adParamInput, 0, 175.50));

var rs = cmd.Execute();

// write column names:
tmpHeader = "";
for (var i = 0; i < rs.Fields.Count; i++)
{
    tmpHeader += rs.Fields(i).Name + "\t";    
}
WScript.Echo(tmpHeader);
WScript.Echo("");

// write data:
while (!rs.EOF) 
{                                   
    var tmpLine = "";
    for (var i = 0; i < rs.Fields.Count; i++)
    {
        tmpLine += rs.Fields(i).Value + "\t";
    }
    WScript.Echo(tmpLine);
    rs.MoveNext();
}
rs.Close();
rs = null;

// set parameters' value:
cmd.Parameters("currency").Value = "EUR";
cmd.Parameters("price").Value = 0.1;

var rs = cmd.Execute();

// write data:
while (!rs.EOF) 
{                                   
    var tmpLine = "";
    for (var i = 0; i < rs.Fields.Count; i++)
    {
        tmpLine += rs.Fields(i).Value + "\t";
    }
    WScript.Echo(tmpLine);
    rs.MoveNext();
}
rs.Close();
rs = null;

// close connection:
conn.Close();
conn = null;