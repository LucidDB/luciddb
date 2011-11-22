// function to resolve datatype:
function ResolveDataType(typeId)
{
    switch(typeId)
    {
        case 20:
          return "adBigInt";
        case 128:
          return "adBinary";
        case 11:
          return "adBoolean";
        case 8:
          return "adBSTR";
        case 129:
          return "adChar";
        case 6:
          return "adCurrency";
        case 7:
          return "adDate";
        case 133:
          return "adDBDate";
        case 134:
          return "adDBTime";
        case 135:
          return "adDBTimeStamp";
        case 14:
          return "adDecimal";
        case 5:
          return "adDouble";
        case 3:
          return "adInteger";
        case 2:
          return "adSmallInt";
        case 201:
          return "adLongVarChar";
        case 131:
          return "adNumeric";
        case 4:
          return "adSingle";
        case 200:
          return "adVarChar";
        case 202:
          return "adVarWChar";
        
        default:
          return "adUnknown";
       
    }
}

// open connection:
var conn = WScript.CreateObject("ADODB.Connection");
// replace with your ODBC datasource name:
conn.Open("Provider=MSDASQL; DSN=PGSQL");

// write status:
WScript.Echo("Established connection to PG2LucidDB");

// locations:
WScript.Echo("LOCATIONS table");
WScript.Echo("");	

var SQL = "SELECT * FROM PG2LUCIDDBTEST.LOCATION ORDER BY ID";
var rs = conn.Execute(SQL);
if (!rs.EOF)
{
    for (var i = 0; i < rs.Fields.Count; i++)
    {
         WScript.Echo(rs.Fields(i).Name + " = " + ResolveDataType(rs.Fields(i).Type) + ", " + typeof(rs.Fields(i).Value) + " (" + rs.Fields(i).Value + ")" );
    }
}
rs.Close();
rs = null;

// sku:
WScript.Echo("");
WScript.Echo("SKU table");
WScript.Echo("");

var SQL = "SELECT * FROM PG2LUCIDDBTEST.SKU ORDER BY ID";
var rs = conn.Execute(SQL);
if (!rs.EOF)
{
    for (var i = 0; i < rs.Fields.Count; i++)
    {
         WScript.Echo(rs.Fields(i).Name + " = " + ResolveDataType(rs.Fields(i).Type) + ", " + typeof(rs.Fields(i).Value) + " (" + rs.Fields(i).Value + ")" );
    }
}
rs.Close();
rs = null;
	
// sales fact:
WScript.Echo("");
WScript.Echo("SALES_FACT table");
WScript.Echo("");

var SQL = "SELECT * FROM PG2LUCIDDBTEST.SALES_FACT";
var rs = conn.Execute(SQL);
if (!rs.EOF)
{
    for (var i = 0; i < rs.Fields.Count; i++)
    {
         WScript.Echo(rs.Fields(i).Name + " = " + ResolveDataType(rs.Fields(i).Type) + ", " + typeof(rs.Fields(i).Value) + " (" + rs.Fields(i).Value + ")" );
    }
}
rs.Close();
rs = null;

conn.Close();