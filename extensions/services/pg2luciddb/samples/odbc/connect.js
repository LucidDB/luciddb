var conn = WScript.CreateObject("ADODB.Connection");
// replace with your ODBC datasource name:
conn.Open("Provider=MSDASQL; DSN=PGSQL");

// write status:
WScript.Echo("Established connection to PG2LucidDB");

// get server's version:
var rs = conn.Execute("SELECT VERSION()");
if (rs != null && !rs.EOF)
{
    WScript.Echo("Server version: " + rs.Fields(0).Value);
}
rs.Close();

// close connection:
conn.Close();
conn = null;