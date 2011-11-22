To run samples just execute the scripts in console window (cmd) using cscript:

cscript <script.js>

---------------------------------------------------------------------------
Prerequirements
---------------------------------------------------------------------------

1. Download & install latest ODBC provider for PostgreSQL. Current version 
   of PG2LucidDB has been successfully tested with psqlodbc version 08.04.0100 for Win32:

   http://wwwmaster.postgresql.org/download/mirrors-ftp/odbc/versions/msi/psqlodbc_08_04_0100.zip

   Version for 64-bit Windows can be found here:

   http://code.google.com/p/visionmap/wiki/psqlODBC


2. Setup system data source using installed PostgreSQL Unicode ODBC driver
   (ANSI version has not beed tested yet, BTW - what's the reason for it?).

   Required settings:

   "Data source" - name of your DSN, choose any
   "Database" - should be LOCALDB
   "Server" - IP or host name of your server with PG2LucidDB bridge
   "Username" - username
   "SSL Mode" - should be disabled
   "Port" - choose the same as you have in PG2LucidDB.properties

   Advanced recommended ODBC driver settings (Datasource button):

   "Text as LongVarChar" - should be turned on
   "Recognize Unique Indexes" - should be turned off
   "Updatable Cursors" - should be turned off
   "Bools as Char" - should be turned off

   All the rest should be left as it was during DSN setup.


3. Run sample scripts


Included scripts:

connect.js      - just connect to the server
createschema.js - create test schema (PG2LUCIDDBTEST with 3 tables)
dropschema.js   - drop test schema
fetchdata.js    - fetch data from the test schema
prepared.js     - prepared statements test
datatypes.js    - data types mapping test

---------------------------------------------------------------------------
Known limitations
---------------------------------------------------------------------------

Due to the existing solid differences in supported SQL syntax & architecture between LucidDB
and PostgreSQL not all functions / features are supported yet and probably it will be
impossible to implement all the things. Current issues and limitations are mainly caused by
the way how ODBC provider gets meta data from the database (schema, tables, columns, etc).

Theoretically speaking it's possible to hack ODBC driver and rewrite it taking into consideration all 
differences between LucidDB and PostgreSQL. Anyway current bridge's version works pretty fine with ODBC and 
already gives ability to query LucidDB from almost any language / third-party application with built-in ODBC support.

What is still not working yet:
_____________________________

1. Updateable cursors (because LucidDB doesn't have internal object's id column similar to oid / tid in Postgres,
   if you have an idea how to do it - please drop me an e-mail)
2. Column constraints, rules, indexes, SP's and functions are not being discovered using schema retrieval queries

Pay additional attention to:
_____________________________

1. Basic schema retrieval queries (tables & views, columns, namespaces) should work.
2. PostgreSQL treats schema names, tables & other system objects' names are case insensitive. To make all
   schema discovery queries working as they should do, please use ONLY uppercase names for tables, columns, schemas
   especially from third-party ODBC tools
2. Commands with parameters. Please note that current version of ODBC provider doesn't use prepared statements and tries to do
   all parameters binding at client's side what can result in PostgreSQL-specific type casting. Sometimes it can
   result in queries which are not valid for LucidDB (i.e. ODBC provider tends to send decimal / numeric values as 
   typical quoted string values what is not acceptable for LucidDB). The simplest way to overcome it is just to find
   possible data type substitute (see createschema.js for example) or don't use query parameters at all. 
3. All LucidDB data types are supported with the following limitations: CHAR data type is allways treated as VARCHAR.

Current version has been successfully tested with the following third-party tools:

1. Microsoft Excel (using data import tool & Microsoft Query). Please keep in mind that MS Query has very limited
   namespaces (schemas) support, that's why be ready that visual query builder will not work propertly and you will
   have to modify the query manually

2. Linked servers for MS SQL Server 2005 / 2008. Works pretty good both using OPENQUERY() method and direct
   requests to linked server (select * from <LINKED SERVER NAME>.<DBNAME>.<SCHEMA>.<TABLE>). Updates and inserts
   can be done using exec query ( exec('<your query>') at <LINKED SERVER NAME> )

3. Microsoft SSIS with ODBC data source

4. Microsoft Analysis Services 2000. Date/time dimensions are not working at the moment. Also you will
   have to rename the table in the CUBE editor (create alias for it), because Analysis services treats it incorrectly
   trying to combine schema name with table name.    
