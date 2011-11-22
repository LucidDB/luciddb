To build the samples run build.bat (don't forget to modify your connection string first)

PG2LucidDB Bridge has been successfully tested with Npgsql provider version 2.0.6.0 (included) and likely will
work properly with newer versions (the only possible issues can be within the code used to fetch available db data types
and the way how provider treats command parameters types)

Included samples:

connect.cs      - just connect to the server
createschema.cs - create test schema (PG2LUCIDDBTEST)
dropschema.cs   - drop test schema
fetchdata.cs    - fetch data from the test schema
datatypes.cs    - check data types mapping
schemaonly.cs   - check schema-only NpgsqlDataReader behaviour

