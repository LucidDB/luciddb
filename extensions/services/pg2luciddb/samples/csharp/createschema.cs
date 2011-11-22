using System;
using System.Data;
using Npgsql;
using NpgsqlTypes;

class PostgreSQL2LucidDBTest
{
    static public void Main()
    {
        // TODO: change to your real values
        string connString = "Server=127.0.0.1; Port=9999; Database=LOCALDB; User Id=sa; Password=";

        NpgsqlConnection conn = null;
        try
        {
            conn = new NpgsqlConnection(connString);
            conn.Open();
            Console.WriteLine("Established connection to PG2LucidDB");
 
            // create schema:
            NpgsqlCommand comm = conn.CreateCommand();
            comm.CommandText = "CREATE SCHEMA PG2LUCIDDBTEST";
            comm.ExecuteNonQuery();
            comm.CommandText = "SET SCHEMA 'PG2LUCIDDBTEST'"; 
            comm.ExecuteNonQuery();

            // create table LOCATUON:
	    Console.WriteLine("PG2LUCIDDBTEST schema has been successfully created");

            // create LOCATION table
	    string SQL = "CREATE TABLE LOCATION ( " +
  		  "Id integer not null primary key, " +
		  "Name varchar(255) not null, " +
		  "Manager varchar(128), " +
		  "Square float not null, " +
		  "IsFastFood boolean not null, " +
		  "IsClosed boolean not null, " +
		  "DateAdded timestamp not null, " +
		  "DateOpened date not null " + 
		  ")";

	    comm.CommandText = SQL;
            comm.ExecuteNonQuery();

            // append sample data:
            SQL = "INSERT INTO LOCATION VALUES (1, 'Location #1', 'Manager for Location #1', 251.8, false, false, current_timestamp, APPLIB.CHAR_TO_DATE('yyyy-M-d', '2009-07-14'))";
            comm.CommandText = SQL;
            comm.ExecuteNonQuery();

            // run query with parameters:
            SQL = "INSERT INTO LOCATION VALUES (:id, :name, :manager, :square, :isfastfood, :isclosed, :dateadded, :dateopened)";
            comm.CommandText = SQL;

            // append params:
            comm.Parameters.Add(":id", 2);
            comm.Parameters.Add(":name", "Ресторан #2");
            comm.Parameters.Add(":manager", System.DBNull.Value);
            // FIX: numeric type should be specified
            NpgsqlParameter sp = comm.Parameters.Add(":square", NpgsqlDbType.Numeric);
            sp.Value = 58.4;
	    comm.Parameters.Add(":isfastfood", true);
	    comm.Parameters.Add(":isclosed", false);
            // timestamp:
	    NpgsqlParameter sp2 = comm.Parameters.Add(":dateadded", NpgsqlDbType.Timestamp);
            sp2.Value = DateTime.Now;
            // date:
	    NpgsqlParameter sp3 = comm.Parameters.Add(":dateopened", NpgsqlDbType.Date);
            sp3.Value = new DateTime(2008, 2, 1);
            comm.ExecuteNonQuery();

            Console.WriteLine("Table LOCATION has been successfully created");

            // create SKU table:
            SQL = "CREATE TABLE SKU ( " +
		   "Id smallint not null primary key, " +
		   "Name varchar(128) not null, " +
		   "Price numeric(6, 2) not null, " +
		   "Currency char(3) not null " +
		")";

            comm = conn.CreateCommand();
            comm.CommandText = SQL;
            comm.ExecuteNonQuery();

            // fill values:
            comm.CommandText = "insert into SKU values (:id, :name, :price, :currency)";
            // append params:
            comm.Parameters.Add(":id", 1);
            comm.Parameters.Add(":name", "Espresso Dopio");
            comm.Parameters.Add(":price", NpgsqlDbType.Numeric);
            comm.Parameters[":price"].Value = 185;
            comm.Parameters.Add(":currency", "RUR");
            comm.ExecuteNonQuery();

            comm.Parameters[":id"].Value = 2;
            comm.Parameters[":name"].Value = "Latte Medio";
            comm.Parameters[":price"].Value = 175.50;
            comm.Parameters[":currency"].Value = "RUR";
            comm.ExecuteNonQuery();

            comm.Parameters[":id"].Value = 3;
            comm.Parameters[":name"].Value = "Hamburger";
            comm.Parameters[":price"].Value = 3.75;
            comm.Parameters[":currency"].Value = "USD";
            comm.ExecuteNonQuery();

            comm.Parameters[":id"].Value = 4;
            comm.Parameters[":name"].Value = "Coca-cola";
            comm.Parameters[":price"].Value = 0.8;
            comm.Parameters[":currency"].Value = "EUR";
            comm.ExecuteNonQuery();

            Console.WriteLine("Table SKU has been successfully created");

            // create SALES_FACT table:
 	    SQL = "CREATE TABLE SALES_FACT ( " +
		   " TransactionId bigint not null primary key, " +
		   " LocationId integer not null, " +
		   " SKUId smallint not null, " +
		   " TransactionTime time not null, " +
		   " TransactionDate date not null, " +
		   " Comments varchar(128) " +
		" ) ";
 
           comm = conn.CreateCommand();
           comm.CommandText = SQL;
           comm.ExecuteNonQuery();

           comm.CommandText = "INSERT INTO SALES_FACT VALUES (1, 1, 1, APPLIB.CHAR_TO_TIME('kk:mm', '08:10'), APPLIB.CHAR_TO_DATE('yyyy-M-d', '2009-09-23'), NULL)";
           comm.ExecuteNonQuery();

           comm.CommandText = "INSERT INTO SALES_FACT VALUES (2, 1, 3, APPLIB.CHAR_TO_TIME('kk:mm', '08:10'), APPLIB.CHAR_TO_DATE('yyyy-M-d', '2009-09-23'), NULL)";
           comm.ExecuteNonQuery();

           comm.CommandText = "INSERT INTO SALES_FACT VALUES (3, 2, 4, APPLIB.CHAR_TO_TIME('kk:mm', '13:15'), APPLIB.CHAR_TO_DATE('yyyy-M-d', '2009-09-24'), 'Promo')";
           comm.ExecuteNonQuery();

           Console.WriteLine("Table SALES_FACT has been successfully created");
        }
        catch(Exception ex)
        {
            Console.WriteLine("Exception occured: {0}", ex);
        }
        finally
        {
            if (conn != null)
            {
                conn.Close();
            }
        }       
    }
}