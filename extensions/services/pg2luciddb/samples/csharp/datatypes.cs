using System;
using System.Data;
using Npgsql;

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

	    // get list of locations:
	    Console.WriteLine("LOCATIONS table");
	    Console.WriteLine("");	

	    string SQL = "SELECT ID, NAME, MANAGER, SQUARE, ISFASTFOOD, ISCLOSED, DATEADDED, DATEOPENED FROM PG2LUCIDDBTEST.LOCATION ORDER BY ID";            
            NpgsqlCommand comm = conn.CreateCommand();
            comm.CommandText = SQL;
            DataSet ds = new DataSet();
            NpgsqlDataAdapter da = new NpgsqlDataAdapter(comm);
            da.Fill(ds);

            for (int i = 0; i < ds.Tables[0].Columns.Count; i++)
            {
                Console.WriteLine(ds.Tables[0].Columns[i].ColumnName + " = " + ds.Tables[0].Columns[i].DataType + ", " + (ds.Tables[0].Rows[0][i] != null ? ds.Tables[0].Rows[0][i].GetType().ToString() : "NULL") + " (" + (ds.Tables[0].Rows[0][i] != null ? ds.Tables[0].Rows[0][i].ToString() : "NULL") + ")");
            }

	    Console.WriteLine("");	
	    Console.WriteLine("SKU table");
	    Console.WriteLine("");	

	    SQL = "SELECT * FROM PG2LUCIDDBTEST.SKU";            
            comm = conn.CreateCommand();
            comm.CommandText = SQL;
            ds = new DataSet();
            da = new NpgsqlDataAdapter(comm);
            da.Fill(ds);

            for (int i = 0; i < ds.Tables[0].Columns.Count; i++)
            {
		Console.WriteLine(ds.Tables[0].Columns[i].ColumnName + " = " + ds.Tables[0].Columns[i].DataType + ", " + (ds.Tables[0].Rows[0][i] != null ? ds.Tables[0].Rows[0][i].GetType().ToString() : "NULL") + " (" + (ds.Tables[0].Rows[0][i] != null ? ds.Tables[0].Rows[0][i].ToString() : "NULL") + ")");                
            }

	    Console.WriteLine("");	
	    Console.WriteLine("SALES_FACT table");
	    Console.WriteLine("");	

	    SQL = "SELECT * FROM PG2LUCIDDBTEST.SALES_FACT";            
            comm = conn.CreateCommand();
            comm.CommandText = SQL;
            ds = new DataSet();
            da = new NpgsqlDataAdapter(comm);
            da.Fill(ds);

            for (int i = 0; i < ds.Tables[0].Columns.Count; i++)
            {
		Console.WriteLine(ds.Tables[0].Columns[i].ColumnName + " = " + ds.Tables[0].Columns[i].DataType + ", " + (ds.Tables[0].Rows[0][i] != null ? ds.Tables[0].Rows[0][i].GetType().ToString() : "NULL") + " (" + (ds.Tables[0].Rows[0][i] != null ? ds.Tables[0].Rows[0][i].ToString() : "NULL") + ")");                
            }
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