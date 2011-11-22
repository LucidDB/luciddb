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
            // get server version:
            NpgsqlCommand comm = conn.CreateCommand();
            comm.CommandText = "drop schema PG2LUCIDDBTEST cascade";
            comm.ExecuteNonQuery();
            Console.WriteLine("PG2LUCIDDBTEST schema has been successfully deleted");
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