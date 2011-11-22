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

            // grab non existing values:
            NpgsqlCommand comm = conn.CreateCommand();
            comm.CommandText = "select * from PG2LUCIDDBTEST.LOCATION";     
            NpgsqlDataReader dr = comm.ExecuteReader(CommandBehavior.SchemaOnly);
        
            // get schema:
            DataTable dt = dr.GetSchemaTable();

            for (int i = 0; i < dt.Rows.Count; i++)
            {
                 for (int j = 0; j < dt.Columns.Count; j++)
                 {
                      Console.WriteLine(dt.Columns[j].ColumnName + " = " + dt.Rows[i][j]);
                 }
                 Console.WriteLine("");
            }

            dr.Close();                        

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