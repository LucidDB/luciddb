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
	    Console.WriteLine("ID	NAME	MANAGER	SQUARE	ISFASTFOOD	ISCLOSED	DATEADDED	DATEOPENED");
	    Console.WriteLine("");	

	    string SQL = "SELECT ID, NAME, MANAGER, SQUARE, ISFASTFOOD, ISCLOSED, DATEADDED, DATEOPENED FROM PG2LUCIDDBTEST.LOCATION ORDER BY ID";
            NpgsqlCommand comm = conn.CreateCommand();
            comm.CommandText = SQL;

            // execute reader:
            NpgsqlDataReader dr = comm.ExecuteReader();
            while (dr.Read())
            {
                Console.WriteLine("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}", dr["ID"], dr["NAME"], dr["MANAGER"], dr["SQUARE"], dr["ISFASTFOOD"], dr["ISCLOSED"], dr["DATEADDED"], dr["DATEOPENED"]);  
            }
            dr.Close();

            // fill dataset:
	    // get list of SKUs:
	    Console.WriteLine("");
	    Console.WriteLine("SKU table");

            comm.CommandText = "SELECT ID, NAME, PRICE, CURRENCY FROM PG2LUCIDDBTEST.SKU ORDER BY ID";
            DataSet ds = new DataSet();
            NpgsqlDataAdapter da = new NpgsqlDataAdapter(comm);
            da.Fill(ds);

            // write header:
            string tmpHeader = "";
            for (int i = 0; i < ds.Tables[0].Columns.Count; i++)
            {
                tmpHeader += ds.Tables[0].Columns[i].ColumnName + "\t";
            }
            Console.WriteLine(tmpHeader);
            Console.WriteLine("");

            // write data:
            for (int i = 0; i < ds.Tables[0].Rows.Count; i++)
            {
                string tmpLine = "";
                for (int j = 0; j < ds.Tables[0].Columns.Count; j++)
                {
                    tmpLine += String.Format("{0}\t", ds.Tables[0].Rows[i][j]);
                }
                Console.WriteLine(tmpLine);
            }                                                   

            // get list of sales_fact:
	    Console.WriteLine("");
	    Console.WriteLine("SALES_FACT table");

            comm.CommandText = "SET SCHEMA 'PG2LUCIDDBTEST'";
            comm.ExecuteNonQuery();
  
            comm.CommandText = "SELECT T1.TRANSACTIONID, T2.NAME, T3.NAME, T1.TRANSACTIONTIME, T1.TRANSACTIONDATE FROM SALES_FACT T1 INNER JOIN LOCATION T2 ON T2.ID = T1.LOCATIONID INNER JOIN SKU T3 ON T3.ID = T1.SKUID WHERE T1.TRANSACTIONDATE = APPLIB.CHAR_TO_DATE('yyyy-M-d', '2009-09-23') ORDER BY T1.TRANSACTIONID";
            
            ds = new DataSet();
            da = new NpgsqlDataAdapter(comm);
            da.Fill(ds);

            // write header:
            tmpHeader = "";
            for (int i = 0; i < ds.Tables[0].Columns.Count; i++)
            {
                tmpHeader += ds.Tables[0].Columns[i].ColumnName + "\t";
            }
            Console.WriteLine(tmpHeader);
            Console.WriteLine("");

            // write data:
            for (int i = 0; i < ds.Tables[0].Rows.Count; i++)
            {
                string tmpLine = "";
                for (int j = 0; j < ds.Tables[0].Columns.Count; j++)
                {
                    tmpLine += String.Format("{0}\t", ds.Tables[0].Rows[i][j]);
                }
                Console.WriteLine(tmpLine);
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