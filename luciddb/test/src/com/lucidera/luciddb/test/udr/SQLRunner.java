package com.lucidera.luciddb.test.udr;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.PreparedStatement;

public class SQLRunner
    extends Thread
{
    private String errorMsg;

    private PreparedStatement ps;

    public SQLRunner(PreparedStatement ps)
    {
        this.ps = ps;
    }

    public void run()
    {

        try {

            ps.execute();

        } catch (Exception ex) {

            StringWriter writer = new StringWriter();
            ex.printStackTrace(new PrintWriter(writer, true));
            errorMsg = writer.toString();
        }

    }

    public String getErrorMsg()
    {
        return errorMsg;
    }

    private void setErrorMsg(String errorMsg)
    {
        this.errorMsg = errorMsg;
    }
    
    
}
