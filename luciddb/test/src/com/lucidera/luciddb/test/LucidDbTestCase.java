package com.lucidera.luciddb.test;

import com.lucidera.jdbc.*;

import java.io.*;
import java.util.*;

import junit.framework.Test;
import junit.framework.TestSuite;

import net.sf.farrago.catalog.*;
import net.sf.farrago.jdbc.engine.*;
import net.sf.farrago.test.*;

import sqlline.SqlLine;


public class LucidDbTestCase extends FarragoTestCase
{

    public LucidDbTestCase(String testName)
        throws Exception
    {
        super(testName);
    }

    // override runSqlLineTest to use LucidDbLocalDriver
    protected void runSqlLineTest(String sqlFile)
        throws Exception
    {
        FarragoJdbcEngineDriver driver = newJdbcEngineDriver();
        assert (sqlFile.endsWith(".sql"));
        File sqlFileSansExt =
            new File(sqlFile.substring(0, sqlFile.length() - 4));
        String [] args =
            new String [] {
                "-u", driver.getUrlPrefix(), "-d",
                "com.lucidera.jdbc.LucidDbLocalDriver", "-n",
                FarragoCatalogInit.SA_USER_NAME,
                "--force=true", "--silent=true",
                "--showWarnings=false", "--maxWidth=1024"
            };
        PrintStream savedOut = System.out;
        PrintStream savedErr = System.err;

        // read from the specified file
        FileInputStream inputStream = new FileInputStream(sqlFile.toString());

        // to make sure the connection is closed properly, append the
        // !quit command
        String quitCommand = "\n!quit\n";
        ByteArrayInputStream quitStream =
            new ByteArrayInputStream(quitCommand.getBytes());

        SequenceInputStream sequenceStream =
            new SequenceInputStream(inputStream, quitStream);
        try {
            OutputStream outputStream =
                openTestLogOutputStream(sqlFileSansExt);
            PrintStream printStream = new PrintStream(outputStream);
            System.setOut(printStream);
            System.setErr(printStream);

            // tell SqlLine not to exit (this boolean is active-low)
            System.setProperty("sqlline.system.exit", "true");
            SqlLine.mainWithInputRedirection(args, sequenceStream);
            printStream.flush();
            if (shouldDiff()) {
                diffTestLog();
            }
        } finally {
            System.setOut(savedOut);
            System.setErr(savedErr);
            inputStream.close();
        }
    }

}
