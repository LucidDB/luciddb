/*
// $Id$
// LucidDB is a DBMS optimized for business intelligence.
// Copyright (C) 2005-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
//
// This program is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation; either version 2 of the License, or (at your option)
// any later version approved by The Eigenbase Project.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
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


/**
 * LucidDbTestCase overrides {@link net.sf.farrago.test.FarragoTestCase#runSqlLineTest} 
 * to use LucidDbLocalDriver
 *
 * @author Sunny Choi
 * @version $Id$
 */
public class LucidDbTestCase extends FarragoTestCase
{

    public LucidDbTestCase(String testName)
        throws Exception
    {
        super(testName);
    }


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
