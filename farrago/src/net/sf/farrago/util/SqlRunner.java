/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/
package net.sf.farrago.util;

import java.io.*;

import java.sql.*;

import java.util.logging.*;

import sqlline.SqlLine;


/**
 * Wrapper around the sqlline code to enable running of a SQL script from within
 * Java code.
 *
 * @author chard
 * @version $Id$
 * @since Jun 30, 2006
 */
public class SqlRunner
{
    //~ Static fields/initializers ---------------------------------------------

    private static SqlRunner singleton = null;

    protected static Logger logger =
        Logger.getLogger(SqlRunner.class.getName());

    private static InputStream quitStream = null;

    static {
        quitStream = new ByteArrayInputStream("\n!quit\n".getBytes());
    }

    //~ Constructors -----------------------------------------------------------

    /**
     * Private constructor for singleton use. Call the {@link #instance()}
     * method to get an instance of the class to use.
     */
    private SqlRunner()
    {
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Run the specified SQL script against the server at the specified URL
     * using the supplied credentials. All outputs to stdout and stderr are
     * unchanged.
     *
     * @param pathName String containing a path name to a SQL script
     * @param url String specifying the URL of a server
     * @param userName String specifying the user to log into the server as
     * @param password String containing the password for the specified user
     *
     * @throws SQLException
     */
    public void runScript(
        String pathName,
        String url,
        String userName,
        String password)
        throws SQLException
    {
        runScript(pathName, url, userName, password, null, null);
    }

    /**
     * Run the specified SQL script against the server at the specified URL
     * using the supplied credentials. All outputs to stdout and stderr are
     * redirected to the soecified PrintStream.
     *
     * @param pathName String containing a path name to a SQL script
     * @param url String specifying the URL of a server
     * @param userName String specifying the user to log into the server as
     * @param password String containing the password for the specified user
     * @param out PrintStream to redirect stdout and stderr to
     *
     * @throws SQLException
     */
    public void runScript(
        String pathName,
        String url,
        String userName,
        String password,
        PrintStream out)
        throws SQLException
    {
        runScript(pathName, url, userName, password, out, out);
    }

    /**
     * Run the specified SQL script against the server at the specified URL. The
     * caller can redirect stdout and/or stderr to alternate streams, if
     * desired.
     *
     * @param pathName String containing a path name to a SQL script
     * @param url String specifying the URL of a server
     * @param userName String specifying the user to log into the server as
     * @param password String containing the password for the specified user
     * @param out PrintStream to redirect stdout to while executing the script,
     * or null to leave stdout unchanged
     * @param err PrintStream to redirect stderr to while executing the script,
     * or null to leave stderr unchanged
     *
     * @throws SQLException All errors in execution are converted into {@link
     * SQLException} and thrown. Null values for either <code>pathName</code> or
     * <code>url</code> will assert.
     */
    public void runScript(
        String pathName,
        String url,
        String userName,
        String password,
        PrintStream out,
        PrintStream err)
        throws SQLException
    {
        // first, some really basic assumptions
        assert (pathName != null) : "SQL script path cannot be null";
        assert (url != null) : "Server URL cannot be null";
        assert (userName != null) : "User name must be specified";

        PrintStream savedOut = System.out;
        PrintStream savedErr = System.err;

        // now try out the input file
        InputStream in = null;
        try {
            in = new FileInputStream(pathName);

            // make sure we have a useful URL
            Driver driver = DriverManager.getDriver(url);
            if (!url.matches(".*;sessionName=.*")) {
                url += ";sessionName=SqlRunner";
            }
            String [] args =
                new String[] {
                    "-u", url,
                    "-d", driver.getClass().getName(),
                    "-n", userName,
                    "-p", password,
                    "--force=true",
                    "--silent=true",
                    "--showWarnings=false",
                    "--maxWidth=1024"
                };
            SequenceInputStream sequenceStream =
                new SequenceInputStream(in, quitStream);
            System.setProperty("sqlline.system.exit", "true");
            if (out != null) {
                System.setOut(out);
            }
            if (err != null) {
                System.setErr(err);
            }
            SqlLine.mainWithInputRedirection(args, sequenceStream);
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Error processing SQL script", e);
            SQLException se = new SQLException(e.getMessage());
            logger.throwing(
                this.getClass().getName(),
                "runScript()",
                se);
            throw se;
        } finally {
            if (out != null) {
                out.flush();
                System.setOut(savedOut);
            }
            if (err != null) {
                err.flush();
                System.setErr(savedErr);
            }
            try {
                if (in != null) {
                    in.close();
                }
                quitStream.reset();
            } catch (IOException e) {
                logger.warning("Problem closing input: " + e.getMessage());
            }
        }
    }

    /**
     * Get a singleton instance of the SqlRunner class to run one or more
     * scripts with.
     *
     * @return Always returns the same instance of SqlRunner
     */
    public static synchronized SqlRunner instance()
    {
        if (singleton == null) {
            singleton = new SqlRunner();
        }
        return singleton;
    }
}

// End SqlRunner.java
