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

import java.sql.*;
import java.io.*;
import java.util.*;

/**
 * LucidDbMonitor is a simple integration of LucidDB into the
 * <a href="http://moodss.sf.net">moodss</a> performance monitor.  It
 * runs as a client-side JDBC program, polling performance counter values
 * from a LucidDB server and writing them to the filesystem for display
 * by moodss.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class LucidDbMonitor
{
    private Connection conn;
    private PreparedStatement ps;
    private Thread thread;
    private boolean quit;
    
    public static void main(String [] args)
        throws Exception
    {
        LucidDbMonitor monitor = new LucidDbMonitor();
        System.out.println("Connecting...");
        monitor.connect(args);
        try {
            // Perform one poll just to get file created so that moodss
            // display will have all counter names.
            monitor.poll();
            System.out.println("Spawning moodss...");
            Process process = Runtime.getRuntime().exec(
                "moodss -f LucidDbDashboard.moo");
            System.out.println("Spawning polling thread...");
            monitor.startPollingThread();
            System.out.println(
                "Now running; close moodss window to shut down.");
            int rc = process.waitFor();
            if (rc != 0) {
                InputStream errStream = process.getErrorStream();
                InputStreamReader errReader = new InputStreamReader(errStream);
                LineNumberReader lineReader = new LineNumberReader(errReader);
                for (;;) {
                    String err = lineReader.readLine();
                    if (err == null) {
                        break;
                    }
                    System.err.println(err);
                }
            }
            System.out.println("Shutting down...");
            monitor.stopPollingThread();
        } finally {
            monitor.disconnect();
        }
    }

    private class PollingThread extends Thread
    {
        public void run()
        {
            try {
                for (;;) {
                    if (quit) {
                        break;
                    }
                    poll();
                    sleep(1000);
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    private void startPollingThread()
        throws Exception
    {
        thread = new PollingThread();
        thread.start();
    }

    private void stopPollingThread()
        throws Exception
    {
        quit = true;
        thread.join();
        thread = null;
    }

    private void connect(String [] args)
        throws Exception
    {
        String url = "jdbc:luciddb:rmi://localhost";
        String user = "sa";
        String password = "";
        if (args.length > 0) {
            url = args[0];
        }
        if (args.length > 1) {
            user = args[1];
        }
        if (args.length > 2) {
            password = args[2];
        }
        Class.forName("com.lucidera.jdbc.LucidDbRmiDriver");
        conn = DriverManager.getConnection(url, user, password);

        ps = conn.prepareStatement(
            "select counter_name, counter_value from "
            + "sys_root.dba_performance_counters");
    }

    private void disconnect()
        throws Exception
    {
        conn.close();
        conn = null;
    }

    private void poll()
        throws Exception
    {
        // Buffer everything up into a string in order to get as
        // close to atomic file write as possible.
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            String name = rs.getString(1);
            String value = rs.getString(2);
            pw.print(name);
            pw.print(' ');
            pw.println(value);
        }
        rs.close();
        pw.flush();
        String filename = "LucidDbPerfCounters.txt";
        FileWriter fw = new FileWriter(filename);
        fw.write(sw.toString());
        fw.close();
    }
}

// End LucidDbMonitor.java
