/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2007 LucidEra, Inc.
// Copyright (C) 2006-2007 The Eigenbase Project
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
