/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2007-2007 LucidEra, Inc.
// Copyright (C) 2007-2007 The Eigenbase Project
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
package com.lucidera.luciddb.test.mbean;

import java.lang.management.*;
import javax.management.*;
import junit.framework.*;
import org.junit.*;

import com.lucidera.farrago.*;
import com.lucidera.jdbc.*;
import com.lucidera.luciddb.mbean.*;
import com.lucidera.luciddb.mbean.server.*;
import com.lucidera.luciddb.mbean.sysviews.*;

/**
 * Getting attributes and calling operations on LucidDb MBeans
 *
 * @author Sunny Choi
 * @version $Id$
 */
public class LucidDbMBeansTest extends TestCase
{

    MBeanServer server = null;
    static LucidDbServer lserver = null;

    public LucidDbMBeansTest(String method)
    {
        super(method);
        server = ManagementFactory.getPlatformMBeanServer();
    }

    @BeforeClass
    public static void startLucidDb()
        throws Exception
    {
        lserver = new LucidDbServer();
        lserver.start(new LucidDbLocalDriver());
    }

    @AfterClass
    public static void shutdownLucidDb() {
        lserver.stopHard();
        lserver = null;
    }

    public void testPingServer()
        throws Exception
    {
        ObjectName name =
            new ObjectName("com.lucidera.luciddb.mbean:name=PingServer");

        // register PingServer MBean
        server.registerMBean(new PingServer(), name);

        String status = (String) server.getAttribute(name,"CurrentStatus");
        assertTrue(status.equals(PingServer.STATUS_ALIVE));

        String info = (String) server.getAttribute(name, "Info");
        assertTrue(info == null);
    }

    public void testForeignSources()
        throws Exception
    {
        ObjectName name =
            new ObjectName("com.lucidera.luciddb.mbean:name=ForeignSources");

        // register ForeignSources MBean
        server.registerMBean(new ForeignSources(), name);

        server.invoke(name, "printForeignServers", null, null);
        server.invoke(name, "printForeignServerOptions", null, null);
        server.invoke(name, "printForeignWrappers", null, null);
        server.invoke(name, "printForeignWrapperOptions", null, null);

        server.getAttribute(name, "ForeignServers");
        server.getAttribute(name, "ForeignServerOptions");
        server.getAttribute(name, "ForeignWrappers");
        server.getAttribute(name, "ForeignWrapperOptions");
    }

    public void testObjectsInUse()
        throws Exception
    {
        ObjectName name =
            new ObjectName("com.lucidera.luciddb.mbean:name=ObjectsInUse");

        // register ForeignSources MBean
        server.registerMBean(new ObjectsInUse(), name);

        server.invoke(name, "printObjectsInUse", null, null);
        server.getAttribute(name, "ObjectsInUse");
    }

    public void testPerfCounters()
        throws Exception
    {
        ObjectName name =
            new ObjectName("com.lucidera.luciddb.mbean:name=PerfCounters");

        // register ForeignSources MBean
        server.registerMBean(new PerfCounters(), name);

        server.invoke(name, "printPerfCounters", null, null);
        server.getAttribute(name, "PerfCounters");
    }

    public void testSessions()
        throws Exception
    {
        ObjectName name =
            new ObjectName("com.lucidera.luciddb.mbean:name=Sessions");

        // register ForeignSources MBean
        server.registerMBean(new Sessions(), name);

        server.invoke(name, "printSessions", null, null);
        server.getAttribute(name, "Sessions");
    }

    public void testSqlStatements()
        throws Exception
    {
        ObjectName name =
            new ObjectName("com.lucidera.luciddb.mbean:name=SqlStatements");

        // register ForeignSources MBean
        server.registerMBean(new SqlStatements(), name);

        server.invoke(name, "printStatements", null, null);
//         String[] param = new String[] {"1"};
//         server.invoke(name, "getDetailedSqlInfo", param, null);
//         server.invoke(name, "getSqlStatements", param, null);

        server.getAttribute(name, "Statements");
    }

    public void testSystemParameters()
        throws Exception
    {
        ObjectName name =
            new ObjectName("com.lucidera.luciddb.mbean:name=SystemParameters");

        // register ForeignSources MBean
        server.registerMBean(new SystemParameters(), name);

        server.invoke(name, "printSystemParameters", null, null);
        server.getAttribute(name, "SystemParameters");
    }

    public void testStorageManagement()
        throws Exception
    {
        ObjectName name =
            new ObjectName("com.lucidera.luciddb.mbean:name=StorageManagement");

        server.registerMBean(new StorageManagement(), name);

        Object result;

        // for a very large threshold, growth should be expected
        result = server.invoke(
            name,
            "checkDatabaseGrowth",
            new Object [] { 1000000000L },
            new String [] { "long" } );

        assertEquals(StorageManagement.FILE_GROW, result);
        
        result = server.invoke(
            name,
            "checkTempGrowth",
            new Object [] { 1000000000L },
            new String [] { "long" } );

        assertEquals(StorageManagement.FILE_GROW, result);
        
        // for a very small threshold, growth should NOT be expected
        result = server.invoke(
            name,
            "checkDatabaseGrowth",
            new Object [] { 100L },
            new String [] { "long" } );

        assertEquals(StorageManagement.FILE_KEEP, result);
        
        // for a very small threshold, growth should NOT be expected
        result = server.invoke(
            name,
            "checkTempGrowth",
            new Object [] { 100L },
            new String [] { "long" } );

        assertEquals(StorageManagement.FILE_KEEP, result);
        
        // for a negative threshold, growth should NOT be expected
        result = server.invoke(
            name,
            "checkDatabaseGrowth",
            new Object [] { -100L },
            new String [] { "long" } );

        assertEquals(StorageManagement.FILE_KEEP, result);
    }
}

// End LucidDbMBeansTest.java
