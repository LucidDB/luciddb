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
package com.lucidera.luciddb.test.mbean;

import java.lang.management.*;
import javax.management.*;
import junit.framework.*;
import org.junit.*;

import org.luciddb.session.*;
import org.luciddb.jdbc.*;
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
