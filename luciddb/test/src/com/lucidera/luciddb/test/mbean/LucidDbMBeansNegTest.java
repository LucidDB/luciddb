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
import net.sf.farrago.jdbc.*;

/**
 * Tests to verify that LucidDB MBeans do not startup LucidDB engine
 *
 * @author Sunny Choi
 * @version $Id$
 */
public class LucidDbMBeansNegTest extends TestCase
{

    MBeanServer server = null;

    public LucidDbMBeansNegTest(String method)
    {
        super(method);
        server = ManagementFactory.getPlatformMBeanServer();
    }

    public void testPingServer()
        throws Exception
    {
        ObjectName name =
            new ObjectName("com.lucidera.luciddb.mbean:name=PingServer2");

        // register PingServer MBean
        server.registerMBean(new PingServer(), name);
        String status = (String) server.getAttribute(name,"CurrentStatus");
        assertTrue(status.equals(PingServer.STATUS_DEAD));
    }

    public void testForeignSources()
        throws Exception
    {
        ObjectName name =
            new ObjectName("com.lucidera.luciddb.mbean:name=ForeignSources2");

        // register ForeignSources MBean
        server.registerMBean(new ForeignSources(), name);
        try {
            server.invoke(name, "printForeignServers", null, null);
        } catch (Exception e) {
            if (validateException(e)) {
                return;
            }
        }
        fail();
    }

    public void testObjectsInUse()
        throws Exception
    {
        ObjectName name =
            new ObjectName("com.lucidera.luciddb.mbean:name=ObjectsInUse2");

        // register ForeignSources MBean
        server.registerMBean(new ObjectsInUse(), name);
        try {
            server.invoke(name, "printObjectsInUse", null, null);
        } catch (Exception e) {
            if (validateException(e)) {
                return;
            }
        }
        fail();
    }

    public void testPerfCounters()
        throws Exception
    {
        ObjectName name =
            new ObjectName("com.lucidera.luciddb.mbean:name=PerfCounters2");

        // register ForeignSources MBean
        server.registerMBean(new PerfCounters(), name);
        try {
            server.invoke(name, "printPerfCounters", null, null);
        } catch (Exception e) {
            if (validateException(e)) {
                return;
            }
        }
        fail();
    }

    public void testSessions()
        throws Exception
    {
        ObjectName name =
            new ObjectName("com.lucidera.luciddb.mbean:name=Sessions2");

        // register ForeignSources MBean
        server.registerMBean(new Sessions(), name);
        try {
            server.invoke(name, "printSessions", null, null);
        } catch (Exception e) {
            if (validateException(e)) {
                return;
            }
        }
        fail();
    }

    public void testSqlStatements()
        throws Exception
    {
        ObjectName name =
            new ObjectName("com.lucidera.luciddb.mbean:name=SqlStatements2");

        // register ForeignSources MBean
        server.registerMBean(new SqlStatements(), name);
        try {
            server.invoke(name, "printStatements", null, null);
        } catch (Exception e) {
            if (validateException(e)) {
                return;
            }
        }
        fail();
    }

    public void testSystemParameters()
        throws Exception
    {
        ObjectName name =
            new ObjectName("com.lucidera.luciddb.mbean:name=SystemParameters2");

        // register ForeignSources MBean
        server.registerMBean(new SystemParameters(), name);
        try {
            server.invoke(name, "printSystemParameters", null, null);
        } catch (Exception e) {
            if (validateException(e)) {
                return;
            }
        }
        fail();
    }

    public void testStorageManagement()
        throws Exception
    {
        ObjectName name =
            new ObjectName("com.lucidera.luciddb.mbean:name=StorageManagement2");

        server.registerMBean(new StorageManagement(), name);

        Object result;

        // for a very large threshold, growth should be expected
        try {
            result = server.invoke(
                name,
                "checkDatabaseGrowth",
                new Object [] { 1000000000L },
                new String [] { "long" } );
        } catch (Exception e) {
            if (validateException(e)) {
                return;
            }
        }
        fail();
    }

    private boolean validateException(Exception e)
    {
        FarragoJdbcUtil.FarragoSqlException ee =
            (FarragoJdbcUtil.FarragoSqlException)e.getCause();
        return ee.getMessage().equals("Existing database server required");
    }
}

// End LucidDbMBeansNegTest.java
