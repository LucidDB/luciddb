/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2008-2008 LucidEra, Inc.
// Copyright (C) 2008-2008 The Eigenbase Project
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
