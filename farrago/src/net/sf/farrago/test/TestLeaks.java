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
package net.sf.farrago.test;

import net.sf.farrago.fennel.*;


/**
 * TestLeaks helps locate Java memory leaks.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class TestLeaks
    extends FarragoTestCase
{
    //~ Constructors -----------------------------------------------------------

    private TestLeaks()
        throws Exception
    {
        super("TestLeaks");
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Main entry point.
     *
     * @param args ignored
     *
     * @throws Exception .
     */
    public static void main(String [] args)
        throws Exception
    {
        boolean jmp = ((args.length == 1) && args[0].equals("jmp"));
        staticSetUp();
        TestLeaks p = new TestLeaks();
        p.setUp();
        p.go(jmp);
        p.tearDown();
        staticTearDown();
    }

    private void go(boolean jmp)
        throws Exception
    {
        stmt.execute("alter system set \"codeCacheMaxBytes\" = 0");
        String sql = "select * from sales.emps where deptno = 20";
        int nFennelHandles = 0;
        for (int i = 0; i < 50000; ++i) {
            resultSet = stmt.executeQuery(sql);
            if (repos.isFennelEnabled()) {
                assertEquals(
                    2,
                    getResultSetCount());
            } else {
                assertEquals(
                    0,
                    getResultSetCount());
            }
            resultSet.close();
            resultSet = null;

            Runtime rt = Runtime.getRuntime();
            rt.gc();
            System.err.println(
                "used = "
                + (rt.totalMemory() - rt.freeMemory()));
            if (i == 1) {
                nFennelHandles = FennelStorage.getHandleCount();
                if (jmp) {
                    System.out.println("PAUSE");
                    try {
                        synchronized (this) {
                            wait(30000);
                        }
                    } catch (InterruptedException ex) {
                    }
                    System.out.println("RESUME");
                }
            } else if (i > 1) {
                int nFennelHandlesNow = FennelStorage.getHandleCount();
                assert (nFennelHandles == nFennelHandlesNow);
                if (jmp) {
                    stmt.close();
                    stmt = null;
                    System.out.println("SUSPEND");
                    try {
                        synchronized (this) {
                            wait(200000000);
                        }
                    } catch (InterruptedException ex) {
                    }
                }
            }
        }
    }
}

// End TestLeaks.java
