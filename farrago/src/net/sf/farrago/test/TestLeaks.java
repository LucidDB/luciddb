/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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
