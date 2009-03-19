/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2007-2007 The Eigenbase Project
// Copyright (C) 2007-2007 SQLstream, Inc.
// Copyright (C) 2007-2007 LucidEra, Inc.
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

import java.util.*;
import java.util.logging.*;

import junit.framework.*;

import net.sf.farrago.util.*;

import org.eigenbase.util.*;


/**
 * FarragoTimerTest tests {@link FarragoTimerAllocation} and {@link
 * FarragoTimerTask}.
 *
 * @author John Sichi
 * @version $Id$
 */
public class FarragoTimerTest
    extends TestCase
{
    //~ Instance fields --------------------------------------------------------

    int nTicks;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FarragoTimerTest object.
     */
    public FarragoTimerTest(String testName)
        throws Exception
    {
        super(testName);
    }

    //~ Methods ----------------------------------------------------------------

    public void testGoodTask()
    {
        executeTask(new GoodTask());

        // Don't know exactly how many ticks will fit in; verify
        // that it's at least two.
        assertTrue(nTicks > 1);
    }

    public void testBadTask()
    {
        executeTask(new BadTask());

        // Verify that timer stopped ticking after burp.
        assertEquals(3, nTicks);
    }

    private void executeTask(FarragoTimerTask task)
    {
        FarragoCompoundAllocation owner = new FarragoCompoundAllocation();
        try {
            nTicks = 0;
            Timer timer = new Timer("FarragoTimerTest");
            new FarragoTimerAllocation(owner, timer);
            timer.schedule(
                task,
                50,
                50);
            Thread.currentThread().sleep(1000);
        } catch (InterruptedException ex) {
            throw Util.newInternal(ex);
        } finally {
            owner.closeAllocation();
        }
    }

    //~ Inner Classes ----------------------------------------------------------

    private class GoodTask
        extends FarragoTimerTask
    {
        GoodTask()
        {
            super(null);
        }

        // implement FarragoTimerTask
        protected void runTimer()
        {
            ++nTicks;
        }
    }

    private class BadTask
        extends FarragoTimerTask
    {
        BadTask()
        {
            super(null);
        }

        // implement FarragoTimerTask
        public void runTimer()
        {
            ++nTicks;
            if (nTicks == 3) {
                throw new RuntimeException("burp");
            }
        }
    }
}

// End FarragoTimerTest.java
