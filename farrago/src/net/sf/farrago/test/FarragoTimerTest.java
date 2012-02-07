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
