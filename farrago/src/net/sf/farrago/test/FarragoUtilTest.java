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

import junit.framework.*;

import net.sf.farrago.util.*;

import org.eigenbase.util.*;


/**
 * Tests components in package {@link net.sf.farrago.util}.
 *
 * @author John Sichi
 * @version $Id$
 */
public class FarragoUtilTest
    extends TestCase
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FarragoUtilTest object.
     */
    public FarragoUtilTest(String testName)
    {
        super(testName);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Tests {@link FarragoTimerAllocation}.
     */
    public void testTimerAllocation()
    {
        for (int i = 0; i < 1000; ++i) {
            FarragoCompoundAllocation owner = new FarragoCompoundAllocation();
            try {
                Timer timer = new Timer("FarragoUtilTest");
                new FarragoTimerAllocation(owner, timer);
                timer.schedule(
                    new TimerTestTask(),
                    10,
                    10);
                Thread.sleep(30);
            } catch (InterruptedException ex) {
                throw Util.newInternal(ex);
            } finally {
                owner.closeAllocation();
            }
        }
    }

    //~ Inner Classes ----------------------------------------------------------

    private class TimerTestTask
        extends TimerTask
    {
        public void run()
        {
            try {
                Thread.sleep(5);
            } catch (InterruptedException ex) {
                throw Util.newInternal(ex);
            }
        }
    }
}

// End FarragoUtilTest.java
