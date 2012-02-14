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
package net.sf.farrago.util;

import java.util.*;


/**
 * FarragoTimerAllocation takes care of cancelling a Timer when it is closed.
 * Cancellation is implemented synchronously and without any delay, making it
 * easy to avoid shutdown races.
 *
 * <p>NOTE jvs 13-Aug-2007: The shutdown mechanism requires that cancellation
 * not happen via any other means. If it does, the shutdown could result in an
 * {@link IllegalStateException} or a hang. Use {@link FarragoTimerTask} to
 * avoid the implicit cancellation which can occur when a timer task throws an
 * exception.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoTimerAllocation
    implements FarragoAllocation
{
    //~ Instance fields --------------------------------------------------------

    private Timer timer;
    private final Object shutdownSynch = new Integer(0);

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FarragoTimerAllocation.
     *
     * @param owner the owner for the timer
     * @param timer the timer to be cancelled when this allocation is closed
     */
    public FarragoTimerAllocation(
        FarragoAllocationOwner owner,
        Timer timer)
    {
        this.timer = timer;
        owner.addAllocation(this);
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoAllocation
    public void closeAllocation()
    {
        if (timer == null) {
            return;
        }

        // We want synchronous cancellation as soon as possible.  Timer
        // guarantees that if timer.cancel is called from within a task
        // scheduled by timer, then timer will not execute any more tasks after
        // that one.  So, we schedule a private cancellation task, using delay=0
        // to request immediate execution.  If there is already a task in
        // progress, it will complete first.
        synchronized (shutdownSynch) {
            timer.schedule(
                new CancelTask(),
                0);
            while (timer != null) {
                try {
                    shutdownSynch.wait();
                } catch (InterruptedException ex) {
                    throw new AssertionError();
                }
            }
        }
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Helper class implementing synchronous cancellation.
     */
    private class CancelTask
        extends TimerTask
    {
        // implement Runnable
        public void run()
        {
            timer.cancel();
            synchronized (shutdownSynch) {
                timer = null;
                shutdownSynch.notifyAll();
            }
        }
    }
}

// End FarragoTimerAllocation.java
