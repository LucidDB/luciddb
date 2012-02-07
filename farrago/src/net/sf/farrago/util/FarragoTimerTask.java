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
import java.util.logging.*;


/**
 * FarragoTimerTask refines {@link TimerTask} to guarantee safety based on the
 * way Farrago runs timers (see {@link FarragoTimerAllocation}).
 *
 * <p>TODO jvs 13-Aug-2007: add a facility for subclasses to be able to
 * distinguish fatal exceptions from recoverable ones, so that timers can be
 * allowed to keep running after an exception.
 *
 * @author John Sichi
 * @version $Id$
 */
public abstract class FarragoTimerTask
    extends TimerTask
{
    //~ Instance fields --------------------------------------------------------

    private final Logger tracer;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FarragoTimerTask.
     *
     * @param tracer logger on which to trace exceptions
     */
    protected FarragoTimerTask(Logger tracer)
    {
        this.tracer = tracer;
    }

    //~ Methods ----------------------------------------------------------------

    // implement Runnable
    public final void run()
    {
        try {
            runTimer();
        } catch (Throwable ex) {
            // NOTE jvs 13-Aug-2007: Do not propagate the exception, since that
            // would kill other tasks running on the same timer, and worse,
            // cause shutdown errors or hangs when we attempt to cancel the
            // timer.  (See http://issues.eigenbase.org/browse/FRG-99.)
            // Instead, just log the exception's stack and cancel the task (not
            // the timer).  In the future, call subclass to decide on
            // recoverability; if recoverable, don't cancel.
            if (tracer != null) {
                tracer.log(Level.SEVERE, "FarragoTimerTask failed", ex);
            }
            cancel();
        }
    }

    /**
     * Runs the timer action as specified by subclass.
     */
    protected abstract void runTimer();
}

// End FarragoTimerTask.java
