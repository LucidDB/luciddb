/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2007 The Eigenbase Project
// Copyright (C) 2007 SQLstream, Inc.
// Copyright (C) 2007 Dynamo BI Corporation
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
