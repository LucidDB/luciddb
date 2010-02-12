/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2003-2009 John V. Sichi
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
        // We want synchronous cancellation as soon as possible.  Timer
        // guarantees that if timer.cancel is called from within a task
        // scheduled by timer, then timer will not execute any more tasks after
        // that one.  So, we schedule a private cancellation task, using delay=0
        // to request immediate execution.  If there is already a task in
        // progress, it will complete first.
        synchronized (shutdownSynch) {
            if (timer == null) {
                return;
            }
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
            synchronized (shutdownSynch) {
                timer.cancel();
                timer = null;
                shutdownSynch.notifyAll();
            }
        }
    }
}

// End FarragoTimerAllocation.java
