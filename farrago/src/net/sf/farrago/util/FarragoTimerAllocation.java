/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

package net.sf.farrago.util;

import java.util.*;

/**
 * FarragoTimerAllocation takes care of cancelling a Timer when it is closed.
 * Cancellation is implemented synchronously and without any delay, making it
 * easy to avoid shutdown races.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoTimerAllocation implements FarragoAllocation
{
    private Timer timer;

    private final Object shutdownSynch = new Integer(0);

    /**
     * Create a new FarragoTimerAllocation.
     *
     * @param owner the owner for the timer
     *
     * @param timer the timer to be cancelled when this allocation is closed
     */
    public FarragoTimerAllocation(
        FarragoAllocationOwner owner,
        Timer timer)
    {
        this.timer = timer;
        owner.addAllocation(this);
    }

    // implement FarragoAllocation
    public void closeAllocation()
    {
        if (timer == null) {
            return;
        }

        // We want synchronous cancellation as soon as possible.  Timer
        // guarantees that if timer.cancel is called from within a task
        // scheduled by timer, then timer will not execute any more tasks after
        // that one.  So, we schedule a private cancellation task, using
        // delay=0 to request immediate execution.  If there is already a task
        // in progress, it will complete first.
        synchronized(shutdownSynch) {
            timer.schedule(new CancelTask(),0);
            while (timer != null) {
                try {
                    shutdownSynch.wait();
                } catch (InterruptedException ex) {
                    assert(false);
                }
            }
        }
    }

    /**
     * Helper class implementing synchronous cancellation.
     */
    private class CancelTask extends TimerTask 
    {
        // implement Runnable
        public void run()
        {
            timer.cancel();
            synchronized(shutdownSynch) {
                timer = null;
                shutdownSynch.notifyAll();
            }
        }
    }
}

// End FarragoTimerAllocation.java
