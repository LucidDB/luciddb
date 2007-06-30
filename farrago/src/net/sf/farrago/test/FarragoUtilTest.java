/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2007-2007 The Eigenbase Project
// Copyright (C) 2007-2007 Disruptive Tech
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

import junit.framework.*;

import java.util.*;

import net.sf.farrago.util.*;

import org.eigenbase.util.*;

/**
 * Tests components in package {@link net.sf.farrago.util}.
 *
 * @author John Sichi
 * @version $Id$
 */
public class FarragoUtilTest extends TestCase
{
    /**
     * Creates a new FarragoUtilTest object.
     */
    public FarragoUtilTest(String testName)
    {
        super(testName);
    }

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

    private class TimerTestTask extends TimerTask
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
