/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
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
package org.eigenbase.trace;

import java.text.*;

import java.util.logging.*;


/**
 * EigenbaseTimingTracer provides a mechanism for tracing the timing of a call
 * sequence at nanosecond resolution.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class EigenbaseTimingTracer
{

    //~ Static fields/initializers ---------------------------------------------

    private static final DecimalFormat decimalFormat =
        new DecimalFormat("###,###,###,###,###");

    //~ Instance fields --------------------------------------------------------

    private final Logger logger;

    private long lastNanoTime;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new timing tracer, publishing an initial event (at elapsed time
     * 0).
     *
     * @param logger logger on which to log timing events; level FINE will be
     * used
     * @param startEvent event to trace as start of timing
     */
    public EigenbaseTimingTracer(
        Logger logger,
        String startEvent)
    {
        if (!logger.isLoggable(Level.FINE)) {
            this.logger = null;
            return;
        } else {
            this.logger = logger;
        }
        lastNanoTime = System.nanoTime();
        logger.fine(startEvent + ":  elapsed nanos=0");
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Publishes an event with the time elapsed since the previous event.
     *
     * @param event event to trace
     */
    public void traceTime(String event)
    {
        if (logger == null) {
            return;
        }
        long newNanoTime = System.nanoTime();
        long elapsed = newNanoTime - lastNanoTime;
        lastNanoTime = newNanoTime;
        logger.fine(
            event + ":  elapsed nanos=" + decimalFormat.format(elapsed));
    }
}

// End EigenbaseTimingTracer.java
