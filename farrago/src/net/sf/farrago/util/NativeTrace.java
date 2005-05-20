/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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

import java.util.logging.*;


/**
 * NativeTrace provides integration between native code and Java logging
 * facilities.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class NativeTrace
{
    //~ Instance fields -------------------------------------------------------

    private String loggerPrefix;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new NativeTrace object.
     *
     * @param loggerPrefix prefix to use in constructing logger names
     */
    public NativeTrace(String loggerPrefix)
    {
        this.loggerPrefix = loggerPrefix;
    }

    //~ Methods ---------------------------------------------------------------

    private Logger getLogger(String loggerSuffix)
    {
        return Logger.getLogger(loggerPrefix + loggerSuffix);
    }

    /**
     * Called from native code to determine the trace level
     * for a given component.
     *
     * @param loggerSuffix suffix to use in constructing logger name
     *
     * @return trace level to use for named source (from Level enum)
     */
    private int getSourceTraceLevel(String loggerSuffix)
    {
        Logger tracer = getLogger(loggerSuffix);
        for (;;) {
            Level level = tracer.getLevel();
            if (level != null) {
                return level.intValue();
            }
            tracer = tracer.getParent();
            if (tracer == null) {
                return Level.OFF.intValue();
            }
        }
    }

    /**
     * Called from native code to emit a trace message.
     *
     * @param loggerSuffix suffix to use in constructing logger name
     * @param iLevel level (from Level enum) at which to trace
     * @param message trace message text
     */
    private void trace(
        String loggerSuffix,
        int iLevel,
        String message)
    {
        Logger tracer = getLogger(loggerSuffix);
        Level level = Level.parse(Integer.toString(iLevel));
        tracer.logp(level, loggerPrefix + loggerSuffix, "<native>", message);
    }
}


// End NativeTrace.java
