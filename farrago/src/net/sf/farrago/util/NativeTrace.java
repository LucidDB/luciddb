/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2003 John V. Sichi
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
 * NativeTrace provides integration between native code and Java logging
 * facilities.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class NativeTrace
{
    //~ Static fields/initializers ---------------------------------------------

    private static NativeTrace instance = null;

    // NOTE jvs 17-Sept-2006:  Values below have to match
    // TraceLevel enum in fennel/common/TraceTarget.h

    private static final int TRACE_PERFCOUNTER_BEGIN_SNAPSHOT = 20002;

    private static final int TRACE_PERFCOUNTER_END_SNAPSHOT = 20001;

    private static final int TRACE_PERFCOUNTER_UPDATE = 20000;

    private static final String SEGMENT_LOGGER_PREFIX = "net.sf.fennel.segment";

    private static final String XO_LOGGER_PREFIX = "net.sf.fennel.xo";

    //~ Instance fields --------------------------------------------------------

    private String loggerPrefix;

    private Map<String, String> perfCounters;

    private Map<String, String> perfCountersNew;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new NativeTrace object. Do not construct NativeTrace objects
     * directly. This constructor is protected only for the use of subclasses.
     *
     * @param loggerPrefix prefix to use in constructing logger names
     */
    protected NativeTrace(String loggerPrefix)
    {
        this.loggerPrefix = loggerPrefix;
        perfCounters = new HashMap<String, String>();
    }

    //~ Methods ----------------------------------------------------------------

    public static void createInstance(String loggerPrefix)
    {
        instance = new NativeTrace(loggerPrefix);
    }

    public static NativeTrace instance()
    {
        return instance;
    }

    private Logger getLogger(
        String loggerSuffix,
        boolean stripLoggerIdentity)
    {
        String loggerName = loggerPrefix + loggerSuffix;
        if (stripLoggerIdentity) {
            // TODO jvs 20-Feb-2008:  See
            // http://issues.eigenbase.org/browse/FRG-309 for improvements
            // needed here.  Fennel dynamically generates logger names such as
            // "net.sf.fennel.xo.FennelReshapRel.#234:501".  This is
            // problematic because once a logger is created, it never
            // goes away, thus there's a small leak with each SQL statement
            // prepared.  To avoid this, we strip off the varying
            // portion of the logger name here for known cases.  The
            // stripping only happens for getSourceTraceLevel to prevent
            // the leak in the common case where the logger is squelched.
            // This means that the leak still occurs when the logger is
            // enabled.  The only way to avoid this would be to stop
            // including object identity in the logger name, and instead
            // move it as a prefix to the logged message.  A proper
            // solution needs to eliminate any special-casing here.
            String identityPrefix = null;
            if (loggerName.startsWith(SEGMENT_LOGGER_PREFIX)) {
                identityPrefix = SEGMENT_LOGGER_PREFIX;
            } else if (loggerName.startsWith(XO_LOGGER_PREFIX)) {
                identityPrefix = XO_LOGGER_PREFIX;
            }
            if (identityPrefix != null) {
                // Trim net.sf.fennel.identityPrefix.foo.bar...
                // to net.sf.fennel.identityPrefix.foo
                if (loggerName.length() > identityPrefix.length()) {
                    if (loggerName.charAt(identityPrefix.length()) == '.') {
                        // skip past the dot after identityPrefix and
                        // look for the next dot (before bar)
                        int iDot =
                            loggerName.indexOf(
                                '.',
                                identityPrefix.length() + 1);
                        if (iDot > -1) {
                            loggerName = loggerName.substring(0, iDot);
                        }
                    }
                }
            }
        }
        return Logger.getLogger(loggerName);
    }

    /**
     * Called from native code to determine the trace level for a given
     * component.
     *
     * @param loggerSuffix suffix to use in constructing logger name
     *
     * @return trace level to use for named source (from Level enum)
     */
    private int getSourceTraceLevel(String loggerSuffix)
    {
        Logger tracer = getLogger(loggerSuffix, true);
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
        if (iLevel >= TRACE_PERFCOUNTER_UPDATE) {
            handlePerfCounter(iLevel, loggerSuffix, message);
            return;
        }
        Logger tracer = getLogger(loggerSuffix, false);
        Level level = Level.parse(Integer.toString(iLevel));
        tracer.logp(level, loggerPrefix + loggerSuffix, "<native>", message);
    }

    private synchronized void handlePerfCounter(
        int iLevel,
        String loggerSuffix,
        String message)
    {
        switch (iLevel) {
        case TRACE_PERFCOUNTER_BEGIN_SNAPSHOT:
            perfCountersNew = new HashMap<String, String>();
            break;
        case TRACE_PERFCOUNTER_END_SNAPSHOT:

            // rollin' rollin' rollin'
            if (perfCountersNew != null) {
                perfCounters = perfCountersNew;
                perfCountersNew = null;
            }
            break;
        case TRACE_PERFCOUNTER_UPDATE:
            if (perfCountersNew != null) {
                perfCountersNew.put(loggerSuffix, message);
            }
            break;
        }
    }

    /**
     * @return a consistent snapshot of all performance counters currently set
     */
    public synchronized Map<String, String> getPerfCounters()
    {
        return perfCounters;
    }
}

// End NativeTrace.java
