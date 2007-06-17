/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2002-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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

import java.util.logging.*;


/**
 * This class is a small extension to {@link Logger}. {@link Logger#log(Level
 * level, String msg, Object[] params)} is expensive to call, since the caller
 * must always allocate and fill in the array <code>params</code>, even when
 * <code>level</code> will prevent a message being logged. On the other hand,
 * {@link Logger#log(Level level, String msg)} and {@link Logger#log(Level
 * level, String msg, Object)} do not have this problem. As a workaround this
 * class provides {@link #log(Level, String msg, Object, Object)} etc. (The
 * varargs feature of java 1.5 half-solves this problem, by automatically
 * wrapping args in an array, but it does so without testing the level.) Usage:
 * replace: <code>static final Logger tracer =
 * EigenbaseTracer.getMyTracer();</code> by: <code>static final EigenbaseLogger
 * tracer = new EigenbaseLogger(EigenbaseTracer.getMyTracer());</code>
 */
public class EigenbaseLogger
{
    //~ Instance fields --------------------------------------------------------

    private final Logger logger; // delegate

    //~ Constructors -----------------------------------------------------------

    public EigenbaseLogger(Logger logger)
    {
        assert (logger != null);
        this.logger = logger;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Conditionally logs a message with two Object parameters
     */
    public void log(Level level, String msg, Object arg1, Object arg2)
    {
        if (logger.isLoggable(level)) {
            logger.log(
                level,
                msg,
                new Object[] { arg1, arg2 });
        }
    }

    /**
     * Conditionally logs a message with three Object parameters
     */
    public void log(
        Level level,
        String msg,
        Object arg1,
        Object arg2,
        Object arg3)
    {
        if (logger.isLoggable(level)) {
            logger.log(
                level,
                msg,
                new Object[] { arg1, arg2, arg3 });
        }
    }

    /**
     * Conditionally logs a message with four Object parameters
     */
    public void log(
        Level level,
        String msg,
        Object arg1,
        Object arg2,
        Object arg3,
        Object arg4)
    {
        if (logger.isLoggable(level)) {
            logger.log(
                level,
                msg,
                new Object[] { arg1, arg2, arg3, arg4 });
        }
    }

    // We expose and delegate the commonly used part of the Logger interface.
    // For everything else, just expose the delegate. (Could use reflection.)
    public Logger getLogger()
    {
        return logger;
    }

    public void log(Level level, String msg)
    {
        logger.log(level, msg);
    }

    public void log(Level level, String msg, Object param1)
    {
        logger.log(level, msg, param1);
    }

    public void log(Level level, String msg, Object [] params)
    {
        logger.log(level, msg, params);
    }

    public void log(Level level, String msg, Throwable thrown)
    {
        logger.log(level, msg, thrown);
    }

    public void severe(String msg)
    {
        logger.severe(msg);
    }

    public void warning(String msg)
    {
        logger.warning(msg);
    }

    public void info(String msg)
    {
        logger.info(msg);
    }

    public void config(String msg)
    {
        logger.config(msg);
    }

    public void fine(String msg)
    {
        logger.fine(msg);
    }

    public void finer(String msg)
    {
        logger.finer(msg);
    }

    public void finest(String msg)
    {
        logger.finest(msg);
    }
}

// End EigenbaseLogger.java
