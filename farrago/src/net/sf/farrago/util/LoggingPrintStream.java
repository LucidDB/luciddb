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

import java.io.*;
import java.util.logging.*;


/**
 * LoggingPrintStream is a hack to divert debug information from Saffron into
 * the Farrago trace file.
 *
 * <p>
 * TODO:  get Saffron onto the java.util.logging truck
 * </p>
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class LoggingPrintStream extends PrintStream
{
    //~ Constructors ----------------------------------------------------------

    /**
     * Construct a new LoggingPrintStream.
     *
     * @param logger the logger to which to send messages
     * @param level the level at which messages should be logged
     */
    public LoggingPrintStream(
        Logger logger,
        Level level)
    {
        super(new InterceptStream(logger, level), true);
    }

    //~ Inner Classes ---------------------------------------------------------

    /**
     * Private stream for storing intercepted data.
     */
    private static class InterceptStream extends ByteArrayOutputStream
    {
        private Level level;
        private Logger logger;

        /**
         * Creates a new InterceptStream object.
         *
         * @param logger .
         * @param level .
         */
        InterceptStream(
            Logger logger,
            Level level)
        {
            this.logger = logger;
            this.level = level;
        }

        // implement OutputStream
        public void flush()
        {
            String msg = toString().trim();
            if (msg.length() > 0) {
                logger.log(level, msg);
            }
            reset();
        }
    }
}


// End LoggingPrintStream.java
