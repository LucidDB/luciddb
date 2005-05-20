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

import java.io.*;
import java.sql.*;
import java.util.logging.*;

import org.eigenbase.util.*;
import org.eigenbase.sql.validate.SqlValidatorException;


/**
 * Miscellaneous static utilities that don't fit into other categories.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FarragoUtil
{
    //~ Methods ---------------------------------------------------------------

    /**
     * Calculates the memory used by a string's data (not including the String
     * object itself).  This represents the actual memory used by the Java
     * Unicode representation, not an encoding.
     *
     * @return number of bytes used
     */
    public static int getStringMemoryUsage(String s)
    {
        return s.length() * 2;
    }

    /**
     * Copies everything from a Reader into a Writer.
     *
     * @param reader source
     *
     * @param writer destination
     *
     * @return number of chars copied
     */
    public static int copyFromReaderToWriter(
        Reader reader,
        Writer writer)
        throws IOException
    {
        char [] buf = new char[4096];
        int charsCopied = 0;
        for (;;) {
            int charsRead = reader.read(buf, 0, buf.length);
            if (charsRead == -1) {
                return charsCopied;
            }
            writer.write(buf, 0, charsRead);
            charsCopied += charsRead;
        }
    }

    /**
     * Converts any Throwable to a SQLException.
     *
     * @param ex Throwable to be converted
     *
     * @param tracer Logger on which to trace exceptions as they are
     * converted
     *
     * @return ex as a SQLException
     */
    public static SQLException newSqlException(
        Throwable ex,
        Logger tracer)
    {
        final String message = ex.getMessage();
        tracer.severe(message);
        tracer.throwing("FarragoUtil", "newSqlException", ex);

        Throwable cause = ex.getCause();
        SQLException sqlExcn;
        if (ex instanceof EigenbaseException) {
            // TODO:  map for SQLState
            if (cause instanceof SqlValidatorException) {
                // We're looking at
                //   ex = "Validation error at line 5, column 10"
                //   ex.cause = "Bad column 'FOO'"
                // so the message should be
                //   "Validation error at line 5, column 10: Bad column 'FOO'"
                final String causeMessage = cause.getMessage();
                sqlExcn = new SQLException(message + ": " + causeMessage);
                // Discard this cause and move on to next.
                cause = cause.getCause();
            } else {
                sqlExcn = new SQLException(message);
            }
        } else if (ex instanceof SQLException) {
            sqlExcn = (SQLException) ex;
        } else {
            // for anything else, include the class name
            // as part of what went wrong
            sqlExcn =
                new SQLException(ex.getClass().getName() + ": "
                    + message);
        }

        // preserve additional attributes of the original excn
        sqlExcn.setStackTrace(ex.getStackTrace());

        // convert to SQLException-style chaining
        if (cause != null) {
            // NOTE jvs 18-June-2004:  reverse the order so that
            // the underlying cause comes out on top
            SQLException sqlCause = newSqlException(cause, tracer);
            sqlCause.setNextException(sqlExcn);
            return sqlCause;
        } else {
            return sqlExcn;
        }
    }
}


// End FarragoUtil.java
