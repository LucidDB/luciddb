/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2004-2007 John V. Sichi
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
package net.sf.farrago.jdbc;

import java.sql.*;

import java.util.logging.*;

import org.eigenbase.util.*;
import org.eigenbase.util14.*;


/**
 * Utility functions for the Farrago JDBC driver (refactored from FarragoUtil)
 *
 * @author angel
 * @version $Id$
 * @since Mar 18, 2006
 */
public class FarragoJdbcUtil
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Converts any Throwable to a SQLException.
     *
     * @param ex Throwable to be converted
     * @param tracer Logger on which to trace exceptions as they are converted;
     * must not be <code>null</code>
     *
     * @return ex as a SQLException
     */
    public static SQLException newSqlException(
        final Throwable ex,
        Logger tracer)
    {
        final String message = ex.getMessage();
        tracer.severe(message);
        tracer.throwing("FarragoJdbcUtil", "newSqlException(ex)", ex);

        Throwable cause = ex.getCause();
        SQLException sqlExcn;
        if (ex instanceof EigenbaseException) {
            // TODO:  map for SQLState
            if (cause instanceof EigenbaseValidatorException) {
                // We're looking at
                //   ex = "Validation error at line 5, column 10"
                //   ex.cause = "Bad column 'FOO'"
                // so the message should be
                //   "Validation error at line 5, column 10: Bad column 'FOO'"
                final String causeMessage = cause.getMessage();
                sqlExcn =
                    new FarragoSqlException(message + ": " + causeMessage, ex);

                // Discard this cause and move on to next.
                cause = cause.getCause();
            } else {
                sqlExcn = new FarragoSqlException(message, ex);
            }
        } else if (ex instanceof SQLException) {
            sqlExcn = (SQLException) ex;
        } else {
            // for anything else, include the class name
            // as part of what went wrong
            sqlExcn =
                new FarragoSqlException(
                    ex.getClass().getName() + ": "
                    + message,
                    ex);
        }

        // preserve additional attributes of the original excn
        sqlExcn.setStackTrace(ex.getStackTrace());

        // Convert to SQLException-style chaining. That means that the
        // underlying cause -- that is, the exception at the end of the cause
        // chain -- comes out on top.
        //
        // If this is a parse exception, the underlying cause will be a generated
        // class specific to our particular parser implementation, so we stop at
        // the SqlParseException which is just above it.
        if (cause == null) {
            return sqlExcn;
        } else if (ex instanceof EigenbaseParserException) {
            return sqlExcn;
        } else {
            // NOTE jvs 18-June-2004:  reverse the order so that
            // the underlying cause comes out on top
            SQLException sqlCause = newSqlException(cause, tracer);
            sqlCause.setNextException(sqlExcn);
            return sqlCause;
        }
    }

    /**
     * Creates a new SQLException.
     *
     * @param message detail message, the reason for this exception
     * @param tracer Logger on which to trace new exceptions; must not be <code>
     * null</code>
     *
     * @return new SQLException
     */
    public static SQLException newSqlException(
        final String message,
        Logger tracer)
    {
        SQLException ex = new SQLException(message);

        // REVIEW: not sure tracing everything is desirable. Consider
        // allowing/testing for null tracer, or creating an alternative
        // newSqlException(message) method.
        tracer.severe(message);
        tracer.throwing("FarragoJdbcUtil", "newSqlException(msg)", ex);
        return ex;
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Exception thrown by Farrago JDBC driver.
     *
     * <p>The exception contains the original, undiluted exception for more
     * detailed diagnostics. This is used by the testing infrastructure to
     * ensure that the error occurs at the right (line, col) thru (line, col)
     * position.
     *
     * <p>The original exception is returned by the {@link
     * #getOriginalThrowable()} method, but will not be returned from the
     * standard {@link #getNextException()} or {@link #getCause()} methods; this
     * exception therefore behaves exactly like a regular {@link
     * java.sql.SQLException}.
     */
    public static class FarragoSqlException
        extends SQLException
    {
        /**
         * SerialVersionUID created with JDK 1.5 serialver tool.
         */
        private static final long serialVersionUID = -2302810435386763566L;

        /**
         * Original exception. Marked 'transient' so that it does not prevent
         * this exception from being serializable.
         */
        private transient final Throwable original;

        /**
         * Creates an exception with a message and a record of the undiluted
         * original exception.
         *
         * @param s
         * @param original
         */
        public FarragoSqlException(
            String s,
            Throwable original)
        {
            super(s);
            this.original = original;
        }

        public Throwable getOriginalThrowable()
        {
            return original;
        }
    }
}

// End FarragoJdbcUtil.java
