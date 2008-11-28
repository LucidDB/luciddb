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
import java.util.*;
import java.lang.reflect.*;
import java.io.*;

import org.eigenbase.util.*;
import org.eigenbase.util14.*;


/**
 * Utility functions for the Farrago JDBC driver.
 *
 * <p>This class is JDK 1.4 compatible.
 *
 * @author angel
 * @version $Id$
 * @since Mar 18, 2006
 */
public class FarragoJdbcUtil
{
    /**
     * Contains the serialization checker for each thread.
     */
    private static final ThreadLocal/*<SerializationChecker>*/ threadChecker =
        new ThreadLocal/*<SerializationChecker>*/()
        {
            protected Object /*SerializationChecker*/ initialValue()
            {
                return new SerializationChecker();
            }
        };

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
            String stmt = null;
            if (ex instanceof EigenbaseContextException) {
                stmt = ((EigenbaseContextException)ex).getOriginalStatement();
            }
            // TODO:  map for SQLState
            if (cause instanceof EigenbaseValidatorException) {
                // We're looking at
                //   ex = "Validation error at line 5, column 10"
                //   ex.cause = "Bad column 'FOO'"
                // so the message should be
                //   "Validation error at line 5, column 10: Bad column 'FOO'"
                final String causeMessage = cause.getMessage();
                //noinspection ThrowableInstanceNeverThrown
                sqlExcn =
                    new FarragoSqlException(
                        message + ": " + causeMessage,
                        ex,
                        stmt,
                        null);

                // Discard this cause and move on to next.
                cause = cause.getCause();
            } else {
                //noinspection ThrowableInstanceNeverThrown
                sqlExcn = new FarragoSqlException(message, ex, stmt, null);
            }
        } else if (ex instanceof SQLException) {
            sqlExcn = (SQLException) ex;
        } else {
            // for anything else, include the class name
            // as part of what went wrong
            //noinspection ThrowableInstanceNeverThrown
            sqlExcn =
                new FarragoSqlException(
                    ex.getClass().getName() + ": "
                    + message,
                    ex,
                    null,
                    null);
        }

        // preserve additional attributes of the original excn
        sqlExcn.setStackTrace(ex.getStackTrace());

        // Convert to SQLException-style chaining. That means that the
        // underlying cause -- that is, the exception at the end of the cause
        // chain -- comes out on top.
        //
        // If this is a parse exception, the underlying cause will be a
        // generated class specific to our particular parser implementation, so
        // we stop at the SqlParseException which is just above it.
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
        //noinspection ThrowableInstanceNeverThrown
        SQLException ex = new SQLException(message);

        // REVIEW: not sure tracing everything is desirable. Consider
        // allowing/testing for null tracer, or creating an alternative
        // newSqlException(message) method.
        tracer.severe(message);
        tracer.throwing("FarragoJdbcUtil", "newSqlException(msg)", ex);
        return ex;
    }

    /**
     * Walks an exception stack using reflection looking for the original SQL
     * statement that MAY be in the exception stack.
     *
     * @param ex top of exception stack
     * @return Original input text that generated the error or  <code>null</code>
     */
    public static String findInputString(final Throwable ex)
    {
        Class clazz = ex.getClass();

        // Check to see if the current exception has the string we are looking
        // for.
        try {
            // EigenbaseContextException and FarragoSqlException both have a
            // getOriginalStatement() method.
            Method meth =
                clazz.getMethod("getOriginalStatement", (Class[]) null);
            Object valObj = meth.invoke(ex, (Object[]) null);
            if (valObj != null) {
                return (String)valObj;
            }
        } catch (IllegalAccessException e) {
            //intentionally empty
        } catch (NoSuchMethodException e) {
            //intentionally empty
        } catch (InvocationTargetException e) {
            //intentionally empty
        }

        // Here is where is starts getting tricky.  Parser and Validator chain
        // expceptions in different ways.  Parser exceptions use the "next"
        // field of java.lang.SQLException while the validator chains through
        // the "original" field defined in FarragoSqlException.  None of the
        // methods seem to use the "cause" field defined by Throwable.
        //
        // Since the parser can have entries in both "original" and "next" we
        // will try "next" first.
        try {
            // SQLException has a method getNextException()
            Method meth = clazz.getMethod("getNextException", (Class[]) null);
            Object valObj = meth.invoke(ex, (Object[]) null);
            if ((valObj != null) && ((Throwable)valObj != ex)) {
                String query = findInputString((Throwable)valObj);
                if (query != null) {
                    return query;
                }
            }
        } catch (IllegalAccessException e) {
            // intentionally empty
        } catch (NoSuchMethodException e) {
            // intentionally empty
        } catch (InvocationTargetException e) {
            // intentionally empty
        }

        // Check for the original cause
        try {
            // FarragoSqlException has a method getOriginalThrowable().
            Method meth =
                clazz.getMethod("getOriginalThrowable", (Class[]) null);
            Object valObj = meth.invoke(ex, (Object[]) null);
            if ((valObj != null) && ((Throwable) valObj != ex)) {
                return findInputString((Throwable)valObj);
            }
        } catch (IllegalAccessException e) {
             // intentionally empty
        } catch (NoSuchMethodException e) {
             // intentionally empty
        } catch (InvocationTargetException e) {
             // intentionally empty
        }
        return null;
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Tests whether an object (in particular an exception) is serializable.
     * Maintains a list of objects actively being tested so that we do not
     * recursively test the same object.
     */
    private static class SerializationChecker
    {
        private final Set/*<Object>*/ active = new HashSet/*<Object>*/();

        /**
         * Whether the client has the RMI class loader enabled. If true,
         * serializabilty is sufficient. If false, the class also has to be
         * available on the client.
         */
        private final boolean rmiClassLoader = true;

        /**
         * Converts a {@code Throwable} into a similar exception that is
         * serializable. The original object, or at least its cause, is
         * used if possible; and the new throwable has the same stack trace.
         *
         * @param throwable Exception
         * @return Exception that is serializable and of the same general type
         */
        Throwable makeSerializable(Throwable throwable)
        {
            // REVIEW: Is serializability sufficient? The class may not be
            // available on the client.
            if (isSerializable(throwable)) {
                return throwable;
            }
            Throwable cause = throwable.getCause();
            if (cause == throwable) {
                cause = null;
            }
            if (cause != null) {
                cause = makeSerializable(cause);
            }
            String message =
                throwable.getClass().getName() + ": " + throwable.getMessage();
            Throwable serializable;
            if (throwable instanceof RuntimeException) {
                //noinspection ThrowableInstanceNeverThrown
                serializable = new RuntimeException(message, cause);
            } else if (throwable instanceof Exception) {
                //noinspection ThrowableInstanceNeverThrown
                serializable = new Exception(message, cause);
            } else if (throwable instanceof Error) {
                //noinspection ThrowableInstanceNeverThrown
                serializable = new Error(message, cause);
            } else {
                //noinspection ThrowableInstanceNeverThrown
                serializable = new Throwable(message, cause);
            }
            serializable.setStackTrace(throwable.getStackTrace());

            return serializable;
        }

        /**
         * Returns whether an object is serializable.
         *
         * @param o Object
         * @return Whether object is serializable
         */
        boolean isSerializable(Object o)
        {
            if (!rmiClassLoader) {
                String className = o.getClass().getName();
                if (className.startsWith("org.eigenbase.sql.")) {
                    // In particular:
                    //  org.eigenbase.sql.parser.SqlParseException
                    //  org.eigenbase.sql.parser.SqlParserPos
                    //  org.eigenbase.sql.SqlNode (and subclasses)
                    return false;
                }
            }
            if (!active.add(o)) {
                // The object is already being tested for serialization. Tell a
                // white lie and say that it is serializable. If it is not
                // serializable, the first call will find out.
                return true;
            }
            try {
                ObjectOutputStream oos =
                    new ObjectOutputStream(
                        new ByteArrayOutputStream());
                oos.writeObject(o);
                return true;
            } catch (NotSerializableException e) {
                if (o instanceof EigenbaseParserException) {
                    // We know there are problems serializing
                    // EigenbaseParserException: specifically, the cause of a
                    // SqlParseException is usually a ParseException generated
                    // by JavaCC, and this is not serializable because it contains
                    // Token.
                } else if (o instanceof Serializable) {
                    // If you get this error, you should fix the class and make
                    // sure all of its fields are types that extend Serializable.
                    // Then as long as the instances of those types are being
                    // honest, everything should be hunky dory.
                    System.out.println(
                        "Warning: Object [" + o + "] of class " + o.getClass()
                            + " implements Serializable but is not serializable. "
                            + "Error is as follows:");
                    e.printStackTrace(System.out);
                }
                return false;
            } catch (IOException e) {
                throw new RuntimeException(
                    "Error while testing serializability", e);
            } finally {
                active.remove(o);
            }
        }
    }

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
         * Original exception.
         */
        private final Throwable original;

        private final String originalStatement;

        /**
         * Creates an exception with a message and a record of the undiluted
         * original exception.
         *
         * @param reason   A description of the exception
         * @param original Original exception
         * @param originalStatement Original statement
         */
        public FarragoSqlException(
            String reason,
            Throwable original,
            String originalStatement,
            Throwable cause)
        {
            super(reason);
            initCause(cause);
            this.original = original;
            this.originalStatement = originalStatement;
        }

        /**
         * Returns the original exception.
         *
         * @return original exception
         */
        public Throwable getOriginalThrowable()
        {
            return original;
        }

        /**
         * Returns the original statement.
         *
         * @return original statement
         */
        public String getOriginalStatement()
        {
            return originalStatement;
        }

        /**
         * Per {@link java.io.Serializable} API, provides a replacement object
         * to be written during serialization.
         *
         * <p>This implementation converts this FarragoSqlException into an
         * exception that looks similar but is serializable.
         */
        private Object writeReplace()
        {
            boolean needNewException = false;
            Throwable serializableOriginal;
            final SerializationChecker checker =
                (SerializationChecker) threadChecker.get();

            // Replace original if it is not serializable.
            if (original != null
                && original != this
                && !checker.isSerializable(original))
            {
                needNewException = true;
                serializableOriginal = checker.makeSerializable(original);
            } else {
                serializableOriginal = original;
            }

            // Replace next if it is not serializable.
            final SQLException next = getNextException();
            SQLException serializableNext;
            if (next != null
                && next != this
                && !checker.isSerializable(next))
            {
                needNewException = true;
                serializableNext =
                    (SQLException) checker.makeSerializable(next);
            } else {
                serializableNext = next;
            }

            final Throwable cause = getCause();
            Throwable serializableCause;
            if (cause != null
                && cause != this
                && !checker.isSerializable(cause))
            {
                needNewException = true;
                serializableCause = checker.makeSerializable(cause);
            } else {
                serializableCause = cause;
            }

            // If original, next and cause are all serializable, we can use the
            // original exception. Otherwise we need a new exception with the
            // non-serializable parts replaced.
            if (needNewException) {
                //noinspection ThrowableInstanceNeverThrown
                final FarragoSqlException fse =
                    new FarragoSqlException(
                        getMessage(),
                        serializableOriginal,
                        originalStatement,
                        serializableCause);
                fse.setNextException(serializableNext);
                return fse;
            } else {
                return this;
            }
        }
    }
}

// End FarragoJdbcUtil.java
