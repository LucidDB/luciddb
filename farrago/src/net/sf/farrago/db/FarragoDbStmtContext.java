/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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
package net.sf.farrago.db;

import java.sql.*;
import java.util.*;
import java.util.logging.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.query.*;
import net.sf.farrago.resource.FarragoResource;
import net.sf.farrago.runtime.*;
import net.sf.farrago.session.*;
import net.sf.farrago.trace.*;
import net.sf.farrago.util.*;
import net.sf.farrago.fennel.*;

import org.eigenbase.oj.stmt.*;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.runtime.IteratorResultSet;
import org.eigenbase.sql.SqlKind;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.util.*;


/**
 * FarragoDbStmtContext implements the
 * {@link net.sf.farrago.session.FarragoSessionStmtContext} interface
 * in terms of a {@link FarragoDbSession}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoDbStmtContext implements FarragoSessionStmtContext
{
    //~ Static fields/initializers --------------------------------------------

    private static final Logger tracer =
        FarragoTrace.getDatabaseStatementContextTracer();

    //~ Instance fields -------------------------------------------------------

    private int updateCount;
    private FarragoDbSession session;

    /**
     * Definitions of dynamic parameters.
     */
    private ParamDef [] dynamicParamDefs;
    private Object [] dynamicParamValues;
    private boolean daemon;
    private ResultSet resultSet;
    private FarragoSessionExecutableStmt executableStmt;
    private FarragoCompoundAllocation allocations;
    private String sql;
    
    private FennelStreamGraph streamGraph;
    private Object streamGraphMutex = new Integer(0);

    /**
     * query timeout in seconds, default to 0.
     */
    private int queryTimeoutMillis = 0;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FarragoDbStmtContext object.
     *
     * @param session the session creating this statement
     */
    public FarragoDbStmtContext(FarragoDbSession session)
    {
        this.session = session;
        updateCount = -1;
    }

    //~ Methods ---------------------------------------------------------------

    // implement FarragoAllocation
    public void closeAllocation()
    {
        unprepare();

        // purge self from session's list
        session.forgetAllocation(this);
    }

    // implement FarragoSessionStmtContext
    public FarragoSession getSession()
    {
        return session;
    }

    // implement FarragoSessionStmtContext
    public boolean isPrepared()
    {
        return (executableStmt != null);
    }

    // implement FarragoSessionStmtContext
    public boolean isPreparedDml()
    {
        return executableStmt.isDml();
    }

    // implement FarragoSessionStmtContext
    public void daemonize()
    {
        daemon = true;
    }

    // implement FarragoSessionStmtContext
    public void prepare(
        String sql,
        boolean isExecDirect)
    {
        unprepare();
        allocations = new FarragoCompoundAllocation();
        this.sql = sql;
        executableStmt = session.prepare(sql, allocations, isExecDirect, null);
        postprepare();
    }

    private void postprepare()
    {
        if (isPrepared()) {
            final RelDataType dynamicParamRowType =
                executableStmt.getDynamicParamRowType();
            final RelDataTypeField [] fields = dynamicParamRowType.getFields();

            // Allocate an array to hold parameter values.
            dynamicParamValues = new Object[fields.length];

            // Allocate an array of validators, one for each parameter.
            dynamicParamDefs = new ParamDef[fields.length];
            for (int i = 0; i < fields.length; i++) {
                final RelDataTypeField field = fields[i];
                dynamicParamDefs[i] =
                    ParamDef.create(
                        field.getName(),
                        field.getType());
            }
        } else {
            // always zero for DDL
            updateCount = 0;
        }
    }

    // implement FarragoSessionStmtContext
    public void prepare(
        RelNode plan,
        SqlKind kind,
        boolean logical,
        FarragoSessionPreparingStmt prep)
    {
        unprepare();
        allocations = new FarragoCompoundAllocation();
        this.sql = ""; // not available

        executableStmt =
            session.getDatabase().implementStmt(prep, plan, kind, logical,
                allocations);
        postprepare();
    }

    // implement FarragoSessionStmtContext
    public RelDataType getPreparedRowType()
    {
        assert (isPrepared());
        return executableStmt.getRowType();
    }

    // implement FarragoSessionStmtContext
    public RelDataType getPreparedParamType()
    {
        assert (isPrepared());
        return executableStmt.getDynamicParamRowType();
    }

    // implement FarragoSessionStmtContext
    public void setDynamicParam(
        int parameterIndex,
        Object x)
    {
        assert (isPrepared());
        Object y = dynamicParamDefs[parameterIndex].scrubValue(x);
        dynamicParamValues[parameterIndex] = y;
    }

    // implement FarragoSessionStmtContext
    public void clearParameters()
    {
        assert (isPrepared());
        Arrays.fill(dynamicParamValues, null);
    }

    // implement FarragoSessionStmtContext
    public void setQueryTimeout(int millis)
    {
        queryTimeoutMillis = millis;
    }

    // implement FarragoSessionStmtContext
    public int getQueryTimeout()
    {
        return queryTimeoutMillis;
    }

    // implement FarragoSessionStmtContext
    public void execute()
    {
        assert (isPrepared());
        closeResultSet();
        traceExecute();
        boolean isDml = executableStmt.isDml();
        boolean success = false;

        try {
            FarragoSessionRuntimeParams params =
                session.newRuntimeContextParams();
            if (!isDml) {
                params.txnCodeCache = null;
            }
            params.dynamicParamValues = dynamicParamValues;
            FarragoSessionRuntimeContext context =
                session.getPersonality().newRuntimeContext(params);
            if (allocations != null) {
                context.addAllocation(allocations);
                allocations = null;
            }
            if (daemon) {
                context.addAllocation(this);
            }

            resultSet = executableStmt.execute(context);
            synchronized(streamGraphMutex) {
                streamGraph = context.getFennelStreamGraph();
            }

            if (queryTimeoutMillis > 0) {
                IteratorResultSet iteratorRS = (IteratorResultSet) resultSet;
                iteratorRS.setTimeout(queryTimeoutMillis);
            }
            success = true;
        } finally {
            if (!success) {
                session.endTransactionIfAuto(false);
            }
        }
        if (isDml) {
            success = false;
            try {
                boolean found = resultSet.next();
                assert (found);
                updateCount = resultSet.getInt(1);
                // REVIEW jvs 13-Sept-2004:  johnp, is this still needed?
                while (resultSet.next()) {
                }
                if (tracer.isLoggable(Level.FINE)) {
                    tracer.fine("Update count = " + updateCount);
                }
                success = true;
            } catch (SQLException ex) {
                throw Util.newInternal(ex);
            } finally {
                if (!success) {
                    session.endTransactionIfAuto(false);
                }
                try {
                    resultSet.close();
                } catch (SQLException ex) {
                    throw Util.newInternal(ex);
                } finally {
                    resultSet = null;
                    synchronized(streamGraphMutex) {
                        streamGraph = null;
                    }
                }
            }
        }

        // NOTE:  for now, we only auto-commit after DML.  Queries aren't an
        // issue until locking gets implemented.
        if (resultSet == null) {
            session.endTransactionIfAuto(true);
        }
    }

    // implement FarragoSessionStmtContext
    public ResultSet getResultSet()
    {
        return resultSet;
    }

    // implement FarragoSessionStmtContext
    public int getUpdateCount()
    {
        int count = updateCount;
        updateCount = -1;
        return count;
    }

    // implement FarragoSessionStmtContext
    public void cancel()
    {
        synchronized(streamGraphMutex) {
            if (streamGraph != null) {
                streamGraph.abort();
            }
        }
    }

    // implement FarragoSessionStmtContext
    public void closeResultSet()
    {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (Throwable ex) {
                throw Util.newInternal(ex);
            }
            resultSet = null;
            synchronized(streamGraphMutex) {
                streamGraph = null;
            }
        }
    }

    // implement FarragoSessionStmtContext
    public void unprepare()
    {
        closeResultSet();
        if (allocations != null) {
            allocations.closeAllocation();
            allocations = null;
        }
        executableStmt = null;
        sql = null;
        dynamicParamValues = null;
    }

    void traceExecute()
    {
        if (!tracer.isLoggable(Level.FINE)) {
            return;
        }
        tracer.fine(sql);
        if (!tracer.isLoggable(Level.FINER)) {
            return;
        }
        for (int i = 0; i < dynamicParamValues.length; ++i) {
            tracer.finer("?" + (i + 1) + " = [" + dynamicParamValues[i] + "]");
        }
    }

    //~ Inner Classes ---------------------------------------------------------

    /**
     * Enforces constraints on parameters.
     *
     * The constraints are:<ol>
     *
     * <li>Ensures that null values cannot be inserted into not-null columns.
     *
     * <li>Ensures that value is the right type.
     *
     * <li>Ensures that the value is within range. For example, you can't
     *    insert a 10001 into a DECIMAL(5) column.
     *
     * </ol>
     *
     * <p>TODO: Actually enfore these constraints.
     */
    private static class ParamDef
    {
        static final TimeZone defaultZone = TimeZone.getDefault();
        static final TimeZone gmtZone = TimeZone.getTimeZone("GMT");
        final RelDataType type;
        final String paramName;

        public ParamDef(
            String paramName,
            RelDataType type)
        {
            this.type = type;
            this.paramName = paramName;
        }

        /**
         * Creates a parameter definition.
         *
         * @post return != null
         */
        static ParamDef create(
            String paramName,
            RelDataType type)
        {
            final SqlTypeName sqlTypeName = type.getSqlTypeName();
            switch (sqlTypeName.ordinal) {
            case SqlTypeName.Char_ordinal:
            case SqlTypeName.Varchar_ordinal:
                return new StringParamDef(paramName, type);
            case SqlTypeName.Binary_ordinal:
            case SqlTypeName.Varbinary_ordinal:
                return new BinaryParamDef(paramName, type);
            case SqlTypeName.Date_ordinal:
                return new DateParamDef(paramName, type);
            case SqlTypeName.Timestamp_ordinal:
                return new TimestampParamDef(paramName, type);
            case SqlTypeName.Time_ordinal:
                return new TimeParamDef(paramName, type);
            default:
                return new ParamDef(paramName, type);
            }
        }

        /**
         * Checks the type of a value, and throws an error if it is invalid.
         *
         * @param x
         * @return Value to be sent into the depths...
         */
        public Object scrubValue(Object x)
        {
            return x;
        }

        /**
         * Returns an error that the value is not valid for the desired SQL
         * type.
         */
        protected EigenbaseException newInvalidType(Object x)
        {
            return FarragoResource.instance().newParameterValueIncompatible(
                x.getClass().getName(),
                type.toString());
        }
    }

    // TODO jvs 7-Oct-2004: according to Appendix B of the JDBC spec (Data Type
    // Conversion Tables), it's possible to pass String objects as the values
    // for date/time/timestamp/binary parameters.  Need to implement the
    // appropriate conversions here.  Also, need a NumericParamDef impl.
    
    /**
     * Definition of a Timestamp parameter. Converts parameters from local time
     * (the JVM's timezone) into system time.
     */
    private static class TimestampParamDef extends ParamDef
    {
        public TimestampParamDef(
            String paramName,
            RelDataType type)
        {
            super(paramName, type);
        }

        public Object scrubValue(Object x)
        {
            // java.sql.Date, java.sql.Time, java.sql.Timestamp are all OK.
            if (!(x instanceof java.util.Date)) {
                throw newInvalidType(x);
            }
            java.util.Date timestamp = (java.util.Date) x;
            long millis = timestamp.getTime();
            int timeZoneOffset = defaultZone.getOffset(millis);

            // shift the time into gmt
            return new Timestamp(millis + timeZoneOffset);
        }
    }

    /**
     * Definition of a date parameter. Converts parameters from local time
     * (the JVM's timezone) into system time.
     */
    private static class DateParamDef extends ParamDef
    {
        public DateParamDef(
            String paramName,
            RelDataType type)
        {
            super(paramName, type);
        }

        public Object scrubValue(Object x)
        {
            if (!(x instanceof java.util.Date)) {
                throw newInvalidType(x);
            }
            java.util.Date date = (java.util.Date) x;
            final long millis = date.getTime();
            final long shiftedMillis;

            // Shift time into gmt and truncate to previous midnight.
            // (There's probably a more efficient way of doing this.)
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(millis);

            // Truncate to midnight before we shift into GMT, just in case
            // the untruncated date falls in a different day in GMT.
            cal.set(Calendar.HOUR_OF_DAY, 0);
            cal.set(Calendar.MINUTE, 0);
            cal.set(Calendar.SECOND, 0);
            cal.set(Calendar.MILLISECOND, 0);

            // Shift into gmt and truncate again.
            cal.setTimeZone(gmtZone);
            cal.set(Calendar.HOUR_OF_DAY, 0);
            cal.set(Calendar.MINUTE, 0);
            cal.set(Calendar.SECOND, 0);
            cal.set(Calendar.MILLISECOND, 0);
            shiftedMillis = cal.getTimeInMillis();

            return new java.sql.Date(shiftedMillis);
        }
    }

    /**
     * Definition of a time parameter. Converts parameters from local time
     * (the JVM's timezone) into system time.
     */
    private static class TimeParamDef extends ParamDef
    {
        public TimeParamDef(
            String paramName,
            RelDataType type)
        {
            super(paramName, type);
        }

        public Object scrubValue(Object x)
        {
            // java.sql.Date, java.sql.Time, java.sql.Timestamp are all OK.
            if (!(x instanceof java.util.Date)) {
                throw newInvalidType(x);
            }
            java.util.Date time = (java.util.Date) x;

            // create a calendar containing time in locale timezone
            Calendar cal = Calendar.getInstance();
            cal.setTime(time);
            final int hour = cal.get(Calendar.HOUR_OF_DAY);
            final int minute = cal.get(Calendar.MINUTE);
            final int second = cal.get(Calendar.SECOND);
            final int millisecond = cal.get(Calendar.MILLISECOND);

            // set date to epoch
            cal.clear();

            // shift to gmt
            cal.setTimeZone(gmtZone);

            // now restore the time part
            cal.set(Calendar.HOUR_OF_DAY, hour);
            cal.set(Calendar.MINUTE, minute);
            cal.set(Calendar.SECOND, second);
            cal.set(Calendar.MILLISECOND, millisecond);

            // convert to a time object
            return new Time(cal.getTimeInMillis());
        }
    }

    /**
     * Definition of a string parameter. Values which are not strings are
     * converted into strings. Strings are not padded, even for CHAR columns.
     */
    private static class StringParamDef extends ParamDef
    {
        private final int maxCharCount;

        public StringParamDef(
            String paramName,
            RelDataType type)
        {
            super(paramName, type);
            maxCharCount = type.getPrecision();
        }

        public Object scrubValue(Object x)
        {
            if (x == null) {
                return x;
            }
            if (x instanceof String) {
                return x;
            }
            // REVIEW jvs 7-Oct-2004: the default toString() implementation for
            // Float/Double/Date/Time/Timestamp/byte[] may not be correct here.
            final String s = x.toString();
            if (s.length() > maxCharCount) {
                throw FarragoResource.instance().newParameterValueTooLong(
                    s,
                    type.toString());
            }
            return s;
        }
    }

    /**
     * Definition of a binary parameter. Only accepts byte-array values.
     */
    private static class BinaryParamDef extends ParamDef
    {
        private final int maxByteCount;

        public BinaryParamDef(
            String paramName,
            RelDataType type)
        {
            super(paramName, type);
            maxByteCount = type.getPrecision();
        }

        public Object scrubValue(Object x)
        {
            if (x == null) {
                return x;
            }
            if (!(x instanceof byte [])) {
                throw newInvalidType(x);
            }
            final byte [] bytes = (byte []) x;
            if (bytes.length > maxByteCount) {
                throw FarragoResource.instance().newParameterValueTooLong(
                    Util.toStringFromByteArray(bytes,16),
                    type.toString());
            }
            return bytes;
        }
    }
}


// End FarragoDbStmtContext.java
