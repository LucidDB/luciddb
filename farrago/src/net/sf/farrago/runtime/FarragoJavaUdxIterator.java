/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006 The Eigenbase Project
// Copyright (C) 2006 SQLstream, Inc.
// Copyright (C) 2006 Dynamo BI Corporation
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
package net.sf.farrago.runtime;

import java.lang.reflect.*;

import java.math.*;

import java.sql.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;

import net.sf.farrago.jdbc.param.*;
import net.sf.farrago.session.*;
import net.sf.farrago.trace.*;
import net.sf.farrago.type.*;
import net.sf.farrago.type.runtime.*;

import org.eigenbase.reltype.*;
import org.eigenbase.runtime.*;
import org.eigenbase.util.*;


/**
 * FarragoJavaUdxIterator provides runtime support for a call to a Java UDX. It
 * supports both the blocking interface {@link Iterator} and the non-blocking
 * {@link TupleIter}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FarragoJavaUdxIterator
    extends ThreadIterator
    implements RestartableIterator,
        TupleIter
{
    //~ Static fields/initializers ---------------------------------------------

    private static final int QUEUE_ARRAY_SIZE = 100;
    protected static final Logger tracer =
        FarragoTrace.getRuntimeContextTracer();

    //~ Instance fields --------------------------------------------------------

    private final FarragoSyntheticObject [] rowObjs;

    private final PreparedStatement resultInserter;

    // protected because needed by generated subclasses
    protected final FarragoSessionRuntimeContext runtimeContext;

    private int iRow;
    private long defaultTimeout = Long.MAX_VALUE;
    private boolean timeoutAsUnderflow = true;
    private boolean didUnderflow = false;
    private boolean stopThread;

    private CountDownLatch latch;
    private final ParameterMetaData parameterMetaData;
    private List<TupleIter> restartableInputs;
    private List<MoreDataListener> moreDataListeners;

    //~ Constructors -----------------------------------------------------------

    protected FarragoJavaUdxIterator(
        FarragoSessionRuntimeContext runtimeContext,
        Class rowClass,
        RelDataType rowType)
    {
        super(new ArrayBlockingQueue(QUEUE_ARRAY_SIZE));
        this.runtimeContext = runtimeContext;
        runtimeContext.addAllocation(this);

        parameterMetaData = new FarragoParameterMetaData(rowType);

        // NOTE jvs 16-Jan-2006: We construct a circular array with two extra
        // slots:  one for the producer thread to write into, and one for the
        // consumer thread to read from; this guarantees that we
        // never recycle a row still accessible by the consumer.
        rowObjs = new FarragoSyntheticObject[QUEUE_ARRAY_SIZE + 2];
        try {
            for (int i = 0; i < rowObjs.length; ++i) {
                rowObjs[i] = (FarragoSyntheticObject) rowClass.newInstance();
            }
        } catch (Throwable ex) {
            throw Util.newInternal(ex);
        }
        iRow = 0;
        resultInserter =
            (PreparedStatement) Proxy.newProxyInstance(
                null,
                new Class[] { PreparedStatement.class },
                new PreparedStatementInvocationHandler(rowType));

        restartableInputs = new ArrayList<TupleIter>();
        moreDataListeners = new ArrayList<MoreDataListener>();
    }

    //~ Methods ----------------------------------------------------------------

    // override QueueIterator
    public boolean hasNext()
    {
        if (latch == null) {
            // NOTE: we don't actually start the thread until the first call to
            // hasNext, because first we need "this" to be fully constructed,
            // including subclasses; also the Fennel plan needs to be loaded.
            startWithLatch();
        }
        return super.hasNext();
    }

    // override QueueIterator
    public boolean hasNext(long timeout)
        throws QueueIterator.TimeoutException
    {
        if (latch == null) {
            startWithLatch();
        }
        return super.hasNext(timeout);
    }

    // implement TupleIter
    public boolean setTimeout(long timeout, boolean asUnderflow)
    {
        this.defaultTimeout = timeout;
        this.timeoutAsUnderflow = asUnderflow;
        return true;
    }

    // implement TupleIter
    public boolean addListener(MoreDataListener c)
    {
        if (tracer.isLoggable(Level.FINE)) {
            tracer.log(
                Level.FINE, "FarragoJavaUdxIterator {0} added listener {1}",
                new Object[] {this, c});
        }
        moreDataListeners.add(c);
        return true;
    }

    private void onUnderflow()
    {
        tracer.fine("underflow");
        didUnderflow = true;
    }

    private void onData()
    {
        if (didUnderflow) {
            tracer.fine("more data after underflow");
            didUnderflow = false;
            for (MoreDataListener c : moreDataListeners) {
                c.onMoreData();
            }
        }
    }

    // override QueueIterator
    public void done(Throwable e)
    {
        super.done(e);
        onData();
    }

    // override QueueIterator
    public void put(Object o)
    {
        super.put(o);
        onData();
    }

    // override QueueIterator
    public boolean offer(Object o, long timeoutMillis)
    {
        if (super.offer(o, timeoutMillis)) {
            onData();
            return true;
        }
        return false;
    }

    // implement TupleIter
    public Object fetchNext()
    {
        try {
            if (defaultTimeout < Long.MAX_VALUE) {
                return next(defaultTimeout);
            } else {
                return next();
            }
        } catch (NoSuchElementException e) {
            return NoDataReason.END_OF_DATA;
        } catch (QueueIterator.TimeoutException e) {
            didUnderflow = true;
            if (timeoutAsUnderflow) {
                return NoDataReason.UNDERFLOW;
            } else {
                throw new TupleIter.TimeoutException();
            }
        }
    }

    /**
     * Called by generated code to add an input cursor's iterator so that it can
     * be restarted as needed.
     *
     * @param inputIter input cursor's iterator
     */
    protected void addRestartableInput(TupleIter inputIter)
    {
        restartableInputs.add(inputIter);
    }

    // implement ThreadIterator
    protected void doWork()
    {
        // Start a repository session in the event that the UDX accesses the
        // metadata repository -- the session is lightweight, so no problem
        // if repository txn is never started
        try {
            // sometimes sessions don't exist (don't ask why, if you don't know
            // you will always have a session)
            if (runtimeContext.getSession() != null) {
                runtimeContext.getSession().getRepos().beginReposSession();
            } else {
                runtimeContext.getRepos().beginReposSession();
            }
            try {
                executeUdx();
            } finally {
                if (runtimeContext.getSession() != null) {
                    runtimeContext.getSession().getRepos().endReposSession();
                } else {
                    runtimeContext.getRepos().endReposSession();
                }
            }
        } finally {
            latch.countDown();
        }
    }

    // NOTE:  called from generated code
    public PreparedStatement getResultInserter()
    {
        return resultInserter;
    }

    public FarragoSyntheticObject getCurrentRow()
    {
        return rowObjs[iRow];
    }

    // implement RestartableIterator
    public void restart()
    {
        stopWithLatch();

        reset(1);

        // Nullify thread.
        onEndOfQueue();

        // Toss anything it was producing.
        queue.clear();

        // Input cursors are currently "throwaway", but this is still
        // needed so that we correctly invoke a restart on Fennel streams.
        for (TupleIter inputIter : restartableInputs) {
            inputIter.restart();
        }
        restartableInputs.clear();

        // Restart a new thread.
        startWithLatch();
    }

    // implement TupleIter
    public void closeAllocation()
    {
        tracer.fine("close");
        stopWithLatch();
    }

    private void stopWithLatch()
    {
        if (latch == null) {
            // thread never ran
            return;
        }

        // Tell the running thread to buzz off.
        stopThread = true;
        try {
            // Wait for it to die.  (TODO:  If we ever get ThreadIterator
            // to stop using daemons, change this to use thread.join instead.)
            latch.await();
        } catch (InterruptedException ex) {
            throw Util.newInternal(ex);
        }
        stopThread = false;
    }

    private void startWithLatch()
    {
        latch = new CountDownLatch(1);
        start();
    }

    private void checkCancel()
    {
        runtimeContext.checkCancel();
        if (stopThread) {
            throw new RuntimeException("UDX thread stop requested");
        }
    }

    /**
     * Calls specific UDX to produce result set. Subclass implementation is
     * typically code-generated.
     */
    protected abstract void executeUdx();

    //~ Inner Classes ----------------------------------------------------------

    public class PreparedStatementInvocationHandler
        extends BarfingInvocationHandler
    {
        private final FarragoJdbcParamDef [] dynamicParamDefs;

        PreparedStatementInvocationHandler(RelDataType paramRowType)
        {
            RelDataTypeField [] fields = paramRowType.getFields();
            dynamicParamDefs = new FarragoJdbcParamDef[fields.length];
            for (int i = 0; i < fields.length; ++i) {
                FarragoParamFieldMetaData paramMetaData =
                    FarragoRuntimeJdbcUtil.newParamFieldMetaData(
                        fields[i].getType(),
                        ParameterMetaData.parameterModeIn);
                dynamicParamDefs[i] =
                    FarragoJdbcParamDefFactory.instance.newParamDef(
                        fields[i].getName(),
                        paramMetaData,
                        false);
            }
        }

        // implement PreparedStatement
        public int executeUpdate()
            throws SQLException
        {
            checkCancel();

            // on a full pipe, timeout every second to check cancellation; we
            // have to do it this way because the iterator above us
            // may not get sucked dry when the cursor is closed, in which
            // case we'll be stuck on the full pipe unless we can check
            // for cancellation
            while (!offer(
                    getCurrentRow(),
                    1000))
            {
                checkCancel();
            }
            ++iRow;
            if (iRow >= rowObjs.length) {
                iRow = 0;
            }
            return 1;
        }

        // implement PreparedStatement
        public ParameterMetaData getParameterMetaData()
        {
            return parameterMetaData;
        }

        // implement PreparedStatement
        public void clearParameters()
            throws SQLException
        {
            int n = getCurrentRow().getFields().length;
            for (int i = 0; i < n; ++i) {
                setDynamicParam(i + 1, null, null);
            }
        }

        private void setDynamicParam(
            int parameterIndex,
            Object obj,
            Calendar calendar)
            throws SQLException
        {
            int iField = parameterIndex - 1;

            // Result types are always nullable, so we should get something
            // which is both a NullableValue and an AssignableValue. However
            // SqlDateTimeWithoutTZ is not a NullableValue, for some reason.
            // Hack around this for the time being, as changing
            // SqlDateTimeWithoutTZ seems to cause unmarshalling problems.
            Object fieldObj = getCurrentRow().getFieldValue(iField);

            if (fieldObj instanceof NullableValue) {
                NullableValue nullableValue = (NullableValue) fieldObj;
                nullableValue.setNull(obj == null);
            } else if (fieldObj instanceof SqlDateTimeWithoutTZ) {
                SqlDateTimeWithoutTZ dt = (SqlDateTimeWithoutTZ) fieldObj;
                dt.setNull(obj == null); // its own public method!
            }

            if (obj != null) {
                AssignableValue assignableValue = (AssignableValue) fieldObj;

                // Note: Calendar is an optional argument so it wouldn't
                // make sense to pass in a null Calendar as a parameter
                Object scrubbedValue;
                if (calendar == null) {
                    scrubbedValue = dynamicParamDefs[iField].scrubValue(obj);
                } else {
                    scrubbedValue =
                        dynamicParamDefs[iField].scrubValue(obj, calendar);
                }
                assignableValue.assignFrom(scrubbedValue);
            }
        }

        // implement PreparedStatement
        public void setNull(
            int parameterIndex,
            int sqlType)
            throws SQLException
        {
            setDynamicParam(parameterIndex, null, null);
        }

        // implement PreparedStatement
        public void setBoolean(
            int parameterIndex,
            boolean x)
            throws SQLException
        {
            setDynamicParam(
                parameterIndex,
                Boolean.valueOf(x),
                null);
        }

        // implement PreparedStatement
        public void setByte(
            int parameterIndex,
            byte x)
            throws SQLException
        {
            setDynamicParam(
                parameterIndex,
                new Byte(x),
                null);
        }

        // implement PreparedStatement
        public void setShort(
            int parameterIndex,
            short x)
            throws SQLException
        {
            setDynamicParam(
                parameterIndex,
                new Short(x),
                null);
        }

        // implement PreparedStatement
        public void setInt(
            int parameterIndex,
            int x)
            throws SQLException
        {
            setDynamicParam(
                parameterIndex,
                new Integer(x),
                null);
        }

        // implement PreparedStatement
        public void setLong(
            int parameterIndex,
            long x)
            throws SQLException
        {
            setDynamicParam(
                parameterIndex,
                new Long(x),
                null);
        }

        // implement PreparedStatement
        public void setFloat(
            int parameterIndex,
            float x)
            throws SQLException
        {
            setDynamicParam(
                parameterIndex,
                new Float(x),
                null);
        }

        // implement PreparedStatement
        public void setDouble(
            int parameterIndex,
            double x)
            throws SQLException
        {
            setDynamicParam(
                parameterIndex,
                new Double(x),
                null);
        }

        // implement PreparedStatement
        public void setBigDecimal(
            int parameterIndex,
            BigDecimal x)
            throws SQLException
        {
            setDynamicParam(parameterIndex, x, null);
        }

        // implement PreparedStatement
        public void setString(
            int parameterIndex,
            String x)
            throws SQLException
        {
            setDynamicParam(parameterIndex, x, null);
        }

        // implement PreparedStatement
        public void setBytes(
            int parameterIndex,
            byte [] x)
            throws SQLException
        {
            setDynamicParam(parameterIndex, x, null);
        }

        // implement PreparedStatement
        public void setDate(
            int parameterIndex,
            java.sql.Date x)
            throws SQLException
        {
            setDynamicParam(parameterIndex, x, null);
        }

        // implement PreparedStatement
        public void setDate(
            int parameterIndex,
            java.sql.Date x,
            Calendar c)
            throws SQLException
        {
            setDynamicParam(parameterIndex, x, c);
        }

        // implement PreparedStatement
        public void setTime(
            int parameterIndex,
            Time x)
            throws SQLException
        {
            setDynamicParam(parameterIndex, x, null);
        }

        // implement PreparedStatement
        public void setTime(
            int parameterIndex,
            Time x,
            Calendar c)
            throws SQLException
        {
            setDynamicParam(parameterIndex, x, c);
        }

        // implement PreparedStatement
        public void setTimestamp(
            int parameterIndex,
            Timestamp x)
            throws SQLException
        {
            setDynamicParam(parameterIndex, x, null);
        }

        // implement PreparedStatement
        public void setTimestamp(
            int parameterIndex,
            Timestamp x,
            Calendar c)
            throws SQLException
        {
            setDynamicParam(parameterIndex, x, c);
        }

        // implement PreparedStatement
        public void setObject(
            int parameterIndex,
            Object x)
            throws SQLException
        {
            setDynamicParam(parameterIndex, x, null);
        }
    }
}

// End FarragoJavaUdxIterator.java
