/*
// $Id$
// Farrago is an extensible data management system.
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
package net.sf.farrago.runtime;

import net.sf.farrago.type.*;
import net.sf.farrago.type.runtime.*;
import net.sf.farrago.session.*;

import org.eigenbase.util.*;
import org.eigenbase.runtime.*;
import org.eigenbase.reltype.*;

import java.math.*;
import java.util.*;
import java.util.concurrent.*;
import java.sql.*;
import java.lang.reflect.*;

/**
 * FarragoJavaUdxIterator provides runtime support for a call to
 * a Java UDX.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FarragoJavaUdxIterator
    extends ThreadIterator
    implements RestartableIterator
{
    private static final int QUEUE_ARRAY_SIZE = 100;
    
    private final FarragoSyntheticObject [] rowObjs;

    private final PreparedStatement resultInserter;

    private final FarragoSessionRuntimeContext runtimeContext;

    private int iRow;

    private boolean restart;

    private CountDownLatch latch;

    private final ParameterMetaData parameterMetaData;

    protected FarragoJavaUdxIterator(
        FarragoSessionRuntimeContext runtimeContext,
        Class rowClass,
        RelDataType rowType)
    {
        super(new ArrayBlockingQueue(QUEUE_ARRAY_SIZE));
        this.runtimeContext = runtimeContext;

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
        resultInserter = (PreparedStatement) Proxy.newProxyInstance(
            null,
            new Class [] { PreparedStatement.class },
            new PreparedStatementInvocationHandler());
    }

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

    // implement ThreadIterator
    protected void doWork()
    {
        try {
            executeUdx();
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
        // Tell the running thread to buzz off.
        if (latch != null) {
            restart = true;

            // Wait for it to die.  (TODO:  If we ever get ThreadIterator
            // to stop using daemons, change this to use thread.join instead.)
            try {
                latch.await();
            } catch (InterruptedException ex) {
                throw Util.newInternal(ex);
            }
            restart = false;
        }

        reset(1);

        // Nullify thread.
        onEndOfQueue();

        // Toss anything it was producing.
        queue.clear();

        // Restart a new thread.
        startWithLatch();
    }

    private void startWithLatch()
    {
        latch = new CountDownLatch(1);
        start();
    }

    private void checkCancel()
    {
        runtimeContext.checkCancel();
        if (restart) {
            throw new RuntimeException("UDX thread restart");
        }
    }
    
    /**
     * Calls specific UDX to produce result set.  Subclass implementation
     * is typically code-generated.
     */
    protected abstract void executeUdx();

    public class PreparedStatementInvocationHandler
        extends BarfingInvocationHandler
    {
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
            while (!offer(getCurrentRow(), 1000)) {
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
                setDynamicParam(i + 1, null);
            }
        }
        
        private void setDynamicParam(
            int parameterIndex,
            Object obj)
            throws SQLException
        {
            int iField = parameterIndex - 1;
            // result types are always nullable, so we're guaranteed
            // to get something which implements both NullableValue
            // and AssignableValue
            Object fieldObj = getCurrentRow().getFieldValue(iField);
            NullableValue nullableValue = (NullableValue) fieldObj;
            if (obj == null) {
                nullableValue.setNull(true);
            } else {
                nullableValue.setNull(false);
                AssignableValue assignableValue = (AssignableValue) fieldObj;
                assignableValue.assignFrom(obj);
            }
        }
        
        // implement PreparedStatement
        public void setNull(
            int parameterIndex,
            int sqlType)
            throws SQLException
        {
            setDynamicParam(parameterIndex, null);
        }

        // implement PreparedStatement
        public void setBoolean(
            int parameterIndex,
            boolean x)
            throws SQLException
        {
            setDynamicParam(
                parameterIndex,
                Boolean.valueOf(x));
        }

        // implement PreparedStatement
        public void setByte(
            int parameterIndex,
            byte x)
            throws SQLException
        {
            setDynamicParam(
                parameterIndex,
                new Byte(x));
        }

        // implement PreparedStatement
        public void setShort(
            int parameterIndex,
            short x)
            throws SQLException
        {
            setDynamicParam(
                parameterIndex,
                new Short(x));
        }

        // implement PreparedStatement
        public void setInt(
            int parameterIndex,
            int x)
            throws SQLException
        {
            setDynamicParam(
                parameterIndex,
                new Integer(x));
        }

        // implement PreparedStatement
        public void setLong(
            int parameterIndex,
            long x)
            throws SQLException
        {
            setDynamicParam(
                parameterIndex,
                new Long(x));
        }

        // implement PreparedStatement
        public void setFloat(
            int parameterIndex,
            float x)
            throws SQLException
        {
            setDynamicParam(
                parameterIndex,
                new Float(x));
        }

        // implement PreparedStatement
        public void setDouble(
            int parameterIndex,
            double x)
            throws SQLException
        {
            setDynamicParam(
                parameterIndex,
                new Double(x));
        }

        // implement PreparedStatement
        public void setBigDecimal(
            int parameterIndex,
            BigDecimal x)
            throws SQLException
        {
            setDynamicParam(parameterIndex, x);
        }

        // implement PreparedStatement
        public void setString(
            int parameterIndex,
            String x)
            throws SQLException
        {
            setDynamicParam(parameterIndex, x);
        }

        // implement PreparedStatement
        public void setBytes(
            int parameterIndex,
            byte [] x)
            throws SQLException
        {
            setDynamicParam(parameterIndex, x);
        }

        // implement PreparedStatement
        public void setDate(
            int parameterIndex,
            java.sql.Date x)
            throws SQLException
        {
            setDynamicParam(parameterIndex, x);
        }

        // implement PreparedStatement
        public void setTime(
            int parameterIndex,
            Time x)
            throws SQLException
        {
            setDynamicParam(parameterIndex, x);
        }

        // implement PreparedStatement
        public void setTimestamp(
            int parameterIndex,
            Timestamp x)
            throws SQLException
        {
            setDynamicParam(parameterIndex, x);
        }

        // implement PreparedStatement
        public void setObject(
            int parameterIndex,
            Object x)
            throws SQLException
        {
            setDynamicParam(parameterIndex, x);
        }
    }
}

// End FarragoJavaUdxIterator.java
