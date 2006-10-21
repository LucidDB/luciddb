/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2006 The Eigenbase Project
// Copyright (C) 2005-2006 Disruptive Tech
// Copyright (C) 2005-2006 LucidEra, Inc.
// Portions Copyright (C) 2003-2006 John V. Sichi
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

import java.sql.*;

import java.util.logging.*;

import net.sf.farrago.jdbc.*;
import net.sf.farrago.session.*;
import net.sf.farrago.trace.*;
import net.sf.farrago.type.*;
import net.sf.farrago.type.runtime.*;
import net.sf.farrago.util.*;

import org.eigenbase.reltype.*;
import org.eigenbase.runtime.*;


/**
 * FarragoTupleIterResultSet is a refinement of TupleIterResultSet which exposes
 * Farrago datatype semantics.
 *
 * @author John V. Sichi, Stephan Zuercher
 * @version $Id$
 */
public class FarragoTupleIterResultSet
    extends TupleIterResultSet
{

    //~ Static fields/initializers ---------------------------------------------

    protected static final Logger tracer =
        FarragoTrace.getFarragoTupleIterResultSetTracer();
    private static final Logger jdbcTracer =
        FarragoTrace.getFarragoJdbcEngineDriverTracer();

    //~ Instance fields --------------------------------------------------------

    private FarragoSessionRuntimeContext runtimeContext;
    private RelDataType rowType;

    //~ Constructors -----------------------------------------------------------

    public FarragoTupleIterResultSet(
        TupleIter tupleIter,
        Class clazz,
        RelDataType rowType,
        FarragoSessionRuntimeContext runtimeContext)
    {
        this(
            tupleIter,
            clazz,
            rowType,
            runtimeContext,
            new SyntheticColumnGetter(clazz));
    }
    
    /**
     * Creates a new FarragoTupleIterResultSet object.
     *
     * @param tupleIter underlying iterator
     * @param clazz Class for objects which iterator will produce
     * @param rowType type info for rows produced
     * @param runtimeContext runtime context for this execution
     * @param columnGetter object used to read individual columns from the the
     * underlying iterator
     */
    public FarragoTupleIterResultSet(
        TupleIter tupleIter,
        Class clazz,
        RelDataType rowType,
        FarragoSessionRuntimeContext runtimeContext,
        ColumnGetter columnGetter)
    {
        super(tupleIter, columnGetter);
        this.rowType = rowType;
        this.runtimeContext = runtimeContext;
        if (tracer.isLoggable(Level.FINE)) {
            tracer.fine(toString());
        }
    }

    //~ Methods ----------------------------------------------------------------

    // implement ResultSet
    public boolean next()
        throws SQLException
    {
        try {
            if (tracer.isLoggable(Level.FINE)) {
                tracer.fine(toString());
            }
            if (runtimeContext != null) {
                runtimeContext.checkCancel();
            }
            boolean rc = super.next();
            if (!rc) {
                if (runtimeContext != null) {
                    FarragoSession session = runtimeContext.getSession();
                    if (session.isAutoCommit()) {
                        // According to the Javadoc for
                        // Connection.setAutoCommit, returning the last
                        // row of a cursor in autocommit mode ends
                        // the transaction.
                        close();
                    }
                }
            }
            return rc;
        } catch (Throwable ex) {
            // trace exceptions as part of JDBC API
            throw FarragoJdbcUtil.newSqlException(ex, jdbcTracer);
        }
    }

    // implement ResultSet
    public ResultSetMetaData getMetaData()
        throws SQLException
    {
        return new FarragoResultSetMetaData(rowType);
    }

    // implement ResultSet
    public void close()
        throws SQLException
    {
        if (tracer.isLoggable(Level.FINE)) {
            tracer.fine(toString());
        }
        if (runtimeContext != null) {
            // NOTE:  this may be called reentrantly for daemon stmts,
            // so need special handling
            FarragoAllocation allocationToClose = runtimeContext;
            runtimeContext = null;
            allocationToClose.closeAllocation();
        }
        super.close();
    }

    // implement AbstractResultSet
    protected Object getRaw(int columnIndex)
    {
        Object obj = super.getRaw(columnIndex);
        if (obj instanceof SpecialDataValue) {
            SpecialDataValue specialValue = (SpecialDataValue) obj;
            obj = specialValue.getSpecialData();
            wasNull = (obj == null);
        } else if (obj instanceof DataValue) {
            DataValue nullableValue = (DataValue) obj;
            obj = nullableValue.getNullableData();
            wasNull = (obj == null);
        } else {
            wasNull = false;
        }
        return obj;
    }
}

// End FarragoTupleIterResultSet.java
