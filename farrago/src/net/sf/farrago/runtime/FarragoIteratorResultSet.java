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

package net.sf.farrago.runtime;

import net.sf.farrago.util.*;
import net.sf.farrago.trace.*;
import net.sf.farrago.type.*;
import net.sf.farrago.type.runtime.*;

import net.sf.saffron.core.*;
import net.sf.saffron.runtime.*;

import java.sql.*;

import java.util.*;
import java.util.logging.*;

/**
 * FarragoIteratorResultSet is a refinement of Saffron's IteratorResultSet
 * which exposes Farrago datatype semantics.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoIteratorResultSet extends IteratorResultSet
{
    private static final Logger tracer =
        FarragoTrace.getFarragoIteratorResultSetTracer();

    private static final Logger jdbcTracer =
        FarragoTrace.getFarragoJdbcEngineDriverTracer();

    //~ Instance fields -------------------------------------------------------

    private FarragoAllocation allocation;
    private SaffronType rowType;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FarragoIteratorResultSet object.
     *
     * @param iterator underlying iterator
     * @param clazz Class for objects which iterator will produce
     * @param rowType type info for rows produced
     * @param allocation object to close when this ResultSet is closed
     */
    public FarragoIteratorResultSet(
        Iterator iterator,
        Class clazz,
        SaffronType rowType,
        FarragoAllocation allocation)
    {
        super(iterator,new SyntheticColumnGetter(clazz));
        this.rowType = rowType;
        this.allocation = allocation;
        if (tracer.isLoggable(Level.FINE)) {
            tracer.fine(toString());
        }
    }

    //~ Methods ---------------------------------------------------------------

    // implement ResultSet
    public boolean next() throws SQLException
    {
        try {
            if (tracer.isLoggable(Level.FINE)) {
                tracer.fine(toString());
            }
            return super.next();
        } catch (Throwable ex) {
            // trace exceptions as part of JDBC API
            throw FarragoUtil.newSqlException(ex,jdbcTracer);
        }
    }
    
    // implement ResultSet
    public ResultSetMetaData getMetaData() throws SQLException
    {
        return new FarragoResultSetMetaData(rowType);
    }

    // implement ResultSet
    public void close() throws SQLException
    {
        if (tracer.isLoggable(Level.FINE)) {
            tracer.fine(toString());
        }
        if (allocation != null) {
            // NOTE:  this may be called reentrantly for daemon stmts,
            // so need special handling
            FarragoAllocation allocationToClose = allocation;
            allocation = null;
            allocationToClose.closeAllocation();
        }
        super.close();
    }

    // implement IteratorResultSet
    protected Object getRaw(int columnIndex)
    {
        Object obj = super.getRaw(columnIndex);
        if (obj instanceof NullableValue) {
            NullableValue nullableValue = (NullableValue) obj;
            obj = nullableValue.getNullableData();
            wasNull = (obj == null);
        } else if (obj instanceof BytePointer) {
            BytePointer bytePointer = (BytePointer) obj;
            obj = bytePointer.getNullableData();
            wasNull = false;
        } else {
            wasNull = false;
        }
        return obj;
    }
}


// End FarragoIteratorResultSet.java
