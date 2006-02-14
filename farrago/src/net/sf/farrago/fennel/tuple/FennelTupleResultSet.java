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

package net.sf.farrago.fennel.tuple;

import org.eigenbase.util14.AbstractResultSet;

import java.nio.ByteBuffer;
import java.sql.*;
import java.math.BigDecimal;

/**
 * FennelTupleResultSet provides an abstract java.sql.ResultSet based on tuples.
 *
 * This object uses the Java Tuple Library to interpret tuple data
 * as presented in fennel tuple format and presents java objects
 * and/or primitives as requested by the application.
 *
 * TODO: FennelTupleesultSet minimizes object creation while remapping
 * tuple data to java objects in order to provide higher performance.
 *
 * @author angel
 * @version $Id$
 * @since Jan 8, 2006
 */
abstract public class FennelTupleResultSet extends AbstractResultSet {
    // instance variables
    protected ResultSetMetaData metaData = null;
    protected FennelTupleDescriptor desc = null;
    protected FennelTupleAccessor accessor = null;
    protected FennelTupleData     data = null;
    protected boolean  tupleComputed = false;
    protected final int tupleAlignment = FennelTupleAccessor.TUPLE_ALIGN4;
    protected final int tupleAlignmentMask = tupleAlignment -1;

    public FennelTupleResultSet(FennelTupleDescriptor desc,
                                ResultSetMetaData metaData)
    {
        this.desc = desc;
        this.metaData = metaData;
    }

    /**
     * compute the tuple accessors
     */
    protected boolean computeTuple()
    {
        assert(desc != null)
            :"ResultSet FennelTupleDescriptor null";
        accessor = new FennelTupleAccessor(tupleAlignment);
        accessor.compute(desc, FennelTupleAccessor.TUPLE_FORMAT_NETWORK);
        data = new FennelTupleData(desc);
        assert(data.getDatumCount() == accessor.size())
            :"ResultSet metadata mismatch";
        tupleComputed = true;
        return true;
    }

    /**
     * Adjusts ByteBuffer position according to the tuple alignment
     * mask. Used when "slicing" tuples from a multiple-tuple buffer.
     * @param buf
     */
    protected void alignBufferPosition(ByteBuffer buf)
    {
        int pos = buf.position();
        int pad = pos & tupleAlignmentMask;
        if (pad > 0) {
            buf.position(pos +pad);
        }
    }

    /**
     * Returns the raw object representing this column
     * @param columnIndex
     * @return
     * @throws SQLException
     */
    protected Object getRaw(int columnIndex) throws SQLException
    {
        int columnType = metaData.getColumnType(columnIndex);
        FennelTupleDatum d = data.getDatum(columnIndex-1);
        if (!d.isPresent()) {
            wasNull = true;
            return null;
        }

        wasNull = false;
        switch (columnType) {
            case Types.TINYINT:     // NOTE: the JDBC spec maps this to an Integer
                return new Byte(d.getByte());
            case Types.SMALLINT:    // NOTE: the JDBC spec maps this to an Integer
                return new Short(d.getShort());
            case Types.INTEGER:
                return new Integer(d.getInt());
            case Types.BIGINT:
                return new Long(d.getLong());
            case Types.REAL:
                return new Float(d.getFloat());
            case Types.DOUBLE:
                return new Double(d.getDouble());
            case Types.DECIMAL:
            case Types.NUMERIC:
                BigDecimal bd = BigDecimal.valueOf(d.getLong());
                bd = bd.movePointLeft(metaData.getScale(columnIndex));
                return bd;
            case Types.BOOLEAN:
            case Types.BIT:
                return new Boolean(d.getBoolean());
            case Types.DATE:
                return new Date(d.getLong());
            case Types.TIME:
                return new Time(d.getLong());
            case Types.TIMESTAMP:
                return new Timestamp(d.getLong());
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return new String(d.getBytes(), 0, d.getLength());
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                byte[] ret = new byte[d.getLength()];
                System.arraycopy(d.getBytes(), 0, ret, 0, d.getLength());
                return ret;
            default:
                throw new UnsupportedOperationException("Operation not supported right now");
        }
    }

    /**
     * The number, types and properties of a ResultSet's columns
     * are provided by the getMetaData method.
     *
     * @return the description of a ResultSet's columns
     */
    public ResultSetMetaData getMetaData() throws SQLException
    {
        return metaData;
    }

    //======================================================================
    // TODO: Provide implementations for the following accessors that convert
    // directly from the tuple data without creating extra classes
    //======================================================================

    /*
    public String getString(int columnIndex) throws SQLException
    {
        return toString(getRaw(columnIndex));
    }

    public byte[] getBytes(int columnIndex) throws SQLException
    {
        return (byte []) getRaw(columnIndex);
    }

    public boolean getBoolean(int columnIndex) throws SQLException
    {
        return toBoolean(getRaw(columnIndex));
    }

    public byte getByte(int columnIndex) throws SQLException
    {
        return toByte(getRaw(columnIndex));
    }

    public short getShort(int columnIndex) throws SQLException
    {
        return toShort(getRaw(columnIndex));
    }

    public int getInt(int columnIndex) throws SQLException
    {
        return toInt(getRaw(columnIndex));
    }

    public long getLong(int columnIndex) throws SQLException
    {
        return toLong(getRaw(columnIndex));
    }

    public float getFloat(int columnIndex) throws SQLException
    {
        return toFloat(getRaw(columnIndex));
    }

    public double getDouble(int columnIndex) throws SQLException
    {
        return toDouble(getRaw(columnIndex));
    }

    public BigDecimal getBigDecimal(int columnIndex) throws SQLException
    {
        return toBigDecimal(getRaw(columnIndex));
    }

    public java.sql.Date getDate(int columnIndex) throws SQLException
    {
        return toDate(getRaw(columnIndex));
    }

    public java.sql.Time getTime(int columnIndex) throws SQLException
    {
        return toTime(getRaw(columnIndex));
    }

    public java.sql.Timestamp getTimestamp(int columnIndex) throws SQLException
    {
        return toTimestamp(getRaw(columnIndex));
    }
     */

}

// End FennelTupleResultSet.java