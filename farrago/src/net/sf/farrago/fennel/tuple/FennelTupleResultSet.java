/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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

import java.io.*;

import java.math.*;

import java.nio.*;

import java.sql.*;

import java.util.Calendar;
import java.util.TimeZone;

import org.eigenbase.util14.*;


/**
 * FennelTupleResultSet provides an abstract java.sql.ResultSet based on tuples.
 * This object uses the Java Tuple Library to interpret tuple data as presented
 * in fennel tuple format and presents java objects and/or primitives as
 * requested by the application. TODO: FennelTupleResultSet minimizes object
 * creation while remapping tuple data to java objects in order to provide
 * higher performance. This class is JDK 1.4 compatible.
 *
 * @author angel
 * @version $Id$
 * @since Jan 8, 2006
 */
abstract public class FennelTupleResultSet
    extends AbstractResultSet
{
    //~ Static fields/initializers ---------------------------------------------

    public static final String ERRMSG_NO_TUPLE = "tuple not yet read";

    /**
     * The default timezone for this Java VM.
     */
    private static final TimeZone defaultZone =
        Calendar.getInstance().getTimeZone();

    //~ Instance fields --------------------------------------------------------

    protected ResultSetMetaData metaData = null;
    protected FennelTupleDescriptor desc = null;
    protected FennelTupleAccessor accessor = null;
    protected FennelTupleData data = null;
    protected boolean tupleComputed = false;
    protected final int tupleAlignment;
    protected final int tupleAlignmentMask;

    //~ Constructors -----------------------------------------------------------

    public FennelTupleResultSet(
        FennelTupleDescriptor desc,
        ResultSetMetaData metaData,
        int tupleAlignment)
    {
        this.desc = desc;
        this.metaData = metaData;
        this.tupleAlignment = tupleAlignment;
        this.tupleAlignmentMask = tupleAlignment - 1;
    }

    public FennelTupleResultSet(
        FennelTupleDescriptor desc,
        ResultSetMetaData metaData)
    {
        this(desc, metaData, FennelTupleAccessor.TUPLE_ALIGN_JVM);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * compute the tuple accessors
     */
    protected boolean computeTuple()
    {
        assert (desc != null) : "ResultSet FennelTupleDescriptor null";
        accessor = new FennelTupleAccessor(tupleAlignment);
        accessor.compute(desc, FennelTupleAccessor.TUPLE_FORMAT_NETWORK);
        data = new FennelTupleData(desc);
        assert (data.getDatumCount() == accessor.size()) : "ResultSet metadata mismatch";
        tupleComputed = true;
        return true;
    }

    /**
     * Adjusts ByteBuffer position according to the tuple alignment mask. Used
     * when "slicing" tuples from a multiple-tuple buffer.
     *
     * @param buf
     */
    protected void alignBufferPosition(ByteBuffer buf)
    {
        // TODO jvs 26-May-2007:  Unify with FennelTupleAccessor.
        int pos = buf.position();
        int pad = pos & tupleAlignmentMask;
        if (pad > 0) {
            buf.position(pos + pad);
        }
    }

    protected static long getMillis(
        FennelTupleDatum d,
        boolean shiftForTimeZone)
    {
        long millis = d.getLong();

        if (shiftForTimeZone) {
            // Shift time from GMT into local timezone
            long timeZoneOffset = defaultZone.getOffset(millis);
            return millis - timeZoneOffset;
        } else {
            return millis;
        }
    }

    protected static long getMillis(FennelTupleDatum d)
    {
        return getMillis(d, false);
    }

    /**
     * Returns the raw object representing this column
     *
     * @param columnIndex column ordinal
     *
     * @return raw object for a column
     *
     * @throws SQLException
     */
    protected Object getRaw(int columnIndex)
        throws SQLException
    {
        // prevent NPE if called before tuple read and accessor computed
        if (!tupleComputed || (data == null)) {
            throw new SQLException(ERRMSG_NO_TUPLE);
        }

        Object d = getRawColumnData(columnIndex, metaData, data);
        wasNull = (d == null);
        return d;
    }

    /**
     * @param columnIndex column ordinal
     * @param metaData metadata for all columns
     * @param tupleData tuple data representing a row of columns
     *
     * @return column data corresponding to a specified column ordinal; null if
     * the data is null
     *
     * @throws SQLException
     */
    public static Object getRawColumnData(
        int columnIndex,
        ResultSetMetaData metaData,
        FennelTupleData tupleData)
        throws SQLException
    {
        FennelTupleDatum d = tupleData.getDatum(columnIndex - 1);
        if (!d.isPresent()) {
            return null;
        }

        int columnType = metaData.getColumnType(columnIndex);
        switch (columnType) {
        case Types.TINYINT: // NOTE: the JDBC spec maps this to an Integer

            // For JDK 1.4 compatibility
            return new Byte(d.getByte());

        //return Byte.valueOf(d.getByte());
        case Types.SMALLINT: // NOTE: the JDBC spec maps this to an Integer

            // For JDK 1.4 compatibility
            return new Short(d.getShort());

        //return Short.valueOf(d.getShort());
        case Types.INTEGER:

            // For JDK 1.4 compatibility
            return new Integer(d.getInt());

        //return Integer.valueOf(d.getInt());
        case Types.BIGINT:

            // For JDK 1.4 compatibility
            return new Long(d.getLong());

        //return Long.valueOf(d.getLong());
        case Types.REAL:

            // For JDK 1.4 compatibility
            return new Float(d.getFloat());

        //return Float.valueOf(d.getFloat());
        case Types.FLOAT:
        case Types.DOUBLE:

            // For JDK 1.4 compatibility
            return new Double(d.getDouble());

        //return Double.valueOf(d.getDouble());
        case Types.DECIMAL:
        case Types.NUMERIC:
            BigDecimal bd = BigDecimal.valueOf(d.getLong());
            bd = bd.movePointLeft(metaData.getScale(columnIndex));
            return bd;
        case Types.BOOLEAN:
        case Types.BIT:
            return Boolean.valueOf(d.getBoolean());
        case Types.DATE:
            ZonelessDate zd = new ZonelessDate();
            zd.setZonelessTime(d.getLong());
            return zd;
        case Types.TIME:
            ZonelessTime zt = new ZonelessTime();
            zt.setZonelessTime(d.getLong());
            return zt;
        case Types.TIMESTAMP:
            ZonelessTimestamp zts = new ZonelessTimestamp();
            zts.setZonelessTime(d.getLong());
            return zts;
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
            String charsetName =
                d.isUnicode() ? ConversionUtil.NATIVE_UTF16_CHARSET_NAME
                : "ISO-8859-1";
            try {
                return new String(
                    d.getBytes(),
                    0,
                    d.getLength(),
                    charsetName);
            } catch (UnsupportedEncodingException ex) {
                // According to Charset javadoc, ISO-8859-1 and
                // UTF-16* should always be available.
                throw new AssertionError(
                    "Standard charset " + charsetName + " missing?");
            }
        case Types.BINARY:
        case Types.VARBINARY:
        case Types.LONGVARBINARY:
            byte [] ret = new byte[d.getLength()];
            System.arraycopy(
                d.getBytes(),
                0,
                ret,
                0,
                d.getLength());
            return ret;
        default:
            throw new UnsupportedOperationException(
                "Conversion not supported");
        }
    }

    /**
     * The number, types and properties of a ResultSet's columns are provided by
     * the getMetaData method.
     *
     * @return the description of a ResultSet's columns
     */
    public ResultSetMetaData getMetaData()
        throws SQLException
    {
        return metaData;
    }

    //======================================================================
    // TODO: Provide implementations for the following accessors that convert
    // directly from the tuple data without creating extra classes
    //======================================================================

    /*
    public String getString(int columnIndex) throws SQLException { return
     toString(getRaw(columnIndex)); }

     public byte[] getBytes(int columnIndex) throws SQLException { return (byte
     []) getRaw(columnIndex); }

     public boolean getBoolean(int columnIndex) throws SQLException { return
     toBoolean(getRaw(columnIndex)); }

     public byte getByte(int columnIndex) throws SQLException { return
     toByte(getRaw(columnIndex)); }

     public short getShort(int columnIndex) throws SQLException { return
     toShort(getRaw(columnIndex)); }

     public int getInt(int columnIndex) throws SQLException { return
     toInt(getRaw(columnIndex)); }

     public long getLong(int columnIndex) throws SQLException { return
     toLong(getRaw(columnIndex)); }

     public float getFloat(int columnIndex) throws SQLException { return
     toFloat(getRaw(columnIndex)); }

     public double getDouble(int columnIndex) throws SQLException { return
     toDouble(getRaw(columnIndex)); }

     public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
     return toBigDecimal(getRaw(columnIndex)); }

     public java.sql.Date getDate(int columnIndex) throws SQLException { return
     toDate(getRaw(columnIndex)); }

     public java.sql.Time getTime(int columnIndex) throws SQLException { return
     toTime(getRaw(columnIndex)); }

     public java.sql.Timestamp getTimestamp(int columnIndex) throws SQLException
     { return toTimestamp(getRaw(columnIndex)); }
     */

}

// End FennelTupleResultSet.java
