/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
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

package org.eigenbase.runtime;

import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.util.*;

import org.eigenbase.util.Util;


/**
 * A <code>IteratorResultSet</code> is an adapter which converts a {@link
 * java.util.Iterator} into a {@link java.sql.ResultSet}.
 *
 * <p>
 * See also its converse adapter, {@link ResultSetIterator}
 * </p>
 */
public class IteratorResultSet implements ResultSet
{
    //~ Instance fields -------------------------------------------------------

    private final ColumnGetter columnGetter;
    private final Iterator iterator;
    private Object current;
    private int row; // 1-based (starts on 0 to represent before first row)
    private TimeoutQueueIterator timeoutIter;
    protected boolean wasNull;
    private long timeoutMillis;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a result set based upon an iterator. The column-getter accesses
     * columns based upon their ordinal.
     *
     * @pre iterator != null
     */
    public IteratorResultSet(
        Iterator iterator,
        ColumnGetter columnGetter)
    {
        Util.pre(iterator != null, "iterator != null");
        this.iterator = iterator;
        String [] columnNames = columnGetter.getColumnNames();
        this.columnGetter = columnGetter;
    }

    //~ Methods ---------------------------------------------------------------

    private String [] getColumnNames()
    {
        String [] columnNames = columnGetter.getColumnNames();
        if (columnNames == null) {
            return Util.emptyStringArray;
        } else {
            return columnNames;
        }
    }
    
    /**
     * Sets the timeout that this IteratorResultSet will wait for a row from
     * the underlying iterator.
     *
     * @param timeoutMillis Timeout in milliseconds. Must be greater than zero.
     * @pre timeoutMillis > 0
     * @pre this.timeoutMillis == 0
     */
    public void setTimeout(long timeoutMillis)
    {
        Util.pre(timeoutMillis > 0, "timeoutMillis > 0");
        Util.pre(this.timeoutMillis == 0, "this.timeoutMillis == 0");
        assert timeoutIter == null;

        // we create a new semaphore for each executeQuery call
        // and then pass ownership to the result set returned
        // the query timeout used is the last set via JDBC.
        this.timeoutMillis = timeoutMillis;
        timeoutIter = new TimeoutQueueIterator(iterator);
        timeoutIter.start();
    }

    public boolean isAfterLast()
        throws SQLException
    {
        // TODO jvs 25-June-2005:  make this return true after
        // next() returns false
        return false;
    }

    public Array getArray(int i)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public Array getArray(String colName)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public InputStream getAsciiStream(int columnIndex)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public InputStream getAsciiStream(String columnName)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public boolean isBeforeFirst()
        throws SQLException
    {
        // REVIEW jvs 25-June-2005:  make this return false if there are
        // no rows?
        return row == 0;
    }

    public BigDecimal getBigDecimal(
        int columnIndex,
        int scale)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public BigDecimal getBigDecimal(
        String columnName,
        int scale)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public BigDecimal getBigDecimal(int columnIndex)
        throws SQLException
    {
        return BigDecimal.valueOf(toLong(getRaw(columnIndex)));
    }

    public BigDecimal getBigDecimal(String columnName)
        throws SQLException
    {
        return BigDecimal.valueOf(toLong(getRaw(columnName)));
    }

    public InputStream getBinaryStream(int columnIndex)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public InputStream getBinaryStream(String columnName)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public Blob getBlob(int i)
        throws SQLException
    {
        return null;
    }

    public Blob getBlob(String colName)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public boolean getBoolean(int columnIndex)
        throws SQLException
    {
        return toBoolean(getRaw(columnIndex));
    }

    public boolean getBoolean(String columnName)
        throws SQLException
    {
        return toBoolean(getRaw(columnName));
    }

    public byte getByte(int columnIndex)
        throws SQLException
    {
        return toByte(getRaw(columnIndex));
    }

    public byte getByte(String columnName)
        throws SQLException
    {
        return toByte(getRaw(columnName));
    }

    public byte [] getBytes(int columnIndex)
        throws SQLException
    {
        return (byte []) getRaw(columnIndex);
    }

    public byte [] getBytes(String columnName)
        throws SQLException
    {
        return (byte []) getRaw(columnName);
    }

    public Reader getCharacterStream(int columnIndex)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public Reader getCharacterStream(String columnName)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public Clob getClob(int i)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public Clob getClob(String colName)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public int getConcurrency()
        throws SQLException
    {
        return CONCUR_READ_ONLY;
    }

    public String getCursorName()
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public Date getDate(int columnIndex)
        throws SQLException
    {
        return toDate(getRaw(columnIndex));
    }

    public Date getDate(String columnName)
        throws SQLException
    {
        return toDate(getRaw(columnName));
    }

    public Date getDate(
        int columnIndex,
        Calendar cal)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public Date getDate(
        String columnName,
        Calendar cal)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public double getDouble(int columnIndex)
        throws SQLException
    {
        return toDouble(getRaw(columnIndex));
    }

    public double getDouble(String columnName)
        throws SQLException
    {
        return toDouble(getRaw(columnName));
    }

    public void setFetchDirection(int direction)
        throws SQLException
    {
        if (direction != FETCH_FORWARD) {
            throw newDirectionError();
        }
    }

    public int getFetchDirection()
        throws SQLException
    {
        return FETCH_FORWARD;
    }

    public void setFetchSize(int rows)
        throws SQLException
    {
    }

    public int getFetchSize()
        throws SQLException
    {
        return 0;
    }

    public boolean isFirst()
        throws SQLException
    {
        return row == 1;
    }

    public float getFloat(int columnIndex)
        throws SQLException
    {
        return toFloat(getRaw(columnIndex));
    }

    public float getFloat(String columnName)
        throws SQLException
    {
        return toFloat(getRaw(columnName));
    }

    public int getInt(int columnIndex)
        throws SQLException
    {
        return toInt(getRaw(columnIndex));
    }

    public int getInt(String columnName)
        throws SQLException
    {
        return toInt(getRaw(columnName));
    }

    public boolean isLast()
        throws SQLException
    {
        return false;
    }

    public long getLong(int columnIndex)
        throws SQLException
    {
        return toLong(getRaw(columnIndex));
    }

    public long getLong(String columnName)
        throws SQLException
    {
        return toLong(getRaw(columnName));
    }

    public ResultSetMetaData getMetaData()
        throws SQLException
    {
        return new MetaData();
    }

    public Object getObject(int columnIndex)
        throws SQLException
    {
        Object o = getRaw(columnIndex);
        if (o == null) {
            wasNull = true;
        } else {
            wasNull = false;
        }
        return o;
    }

    public Object getObject(String columnName)
        throws SQLException
    {
        return getObject(findColumn(columnName));
    }

    public Object getObject(
        int i,
        Map map)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public Object getObject(
        String colName,
        Map map)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public Ref getRef(int i)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public Ref getRef(String colName)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public int getRow()
        throws SQLException
    {
        return row;
    }

    public short getShort(int columnIndex)
        throws SQLException
    {
        return toShort(getRaw(columnIndex));
    }

    public short getShort(String columnName)
        throws SQLException
    {
        return toShort(getRaw(columnName));
    }

    public Statement getStatement()
        throws SQLException
    {
        return null;
    }

    public String getString(int columnIndex)
        throws SQLException
    {
        return toString(getRaw(columnIndex));
    }

    public String getString(String columnName)
        throws SQLException
    {
        return toString(getRaw(columnName));
    }

    public Time getTime(int columnIndex)
        throws SQLException
    {
        return toTime(getRaw(columnIndex));
    }

    public Time getTime(String columnName)
        throws SQLException
    {
        return toTime(getRaw(columnName));
    }

    public Time getTime(
        int columnIndex,
        Calendar cal)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public Time getTime(
        String columnName,
        Calendar cal)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public Timestamp getTimestamp(int columnIndex)
        throws SQLException
    {
        return toTimestamp(getRaw(columnIndex));
    }

    public Timestamp getTimestamp(String columnName)
        throws SQLException
    {
        return toTimestamp(getRaw(columnName));
    }

    public Timestamp getTimestamp(
        int columnIndex,
        Calendar cal)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public Timestamp getTimestamp(
        String columnName,
        Calendar cal)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public int getType()
        throws SQLException
    {
        return TYPE_FORWARD_ONLY;
    }

    public URL getURL(int columnIndex)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public URL getURL(String columnName)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public InputStream getUnicodeStream(int columnIndex)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public InputStream getUnicodeStream(String columnName)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public SQLWarning getWarnings()
        throws SQLException
    {
        return null;
    }

    public boolean absolute(int row)
        throws SQLException
    {
        if ((row < 1) || (getType() == TYPE_FORWARD_ONLY)) {
            throw newDirectionError();
        }
        
        return relative(row - getRow());
    }

    public void afterLast()
        throws SQLException
    {
        throw newDirectionError();
    }

    public void beforeFirst()
        throws SQLException
    {
        throw newDirectionError();
    }

    public void cancelRowUpdates()
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public void clearWarnings()
        throws SQLException
    {
    }

    public void close()
        throws SQLException
    {
        if (timeoutIter != null) {
            final long noTimeout = 0;
            timeoutIter.close(noTimeout);
            timeoutIter = null;
        }
    }

    public void deleteRow()
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public int findColumn(String columnName)
        throws SQLException
    {
        ResultSetMetaData metaData = getMetaData();
        int n = metaData.getColumnCount();
        for (int i = 1; i <= n; i++) {
            if (columnName.equals(metaData.getColumnName(i))) {
                return i;
            }
        }
        throw new SQLException("column '" + columnName + "' not found");
    }

    public boolean first()
        throws SQLException
    {
        throw newDirectionError();
    }

    public void insertRow()
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public boolean last()
        throws SQLException
    {
        throw newDirectionError();
    }

    public void moveToCurrentRow()
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public void moveToInsertRow()
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    // ------------------------------------------------------------------------
    // the remaining methods implement ResultSet
    public boolean next()
        throws SQLException
    {
        if (timeoutIter != null) {
            try {
                long endTime = System.currentTimeMillis() + timeoutMillis;
                if (timeoutIter.hasNext(timeoutMillis)) {
                    long remainingTimeout =
                        endTime - System.currentTimeMillis();
                    if (remainingTimeout <= 0) {
                        // The call to hasNext() took longer than we
                        // expected -- we're out of time.
                        throw new SqlTimeoutException();
                    }
                    this.current = timeoutIter.next(remainingTimeout);
                    this.row++;
                    return true;
                } else {
                    return false;
                }
            } catch (QueueIterator.TimeoutException e) {
                throw new SqlTimeoutException();
            } catch (Throwable e) {
                throw newFetchError(e);
            }
        } else {
            try {
                if (iterator.hasNext()) {
                    this.current = iterator.next();
                    this.row++;
                    return true;
                } else {
                    return false;
                }
            } catch (Throwable e) {
                throw newFetchError(e);
            }
        }
    }

    public boolean previous()
        throws SQLException
    {
        throw newDirectionError();
    }

    public void refreshRow()
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public boolean relative(int rows)
        throws SQLException
    {
        if ((rows < 0) || (getType() == TYPE_FORWARD_ONLY)) {
            throw newDirectionError();
        }
        while (rows-- > 0) {
            if (!next()) {
                return false;
            }
        }
        return true;
    }

    public boolean rowDeleted()
        throws SQLException
    {
        return false;
    }

    public boolean rowInserted()
        throws SQLException
    {
        return false;
    }

    public boolean rowUpdated()
        throws SQLException
    {
        return false;
    }

    public void updateArray(
        int columnIndex,
        Array x)
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public void updateArray(
        String columnName,
        Array x)
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public void updateAsciiStream(
        int columnIndex,
        InputStream x,
        int length)
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public void updateAsciiStream(
        String columnName,
        InputStream x,
        int length)
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public void updateBigDecimal(
        int columnIndex,
        BigDecimal x)
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public void updateBigDecimal(
        String columnName,
        BigDecimal x)
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public void updateBinaryStream(
        int columnIndex,
        InputStream x,
        int length)
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public void updateBinaryStream(
        String columnName,
        InputStream x,
        int length)
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public void updateBlob(
        int columnIndex,
        Blob x)
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public void updateBlob(
        String columnName,
        Blob x)
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public void updateBoolean(
        int columnIndex,
        boolean x)
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public void updateBoolean(
        String columnName,
        boolean x)
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public void updateByte(
        int columnIndex,
        byte x)
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public void updateByte(
        String columnName,
        byte x)
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public void updateBytes(
        int columnIndex,
        byte [] x)
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public void updateBytes(
        String columnName,
        byte [] x)
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public void updateCharacterStream(
        int columnIndex,
        Reader x,
        int length)
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public void updateCharacterStream(
        String columnName,
        Reader reader,
        int length)
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public void updateClob(
        int columnIndex,
        Clob x)
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public void updateClob(
        String columnName,
        Clob x)
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public void updateDate(
        int columnIndex,
        Date x)
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public void updateDate(
        String columnName,
        Date x)
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public void updateDouble(
        int columnIndex,
        double x)
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public void updateDouble(
        String columnName,
        double x)
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public void updateFloat(
        int columnIndex,
        float x)
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public void updateFloat(
        String columnName,
        float x)
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public void updateInt(
        int columnIndex,
        int x)
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public void updateInt(
        String columnName,
        int x)
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public void updateLong(
        int columnIndex,
        long x)
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public void updateLong(
        String columnName,
        long x)
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public void updateNull(int columnIndex)
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public void updateNull(String columnName)
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public void updateObject(
        int columnIndex,
        Object x,
        int scale)
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public void updateObject(
        int columnIndex,
        Object x)
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public void updateObject(
        String columnName,
        Object x,
        int scale)
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public void updateObject(
        String columnName,
        Object x)
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public void updateRef(
        int columnIndex,
        Ref x)
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public void updateRef(
        String columnName,
        Ref x)
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public void updateRow()
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public void updateShort(
        int columnIndex,
        short x)
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public void updateShort(
        String columnName,
        short x)
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public void updateString(
        int columnIndex,
        String x)
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public void updateString(
        String columnName,
        String x)
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public void updateTime(
        int columnIndex,
        Time x)
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public void updateTime(
        String columnName,
        Time x)
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public void updateTimestamp(
        int columnIndex,
        Timestamp x)
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public void updateTimestamp(
        String columnName,
        Timestamp x)
        throws SQLException
    {
        throw newUpdatabilityError();
    }

    public boolean wasNull()
        throws SQLException
    {
        return wasNull;
    }

    /**
     * Returns the raw value of a column as an object.
     */
    protected Object getRaw(int columnIndex)
    {
        return columnGetter.get(current, columnIndex);
    }

    protected Object getRaw(String columnName)
        throws SQLException
    {
        return getRaw(findColumn(columnName));
    }

    private SQLException newConversionError(
        Object o,
        Class clazz)
    {
        return new SQLException("cannot convert " + o.getClass() + "(" + o
            + ") to " + clazz);
    }

    private SQLException newDirectionError()
    {
        return new SQLException("ResultSet is TYPE_FORWARD_ONLY");
    }

    private SQLException newUpdatabilityError()
    {
        return new SQLException("ResultSet is CONCUR_READ_ONLY");
    }

    private SQLException newFetchError(Throwable e)
    {
        final SQLException sqlEx =
            new SQLException("error while fetching from cursor");
        if (e != null) {
            sqlEx.initCause(e);
        }
        return sqlEx;
    }

    private boolean toBoolean(Object o)
        throws SQLException
    {
        if (o == null) {
            wasNull = true;
            return false;
        } else {
            wasNull = false;
        }
        if (o instanceof Boolean) {
            return ((Boolean) o).booleanValue();
        } else {
            long value = toLong(o);
            if (value > 0) {
                return true;
            }
            return false;

            //throw newConversionError(o,boolean.class);
        }
    }

    private byte toByte(Object o)
        throws SQLException
    {
        if (o == null) {
            wasNull = true;
            return 0;
        } else {
            wasNull = false;
        }
        if (o instanceof Byte) {
            return ((Byte) o).byteValue();
        } else {
            return (byte) toLong_(o);
        }
    }

    private java.sql.Date toDate(Object o)
        throws SQLException
    {
        if (o == null) {
            wasNull = true;
            return null;
        } else {
            wasNull = false;
        }
        if (o instanceof java.sql.Date) {
            return (java.sql.Date) o;
        } else if (o instanceof java.util.Date) {
            return new java.sql.Date(((java.util.Date) o).getTime());
        } else {
            throw newConversionError(o, java.sql.Date.class);
        }
    }

    private double toDouble(Object o)
        throws SQLException
    {
        if (o == null) {
            wasNull = true;
            return 0.0;
        } else {
            wasNull = false;
        }
        if (o instanceof Double) {
            return ((Double) o).doubleValue();
        } else if (o instanceof Float) {
            return ((Float) o).doubleValue();
        } else if (o instanceof String) {
            try {
                return Double.parseDouble(((String) o).trim());
            } catch (NumberFormatException e) {
                throw new SQLException(
                    "Fail to convert to internal representation");
            }
        } else {
            return (double) toLong_(o);
        }
    }

    private float toFloat(Object o)
        throws SQLException
    {
        if (o == null) {
            wasNull = true;
            return (float) 0;
        } else {
            wasNull = false;
        }
        if (o instanceof Float) {
            return ((Float) o).floatValue();
        } else if (o instanceof Double) {
            return ((Double) o).floatValue();
        } else if (o instanceof String) {
            try {
                return Float.parseFloat(((String) o).trim());
            } catch (NumberFormatException e) {
                throw new SQLException(
                    "Fail to convert to internal representation");
            }
        } else {
            return (float) toLong_(o);
        }
    }

    private int toInt(Object o)
        throws SQLException
    {
        if (o == null) {
            wasNull = true;
            return 0;
        } else {
            wasNull = false;
        }
        if (o instanceof Integer) {
            return ((Integer) o).intValue();
        } else {
            return (int) toLong_(o);
        }
    }

    private long toLong(Object o)
        throws SQLException
    {
        if (o == null) {
            wasNull = true;
            return 0;
        } else {
            wasNull = false;
        }
        return toLong_(o);
    }

    private long toLong_(Object o)
        throws SQLException
    {
        if (o instanceof Long) {
            return ((Long) o).longValue();
        } else if (o instanceof Integer) {
            return ((Integer) o).longValue();
        } else if (o instanceof Short) {
            return ((Short) o).longValue();
        } else if (o instanceof Character) {
            return ((Character) o).charValue();
        } else if (o instanceof Byte) {
            return ((Byte) o).longValue();
        } else if (o instanceof Double) {
            return ((Double) o).longValue();
        } else if (o instanceof Float) {
            return ((Float) o).longValue();
        } else if (o instanceof Boolean) {
            if (((Boolean) o).booleanValue()) {
                return 1;
            } else {
                return 0;
            }
        } else if (o instanceof String) {
            try {
                return Long.parseLong(((String) o).trim());
            } catch (NumberFormatException e) {
                throw newConversionError(o, long.class);
            }
        } else {
            throw newConversionError(o, long.class);
        }
    }

    private short toShort(Object o)
        throws SQLException
    {
        if (o == null) {
            wasNull = true;
            return 0;
        } else {
            wasNull = false;
        }
        if (o instanceof Short) {
            return ((Short) o).shortValue();
        } else {
            return (short) toLong_(o);
        }
    }

    private String toString(Object o)
    {
        if (o == null) {
            wasNull = true;
            return null;
        } else if (o instanceof byte []) {
            // convert to hex string
            return Util.toStringFromByteArray((byte []) o, 16);
        } else {
            wasNull = false;
            return o.toString();
        }
    }

    private Time toTime(Object o)
        throws SQLException
    {
        if (o == null) {
            wasNull = true;
            return null;
        } else {
            wasNull = false;
        }
        if (o instanceof Time) {
            return (Time) o;
        } else {
            throw newConversionError(o, Time.class);
        }
    }

    private Timestamp toTimestamp(Object o)
        throws SQLException
    {
        if (o == null) {
            wasNull = true;
            return null;
        } else {
            wasNull = false;
        }
        if (o instanceof Timestamp) {
            return (Timestamp) o;
        } else {
            throw newConversionError(o, Timestamp.class);
        }
    }

    //~ Inner Interfaces ------------------------------------------------------

    /**
     * A <code>ColumnGetter</code> retrieves a column from an input row based
     * upon its 1-based ordinal.
     */
    public interface ColumnGetter
    {
        String [] getColumnNames();

        Object get(
            Object o,
            int columnIndex);
    }

    //~ Inner Classes ---------------------------------------------------------

    /**
     * A <code>FieldGetter</code> retrieves each public field as a separate
     * column.
     */
    public static class FieldGetter implements ColumnGetter
    {
        private static final Field [] emptyFieldArray = new Field[0];
        private final Class clazz;
        private final Field [] fields;

        public FieldGetter(Class clazz)
        {
            this.clazz = clazz;
            this.fields = getFields();
        }

        public String [] getColumnNames()
        {
            String [] columnNames = new String[fields.length];
            for (int i = 0; i < fields.length; i++) {
                columnNames[i] = fields[i].getName();
            }
            return columnNames;
        }

        public Object get(
            Object o,
            int columnIndex)
        {
            try {
                return fields[columnIndex - 1].get(o);
            } catch (IllegalArgumentException e) {
                throw Util.newInternal(e,
                    "Error while retrieving field " + fields[columnIndex - 1]);
            } catch (IllegalAccessException e) {
                throw Util.newInternal(e,
                    "Error while retrieving field " + fields[columnIndex - 1]);
            }
        }

        private Field [] getFields()
        {
            List list = new ArrayList();
            final Field [] fields = clazz.getFields();
            for (int i = 0; i < fields.length; i++) {
                Field field = fields[i];
                if (Modifier.isPublic(field.getModifiers())
                        && !Modifier.isStatic(field.getModifiers())) {
                    list.add(field);
                }
            }
            return (Field []) list.toArray(emptyFieldArray);
        }
    }

    // ------------------------------------------------------------------------
    // NOTE jvs 30-May-2003:  I made this public because iSQL wanted it that
    // way for reflection.
    public class MetaData implements ResultSetMetaData
    {
        public boolean isAutoIncrement(int column)
            throws SQLException
        {
            return false;
        }

        public boolean isCaseSensitive(int column)
            throws SQLException
        {
            return false;
        }

        public String getCatalogName(int column)
            throws SQLException
        {
            return null;
        }

        public String getColumnClassName(int column)
            throws SQLException
        {
            return null;
        }

        public int getColumnCount()
            throws SQLException
        {
            return getColumnNames().length;
        }

        public int getColumnDisplaySize(int column)
            throws SQLException
        {
            return 0;
        }

        public String getColumnLabel(int column)
            throws SQLException
        {
            return getColumnName(column);
        }

        public String getColumnName(int column)
            throws SQLException
        {
            return getColumnNames()[column - 1];
        }

        public int getColumnType(int column)
            throws SQLException
        {
            return 0;
        }

        public String getColumnTypeName(int column)
            throws SQLException
        {
            return null;
        }

        public boolean isCurrency(int column)
            throws SQLException
        {
            return false;
        }

        public boolean isDefinitelyWritable(int column)
            throws SQLException
        {
            return false;
        }

        public int isNullable(int column)
            throws SQLException
        {
            return 0;
        }

        public int getPrecision(int column)
            throws SQLException
        {
            return 0;
        }

        public boolean isReadOnly(int column)
            throws SQLException
        {
            return false;
        }

        public int getScale(int column)
            throws SQLException
        {
            return 0;
        }

        public String getSchemaName(int column)
            throws SQLException
        {
            return null;
        }

        public boolean isSearchable(int column)
            throws SQLException
        {
            return false;
        }

        public boolean isSigned(int column)
            throws SQLException
        {
            return false;
        }

        public String getTableName(int column)
            throws SQLException
        {
            return null;
        }

        public boolean isWritable(int column)
            throws SQLException
        {
            return false;
        }
    }

    /**
     * A <code>SingletonColumnGetter</code> retrieves the object itself.
     */
    public static class SingletonColumnGetter implements ColumnGetter
    {
        public SingletonColumnGetter()
        {
        }

        public String [] getColumnNames()
        {
            return new String [] { "column0" };
        }

        public Object get(
            Object o,
            int columnIndex)
        {
            assert (columnIndex == 1);
            return o;
        }
    }

    /**
     * A <code>SyntheticColumnGetter</code> retrieves columns from a synthetic
     * object.
     */
    public static class SyntheticColumnGetter implements ColumnGetter
    {
        String [] columnNames;
        Field [] fields;

        public SyntheticColumnGetter(Class clazz)
        {
            assert (SyntheticObject.class.isAssignableFrom(clazz));
            this.fields = clazz.getFields();
            this.columnNames = new String[fields.length];
            for (int i = 0; i < fields.length; i++) {
                columnNames[i] = fields[i].getName();
            }
        }

        public String [] getColumnNames()
        {
            return columnNames;
        }

        public Object get(
            Object o,
            int columnIndex)
        {
            try {
                return fields[columnIndex - 1].get(o);
            } catch (IllegalArgumentException e) {
                throw new RuntimeException(e);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Indicates that an operation timed out. This is not an error; you can
     * retry the operation.
     */
    public static class SqlTimeoutException extends SQLException
    {
        SqlTimeoutException() {
            // SQLException(reason, SQLState, vendorCode)
            // REVIEW mb 19-Jul-05 Is there a standard SQLState?
            super("timeout", null, 0);
        }
    }
}


// End IteratorResultSet.java
