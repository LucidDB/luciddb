/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// (C) Copyright 2003-2004 John V. Sichi
// You must accept the terms in LICENSE.html to use this software.
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

package net.sf.saffron.runtime;

import net.sf.saffron.util.Util;

import java.io.InputStream;
import java.io.Reader;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import java.math.BigDecimal;

import java.net.URL;

import java.sql.*;
import java.sql.Date;

import java.util.*;


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

    private ColumnGetter columnGetter;
    private Iterator iterator;
    private Object current;
    private String [] columnNames;
    private int row; // 0-based

    protected boolean wasNull;
    
    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a result set based upon an iterator. The column-getter accesses
     * columns based upon their ordinal.
     */
    public IteratorResultSet(Iterator iterator,ColumnGetter columnGetter)
    {
        this.iterator = iterator;
        if (columnNames == null) {
            columnNames = new String[0];
        }
        this.columnNames = columnGetter.getColumnNames();
        this.columnGetter = columnGetter;
    }

    //~ Methods ---------------------------------------------------------------

    public boolean isAfterLast() throws SQLException
    {
        return false;
    }

    public Array getArray(int i) throws SQLException
    {
        return null;
    }

    public Array getArray(String colName) throws SQLException
    {
        return null;
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

    public boolean isBeforeFirst() throws SQLException
    {
        return false;
    }

    public BigDecimal getBigDecimal(int columnIndex,int scale)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public BigDecimal getBigDecimal(String columnName,int scale)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public BigDecimal getBigDecimal(int columnIndex) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public BigDecimal getBigDecimal(String columnName)
        throws SQLException
    {
        throw new UnsupportedOperationException();
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

    public Blob getBlob(int i) throws SQLException
    {
        return null;
    }

    public Blob getBlob(String colName) throws SQLException
    {
        return null;
    }

    public boolean getBoolean(int columnIndex) throws SQLException
    {
        return toBoolean(getRaw(columnIndex));
    }

    public boolean getBoolean(String columnName) throws SQLException
    {
        return toBoolean(getRaw(columnName));
    }

    public byte getByte(int columnIndex) throws SQLException
    {
        return toByte(getRaw(columnIndex));
    }

    public byte getByte(String columnName) throws SQLException
    {
        return toByte(getRaw(columnName));
    }

    public byte [] getBytes(int columnIndex) throws SQLException
    {
        return (byte []) getRaw(columnIndex);
    }

    public byte [] getBytes(String columnName) throws SQLException
    {
        return (byte []) getRaw(columnName);
    }

    public Reader getCharacterStream(int columnIndex) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public Reader getCharacterStream(String columnName)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public Clob getClob(int i) throws SQLException
    {
        return null;
    }

    public Clob getClob(String colName) throws SQLException
    {
        return null;
    }

    public int getConcurrency() throws SQLException
    {
        return 0;
    }

    public String getCursorName() throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public Date getDate(int columnIndex) throws SQLException
    {
        return toDate(getRaw(columnIndex));
    }

    public Date getDate(String columnName) throws SQLException
    {
        return toDate(getRaw(columnName));
    }

    public Date getDate(int columnIndex,Calendar cal) throws SQLException
    {
        return null;
    }

    public Date getDate(String columnName,Calendar cal)
        throws SQLException
    {
        return null;
    }

    public double getDouble(int columnIndex) throws SQLException
    {
        return toDouble(getRaw(columnIndex));
    }

    public double getDouble(String columnName) throws SQLException
    {
        return toDouble(getRaw(columnName));
    }

    public void setFetchDirection(int direction) throws SQLException
    {
    }

    public int getFetchDirection() throws SQLException
    {
        return 0;
    }

    public void setFetchSize(int rows) throws SQLException
    {
    }

    public int getFetchSize() throws SQLException
    {
        return 0;
    }

    public boolean isFirst() throws SQLException
    {
        return false;
    }

    public float getFloat(int columnIndex) throws SQLException
    {
        return toFloat(getRaw(columnIndex));
    }

    public float getFloat(String columnName) throws SQLException
    {
        return toFloat(getRaw(columnName));
    }

    public int getInt(int columnIndex) throws SQLException
    {
        return toInt(getRaw(columnIndex));
    }

    public int getInt(String columnName) throws SQLException
    {
        return toInt(getRaw(columnName));
    }

    public boolean isLast() throws SQLException
    {
        return false;
    }

    public long getLong(int columnIndex) throws SQLException
    {
        return toLong(getRaw(columnIndex));
    }

    public long getLong(String columnName) throws SQLException
    {
        return toLong(getRaw(columnName));
    }

    public ResultSetMetaData getMetaData() throws SQLException
    {
        return new MetaData();
    }

    public Object getObject(int columnIndex) throws SQLException
    {
        Object o = getRaw(columnIndex);
        if (o == null) {
            wasNull = true;
        } else {
            wasNull = false;
        }
        return o;
    }

    public Object getObject(String columnName) throws SQLException
    {
        return getObject(findColumn(columnName));
    }

    public Object getObject(int i,Map map) throws SQLException
    {
        return null;
    }

    public Object getObject(String colName,Map map) throws SQLException
    {
        return null;
    }

    public Ref getRef(int i) throws SQLException
    {
        return null;
    }

    public Ref getRef(String colName) throws SQLException
    {
        return null;
    }

    public int getRow() throws SQLException
    {
        return row + 1;
    }

    public short getShort(int columnIndex) throws SQLException
    {
        return toShort(getRaw(columnIndex));
    }

    public short getShort(String columnName) throws SQLException
    {
        return toShort(getRaw(columnName));
    }

    public Statement getStatement() throws SQLException
    {
        return null;
    }

    public String getString(int columnIndex) throws SQLException
    {
        return toString(getRaw(columnIndex));
    }

    public String getString(String columnName) throws SQLException
    {
        return toString(getRaw(columnName));
    }

    public Time getTime(int columnIndex) throws SQLException
    {
        return toTime(getRaw(columnIndex));
    }

    public Time getTime(String columnName) throws SQLException
    {
        return toTime(getRaw(columnName));
    }

    public Time getTime(int columnIndex,Calendar cal) throws SQLException
    {
        return null;
    }

    public Time getTime(String columnName,Calendar cal)
        throws SQLException
    {
        return null;
    }

    public Timestamp getTimestamp(int columnIndex) throws SQLException
    {
        return toTimestamp(getRaw(columnIndex));
    }

    public Timestamp getTimestamp(String columnName) throws SQLException
    {
        return toTimestamp(getRaw(columnName));
    }

    public Timestamp getTimestamp(int columnIndex,Calendar cal)
        throws SQLException
    {
        return null;
    }

    public Timestamp getTimestamp(String columnName,Calendar cal)
        throws SQLException
    {
        return null;
    }

    public int getType() throws SQLException
    {
        return 0;
    }

    public URL getURL(int columnIndex) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public URL getURL(String columnName) throws SQLException
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

    public SQLWarning getWarnings() throws SQLException
    {
        return null;
    }

    public boolean absolute(int row) throws SQLException
    {
        return relative(row - getRow());
    }

    public void afterLast() throws SQLException
    {
    }

    public void beforeFirst() throws SQLException
    {
    }

    public void cancelRowUpdates() throws SQLException
    {
    }

    public void clearWarnings() throws SQLException
    {
    }

    public void close() throws SQLException
    {
    }

    public void deleteRow() throws SQLException
    {
    }

    public int findColumn(String columnName) throws SQLException
    {
        for (int i = 0; i < columnNames.length; i++) {
            if (columnName.equals(columnNames[i])) {
                return i + 1;
            }
        }
        throw new SQLException("column '" + columnName + "' not found");
    }

    public boolean first() throws SQLException
    {
        return false;
    }

    public void insertRow() throws SQLException
    {
    }

    public boolean last() throws SQLException
    {
        return false;
    }

    public void moveToCurrentRow() throws SQLException
    {
    }

    public void moveToInsertRow() throws SQLException
    {
    }

    // ------------------------------------------------------------------------
    // the remaining methods implement ResultSet
    public boolean next() throws SQLException
    {
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

    public boolean previous() throws SQLException
    {
        return false;
    }

    public void refreshRow() throws SQLException
    {
    }

    public boolean relative(int rows) throws SQLException
    {
        if (rows < 0) {
            throw newDirectionError();
        }
        while (rows-- > 0) {
            if (!next()) {
                return false;
            }
        }
        return true;
    }

    public boolean rowDeleted() throws SQLException
    {
        return false;
    }

    public boolean rowInserted() throws SQLException
    {
        return false;
    }

    public boolean rowUpdated() throws SQLException
    {
        return false;
    }

    public void updateArray(int columnIndex,Array x) throws SQLException
    {
    }

    public void updateArray(String columnName,Array x)
        throws SQLException
    {
    }

    public void updateAsciiStream(int columnIndex,InputStream x,int length)
        throws SQLException
    {
    }

    public void updateAsciiStream(String columnName,InputStream x,int length)
        throws SQLException
    {
    }

    public void updateBigDecimal(int columnIndex,BigDecimal x)
        throws SQLException
    {
    }

    public void updateBigDecimal(String columnName,BigDecimal x)
        throws SQLException
    {
    }

    public void updateBinaryStream(int columnIndex,InputStream x,int length)
        throws SQLException
    {
    }

    public void updateBinaryStream(String columnName,InputStream x,int length)
        throws SQLException
    {
    }

    public void updateBlob(int columnIndex,Blob x) throws SQLException
    {
    }

    public void updateBlob(String columnName,Blob x) throws SQLException
    {
    }

    public void updateBoolean(int columnIndex,boolean x)
        throws SQLException
    {
    }

    public void updateBoolean(String columnName,boolean x)
        throws SQLException
    {
    }

    public void updateByte(int columnIndex,byte x) throws SQLException
    {
    }

    public void updateByte(String columnName,byte x) throws SQLException
    {
    }

    public void updateBytes(int columnIndex,byte [] x)
        throws SQLException
    {
    }

    public void updateBytes(String columnName,byte [] x)
        throws SQLException
    {
    }

    public void updateCharacterStream(int columnIndex,Reader x,int length)
        throws SQLException
    {
    }

    public void updateCharacterStream(
        String columnName,
        Reader reader,
        int length) throws SQLException
    {
    }

    public void updateClob(int columnIndex,Clob x) throws SQLException
    {
    }

    public void updateClob(String columnName,Clob x) throws SQLException
    {
    }

    public void updateDate(int columnIndex,Date x) throws SQLException
    {
    }

    public void updateDate(String columnName,Date x) throws SQLException
    {
    }

    public void updateDouble(int columnIndex,double x)
        throws SQLException
    {
    }

    public void updateDouble(String columnName,double x)
        throws SQLException
    {
    }

    public void updateFloat(int columnIndex,float x) throws SQLException
    {
    }

    public void updateFloat(String columnName,float x)
        throws SQLException
    {
    }

    public void updateInt(int columnIndex,int x) throws SQLException
    {
    }

    public void updateInt(String columnName,int x) throws SQLException
    {
    }

    public void updateLong(int columnIndex,long x) throws SQLException
    {
    }

    public void updateLong(String columnName,long x) throws SQLException
    {
    }

    public void updateNull(int columnIndex) throws SQLException
    {
    }

    public void updateNull(String columnName) throws SQLException
    {
    }

    public void updateObject(int columnIndex,Object x,int scale)
        throws SQLException
    {
    }

    public void updateObject(int columnIndex,Object x)
        throws SQLException
    {
    }

    public void updateObject(String columnName,Object x,int scale)
        throws SQLException
    {
    }

    public void updateObject(String columnName,Object x)
        throws SQLException
    {
    }

    public void updateRef(int columnIndex,Ref x) throws SQLException
    {
    }

    public void updateRef(String columnName,Ref x) throws SQLException
    {
    }

    public void updateRow() throws SQLException
    {
    }

    public void updateShort(int columnIndex,short x) throws SQLException
    {
    }

    public void updateShort(String columnName,short x)
        throws SQLException
    {
    }

    public void updateString(int columnIndex,String x)
        throws SQLException
    {
    }

    public void updateString(String columnName,String x)
        throws SQLException
    {
    }

    public void updateTime(int columnIndex,Time x) throws SQLException
    {
    }

    public void updateTime(String columnName,Time x) throws SQLException
    {
    }

    public void updateTimestamp(int columnIndex,Timestamp x)
        throws SQLException
    {
    }

    public void updateTimestamp(String columnName,Timestamp x)
        throws SQLException
    {
    }

    public boolean wasNull() throws SQLException
    {
        return wasNull;
    }

    /**
     * Returns the raw value of a column as an object.
     */
    protected Object getRaw(int columnIndex)
    {
        return columnGetter.get(current,columnIndex);
    }

    protected Object getRaw(String columnName) throws SQLException
    {
        return getRaw(findColumn(columnName));
    }

    private SQLException newConversionError(Object o,Class clazz)
    {
        return new SQLException(
            "cannot convert " + o.getClass() + "(" + o + ") to " + clazz);
    }

    private SQLException newDirectionError()
    {
        return new SQLException("cannot go backwards");
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

    private boolean toBoolean(Object o) throws SQLException
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
            throw newConversionError(o,boolean.class);
        }
    }

    private byte toByte(Object o) throws SQLException
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

    private java.sql.Date toDate(Object o) throws SQLException
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
            throw newConversionError(o,java.sql.Date.class);
        }
    }

    private double toDouble(Object o) throws SQLException
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
        } else {
            return (double) toLong_(o);
        }
    }

    private float toFloat(Object o) throws SQLException
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
        } else {
            return (float) toLong_(o);
        }
    }

    private int toInt(Object o) throws SQLException
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

    private long toLong(Object o) throws SQLException
    {
        if (o == null) {
            wasNull = true;
            return 0;
        } else {
            wasNull = false;
        }
        return toLong_(o);
    }

    private long toLong_(Object o) throws SQLException
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
        } else {
            throw newConversionError(o,long.class);
        }
    }

    private short toShort(Object o) throws SQLException
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
        } else {
            wasNull = false;
            return o.toString();
        }
    }

    private Time toTime(Object o) throws SQLException
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
            throw newConversionError(o,Time.class);
        }
    }

    private Timestamp toTimestamp(Object o) throws SQLException
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
            throw newConversionError(o,Timestamp.class);
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

        Object get(Object o,int columnIndex);
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

        public Object get(Object o,int columnIndex)
        {
            try {
                return fields[columnIndex - 1].get(o);
            } catch (IllegalArgumentException e) {
                throw Util.newInternal(
                    e,
                    "Error while retrieving field " + fields[columnIndex - 1]);
            } catch (IllegalAccessException e) {
                throw Util.newInternal(
                    e,
                    "Error while retrieving field " + fields[columnIndex - 1]);
            }
        }

        private Field [] getFields()
        {
            List list = new ArrayList();
            final Field [] fields = clazz.getFields();
            for (int i = 0; i < fields.length; i++) {
                Field field = fields[i];
                if (
                    Modifier.isPublic(field.getModifiers())
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
        public boolean isAutoIncrement(int column) throws SQLException
        {
            return false;
        }

        public boolean isCaseSensitive(int column) throws SQLException
        {
            return false;
        }

        public String getCatalogName(int column) throws SQLException
        {
            return null;
        }

        public String getColumnClassName(int column) throws SQLException
        {
            return null;
        }

        public int getColumnCount() throws SQLException
        {
            return columnNames.length;
        }

        public int getColumnDisplaySize(int column) throws SQLException
        {
            return 0;
        }

        public String getColumnLabel(int column) throws SQLException
        {
            return getColumnName(column);
        }

        public String getColumnName(int column) throws SQLException
        {
            return columnNames[column - 1];
        }

        public int getColumnType(int column) throws SQLException
        {
            return 0;
        }

        public String getColumnTypeName(int column) throws SQLException
        {
            return null;
        }

        public boolean isCurrency(int column) throws SQLException
        {
            return false;
        }

        public boolean isDefinitelyWritable(int column)
            throws SQLException
        {
            return false;
        }

        public int isNullable(int column) throws SQLException
        {
            return 0;
        }

        public int getPrecision(int column) throws SQLException
        {
            return 0;
        }

        public boolean isReadOnly(int column) throws SQLException
        {
            return false;
        }

        public int getScale(int column) throws SQLException
        {
            return 0;
        }

        public String getSchemaName(int column) throws SQLException
        {
            return null;
        }

        public boolean isSearchable(int column) throws SQLException
        {
            return false;
        }

        public boolean isSigned(int column) throws SQLException
        {
            return false;
        }

        public String getTableName(int column) throws SQLException
        {
            return null;
        }

        public boolean isWritable(int column) throws SQLException
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

        public Object get(Object o,int columnIndex)
        {
            assert(columnIndex == 1);
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
            assert(SyntheticObject.class.isAssignableFrom(clazz));
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

        public Object get(Object o,int columnIndex)
        {
            try {
                return fields[columnIndex - 1].get(o);
            } catch (IllegalArgumentException e) {
                throw new SaffronError(e);
            } catch (IllegalAccessException e) {
                throw new SaffronError(e);
            }
        }
    }
}


// End IteratorResultSet.java
