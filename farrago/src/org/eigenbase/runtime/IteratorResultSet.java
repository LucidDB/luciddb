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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.*;
import java.util.*;

import org.eigenbase.util.Util;
import org.eigenbase.util14.AbstractResultSet;


/**
 * A <code>IteratorResultSet</code> is an adapter which converts a {@link
 * java.util.Iterator} into a {@link java.sql.ResultSet}.
 *
 * <p>
 * See also its converse adapter, {@link ResultSetIterator}
 * </p>
 */
public class IteratorResultSet extends AbstractResultSet
{
    //~ Instance fields -------------------------------------------------------

    private final ColumnGetter columnGetter;
    private final Iterator iterator;
    private Object current;
    private int row; // 1-based (starts on 0 to represent before first row)
    private TimeoutQueueIterator timeoutIter;
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

    public boolean isBeforeFirst()
        throws SQLException
    {
        // REVIEW jvs 25-June-2005:  make this return false if there are
        // no rows?
        return row == 0;
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

    public boolean isLast()
        throws SQLException
    {
        return false;
    }

    public ResultSetMetaData getMetaData()
        throws SQLException
    {
        return new MetaData();
    }

    public int getRow()
        throws SQLException
    {
        return row;
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

    /**
     * Returns the raw value of a column as an object.
     */
    protected Object getRaw(int columnIndex)
    {
        return columnGetter.get(current, columnIndex);
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
