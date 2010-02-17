/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2002-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2003-2009 John V. Sichi
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

import org.eigenbase.jdbc4.*;
import org.eigenbase.util.*;
import org.eigenbase.util14.*;


/**
 * AbstractIterResultSet provides functionality common to all ResultSet
 * implementations that convert from iterator convention.
 *
 * @author Stephan Zuercher
 * @version $Id$
 */
public abstract class AbstractIterResultSet
    extends AbstractResultSet
{
    //~ Instance fields --------------------------------------------------------

    private final ColumnGetter columnGetter;
    protected Object current;
    protected int row; // 1-based (starts on 0 to represent before first row)
    protected long timeoutMillis;

    //~ Constructors -----------------------------------------------------------

    protected AbstractIterResultSet(ColumnGetter columnGetter)
    {
        Util.discard(columnGetter.getColumnNames());
        this.columnGetter = columnGetter;
    }

    //~ Methods ----------------------------------------------------------------

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
     * Sets the timeout that this AbstractIterResultSet will wait for a row from
     * the underlying iterator. Note that the timeout must be implemented in the
     * abstract method {@link #next()}.
     *
     * @param timeoutMillis Timeout in milliseconds. Must be greater than zero.
     *
     * @pre timeoutMillis > 0
     * @pre this.timeoutMillis == 0
     */
    public void setTimeout(long timeoutMillis)
    {
        Util.pre(timeoutMillis > 0, "timeoutMillis > 0");
        Util.pre(this.timeoutMillis == 0, "this.timeoutMillis == 0");

        this.timeoutMillis = timeoutMillis;
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

    /**
     * Returns the raw value of a column as an object.
     */
    protected Object getRaw(int columnIndex)
    {
        return columnGetter.get(current, columnIndex);
    }

    //~ Inner Interfaces -------------------------------------------------------

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

    //~ Inner Classes ----------------------------------------------------------

    /**
     * A <code>FieldGetter</code> retrieves each public field as a separate
     * column.
     */
    public static class FieldGetter
        implements ColumnGetter
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
                if (Modifier.isPublic(field.getModifiers())
                    && !Modifier.isStatic(field.getModifiers()))
                {
                    list.add(field);
                }
            }
            return (Field []) list.toArray(emptyFieldArray);
        }
    }

    // ------------------------------------------------------------------------
    // NOTE jvs 30-May-2003:  I made this public because iSQL wanted it that
    // way for reflection.
    public class MetaData
        extends Unwrappable
        implements ResultSetMetaData
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
            return "";
        }

        public String getColumnClassName(int column)
            throws SQLException
        {
            return "";
        }

        public int getColumnCount()
            throws SQLException
        {
            return getColumnNames().length;
        }

        public int getColumnDisplaySize(int column)
            throws SQLException
        {
            return getPrecision(column);
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
            return Types.VARCHAR;
        }

        public String getColumnTypeName(int column)
            throws SQLException
        {
            return "VARCHAR";
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
            // NOTE jvs 13-June-2006:  I put this in so that EXPLAIN PLAN
            // would work via VJDBC; but should probably do something
            // better down at the Farrago level instead.
            return 65535;
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
            return "";
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
            return "";
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
    public static class SingletonColumnGetter
        implements ColumnGetter
    {
        public SingletonColumnGetter()
        {
        }

        public String [] getColumnNames()
        {
            return new String[] { "column0" };
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
    public static class SyntheticColumnGetter
        implements ColumnGetter
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
    public static class SqlTimeoutException
        extends SQLException
    {
        SqlTimeoutException()
        {
            // SQLException(reason, SQLState, vendorCode)
            // REVIEW mb 19-Jul-05 Is there a standard SQLState?
            super("timeout", null, 0);
        }
    }
}

// End AbstractIterResultSet.java
