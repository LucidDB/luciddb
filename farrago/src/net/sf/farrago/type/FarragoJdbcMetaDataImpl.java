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
package net.sf.farrago.type;

import java.sql.*;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.type.*;


/**
 * Helper base class for implementing Jdbc metadata interfaces.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoJdbcMetaDataImpl
{
    //~ Instance fields -------------------------------------------------------

    /** Type info to return. */
    protected final RelDataType rowType;

    //~ Constructors ----------------------------------------------------------

    protected FarragoJdbcMetaDataImpl(RelDataType rowType)
    {
        this.rowType = rowType;
    }

    //~ Methods ---------------------------------------------------------------

    protected RelDataType getFieldType(int fieldOrdinal)
    {
        return rowType.getFields()[fieldOrdinal - 1].getType();
    }

    public String getFieldName(int fieldOrdinal)
        throws SQLException
    {
        return rowType.getFields()[fieldOrdinal - 1].getName();
    }

    protected int getFieldCount()
    {
        return rowType.getFieldList().size();
    }

    protected String getFieldClassName(int fieldOrdinal)
    {
        // TODO
        return "";
    }

    protected int getFieldJdbcType(int fieldOrdinal)
    {
        RelDataType type = getFieldType(fieldOrdinal);
        SqlTypeName typeName = type.getSqlTypeName();
        if (typeName == null) {
            return Types.OTHER;
        }
        return typeName.getJdbcOrdinal();
    }

    protected String getFieldTypeName(int fieldOrdinal)
    {
        RelDataType type = getFieldType(fieldOrdinal);
        SqlTypeName typeName = type.getSqlTypeName();
        if (typeName == null) {
            return type.toString();
        }
        return typeName.getName();
    }

    protected int getFieldPrecision(int fieldOrdinal)
    {
        RelDataType type = getFieldType(fieldOrdinal);
        return type.getPrecision();
    }

    protected int getFieldScale(int fieldOrdinal)
    {
        RelDataType type = getFieldType(fieldOrdinal);
        SqlTypeName typeName = type.getSqlTypeName();
        if (typeName == null) {
            return 0;
        }
        if (typeName.allowsPrecScale(true, true)) {
            return type.getScale();
        } else {
            return 0;
        }
    }

    protected int isFieldNullable(int fieldOrdinal)
    {
        RelDataType type = getFieldType(fieldOrdinal);
        return type.isNullable() ? ResultSetMetaData.columnNullable
        : ResultSetMetaData.columnNoNulls;
    }

    protected boolean isFieldSigned(int fieldOrdinal)
    {
        // TODO
        return false;
    }
}


// End FarragoJdbcMetaDataImpl.java
