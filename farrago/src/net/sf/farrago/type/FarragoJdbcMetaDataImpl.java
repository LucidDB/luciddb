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

import org.eigenbase.reltype.*;

import java.sql.*;

/**
 * Helper base class for implementing Jdbc metadata interfaces.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoJdbcMetaDataImpl
{
    /** Type info to return. */
    protected final RelDataType rowType;

    protected FarragoJdbcMetaDataImpl(RelDataType rowType)
    {
        this.rowType = rowType;
    }

    protected FarragoAtomicType getFarragoType(int fieldOrdinal)
    {
        return (FarragoAtomicType)
            rowType.getFields()[fieldOrdinal - 1].getType();
    }
    
    public String getFieldName(int fieldOrdinal) throws SQLException
    {
        return rowType.getFields()[fieldOrdinal - 1].getName();
    }

    protected int getFieldCount()
    {
        return rowType.getFieldCount();
    }
    
    protected String getFieldClassName(int fieldOrdinal)
    {
        // TODO
        return "";
    }

    protected int getFieldType(int fieldOrdinal)
    {
        FarragoAtomicType type = getFarragoType(fieldOrdinal);
        return type.getSimpleType().getTypeNumber().intValue();
    }

    protected String getFieldTypeName(int fieldOrdinal)
    {
        FarragoAtomicType type = getFarragoType(fieldOrdinal);
        return type.getSimpleType().getName();
    }

    protected int getFieldPrecision(int fieldOrdinal)
    {
        FarragoAtomicType type = getFarragoType(fieldOrdinal);
        return type.getPrecision();
    }

    protected int getFieldScale(int fieldOrdinal)
    {
        FarragoAtomicType type = getFarragoType(fieldOrdinal);
        if (type instanceof FarragoPrecisionType) {
            FarragoPrecisionType precisionType = (FarragoPrecisionType) type;
            return precisionType.getScale();
        } else {
            return 0;
        }
    }

    protected int isFieldNullable(int fieldOrdinal)
    {
        FarragoAtomicType type = getFarragoType(fieldOrdinal);
        return type.isNullable()
            ? ResultSetMetaData.columnNullable
            : ResultSetMetaData.columnNoNulls;
    }

    protected boolean isFieldSigned(int fieldOrdinal)
    {
        // TODO
        return false;
    }
}

// End FarragoJdbcMetaDataImpl.java
