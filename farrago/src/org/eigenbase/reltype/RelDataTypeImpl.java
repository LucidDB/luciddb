/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2004-2004 Disruptive Tech
// Copyright (C) 2004-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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
package org.eigenbase.reltype;

import java.nio.charset.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;

/**
 * RelDataTypeImpl is an abstract base for implementations of
 * {@link RelDataType}.
 *
 *
 * <p>
 * Identity is based upon the {@link #digest} field, which each derived
 * class should set during construction.
 * </p>
 *
 * @author jhyde
 * @version $Id$
 */
public abstract class RelDataTypeImpl
    implements RelDataType, RelDataTypeFamily
{
    protected final RelDataTypeField [] fields;
    protected String digest;

    protected RelDataTypeImpl(RelDataTypeField [] fields)
    {
        this.fields = fields;
    }

    // implement RelDataType
    public RelDataTypeField getField(String fieldName)
    {
        for (int i = 0; i < fields.length; i++) {
            RelDataTypeField field = fields[i];
            if (field.getName().equals(fieldName)) {
                return field;
            }
        }
        return null;
    }

    // implement RelDataType
    public int getFieldCount()
    {
        return fields.length;
    }

    // implement RelDataType
    public int getFieldOrdinal(String fieldName)
    {
        for (int i = 0; i < fields.length; i++) {
            RelDataTypeField field = fields[i];
            if (field.getName().equals(fieldName)) {
                return i;
            }
        }
        return -1;
    }

    // implement RelDataType
    public RelDataTypeField [] getFields()
    {
        return fields;
    }

    // implement RelDataType
    public RelDataType getComponentType()
    {
        // this is not a collection type
        return null;
    }

    // implement RelDataType
    public boolean isStruct()
    {
        return false;
    }

    // implement RelDataType
    public boolean equals(Object obj)
    {
        if (obj instanceof RelDataTypeImpl) {
            final RelDataTypeImpl that = (RelDataTypeImpl) obj;
            return this.digest.equals(that.digest);
        }
        return false;
    }

    // implement RelDataType
    public int hashCode()
    {
        return digest.hashCode();
    }

    // implement RelDataType
    public String toString()
    {
        return digest;
    }

    // implement RelDataType
    public String getFullTypeString()
    {
        return digest;
    }

    // implement RelDataType
    public boolean isNullable()
    {
        return false;
    }

    // implement RelDataType
    public boolean isAssignableFrom(
        RelDataType t,
        boolean coerce)
    {
        SqlTypeName thisName = getSqlTypeName();
        SqlTypeName thatName = t.getSqlTypeName();
        if (thisName == null || thatName == null) {
            return false;
        }
        SqlTypeAssignmentRules assignmentRules =
            SqlTypeAssignmentRules.instance();
        return assignmentRules.isAssignableFrom(thisName, thatName, coerce);
    }

    // implement RelDataType
    public Charset getCharset()
    {
        throw Util.newInternal("attribute not applicable");
    }

    // implement RelDataType
    public SqlCollation getCollation()
        throws RuntimeException
    {
        throw Util.newInternal("attribute not applicable");
    }

    // implement RelDataType
    public int getPrecision()
    {
        throw Util.newInternal("attribute not applicable");
    }

    // implement RelDataType
    public int getScale()
    {
        throw Util.newInternal("attribute not applicable");
    }

    // implement RelDataType
    public SqlTypeName getSqlTypeName()
    {
        return null;
    }

    // implement RelDataType
    public RelDataTypeFamily getFamily()
    {
        // by default, put each type into its own family
        return this;
    }
}

// End RelDataTypeImpl.java
