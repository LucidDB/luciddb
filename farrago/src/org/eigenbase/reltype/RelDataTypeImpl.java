/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2004-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later Eigenbase-approved version.
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
import java.util.*;

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
    protected final List fieldList;
    protected String digest;

    protected RelDataTypeImpl(RelDataTypeField [] fields)
    {
        this.fields = fields;
        if (fields != null) {
            fieldList = Collections.unmodifiableList(Arrays.asList(fields));
        } else {
            fieldList = null;
        }
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
    public List getFieldList()
    {
        assert(isStruct());
        return fieldList;
    }
    
    // implement RelDataType
    public RelDataTypeField [] getFields()
    {
        assert(isStruct());
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
        return fields != null;
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
    public Charset getCharset()
    {
        return null;
    }

    // implement RelDataType
    public SqlCollation getCollation()
        throws RuntimeException
    {
        return null;
    }

    // implement RelDataType
    public SqlIntervalQualifier getIntervalQualifier()
    {
        return null;
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
    public SqlIdentifier getSqlIdentifier()
    {
        SqlTypeName typeName = getSqlTypeName();
        if (typeName == null) {
            return null;
        }
        return new SqlIdentifier(typeName.getName(), null);
    }

    // implement RelDataType
    public RelDataTypeFamily getFamily()
    {
        // by default, put each type into its own family
        return this;
    }

    /**
     * Generates a string representation of this type.
     *
     * @param sb StringBuffer into which to generate the string
     *
     * @param withDetail when true, all detail information needed to
     * compute a unique digest (and return from getFullTypeString)
     * should be included; when false, less verbosity is appropriate
     * (for return from toString)
     */
    protected abstract void generateTypeString(
        StringBuffer sb,
        boolean withDetail);

    /**
     * Computes the digest field.  This should be called in every
     * non-abstract subclass constructor once the type is fully defined.
     */
    protected void computeDigest()
    {
        StringBuffer sb = new StringBuffer();
        generateTypeString(sb, true);
        if (!isNullable()) {
            sb.append(" NOT NULL");
        }
        digest = sb.toString();
    }

    // implement RelDataType
    public String toString()
    {
        StringBuffer sb = new StringBuffer();
        generateTypeString(sb, false);
        return sb.toString();
    }
    
    // implement RelDataType
    public RelDataTypePrecedenceList getPrecedenceList()
    {
        // by default, make each type have a precedence list containing
        // only other types in the same family
        return new RelDataTypePrecedenceList() 
            {
                public boolean containsType(RelDataType type)
                {
                    return getFamily() == type.getFamily();
                }
                
                public int compareTypePrecedence(
                    RelDataType type1, RelDataType type2)
                {
                    assert(containsType(type1));
                    assert(containsType(type2));
                    return 0;
                }
            };
    }
}

// End RelDataTypeImpl.java
