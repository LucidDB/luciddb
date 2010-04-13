/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2004 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2004 John V. Sichi
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
package org.eigenbase.sql.type;

import java.io.*;

import org.eigenbase.reltype.*;


/**
 * Abstract base class for SQL implementations of {@link RelDataType}.
 *
 * @author jhyde
 * @version $Id$
 */
public abstract class AbstractSqlType
    extends RelDataTypeImpl
    implements Cloneable,
        Serializable
{
    //~ Instance fields --------------------------------------------------------

    protected final SqlTypeName typeName;
    protected boolean isNullable;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates an AbstractSqlType.
     *
     * @param typeName Type name
     * @param isNullable Whether nullable
     * @param fields Fields of type, or null if not a record type
     */
    protected AbstractSqlType(
        SqlTypeName typeName,
        boolean isNullable,
        RelDataTypeField [] fields)
    {
        super(fields);
        this.typeName = typeName;
        this.isNullable = isNullable || (typeName == SqlTypeName.NULL);
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelDataType
    public SqlTypeName getSqlTypeName()
    {
        return typeName;
    }

    // implement RelDataType
    public boolean isNullable()
    {
        return isNullable;
    }

    // implement RelDataType
    public RelDataTypeFamily getFamily()
    {
        return SqlTypeFamily.getFamilyForSqlType(typeName);
    }

    // implement RelDataType
    public RelDataTypePrecedenceList getPrecedenceList()
    {
        RelDataTypePrecedenceList list =
            SqlTypeExplicitPrecedenceList.getListForType(this);
        if (list != null) {
            return list;
        }
        return super.getPrecedenceList();
    }
}

// End AbstractSqlType.java
