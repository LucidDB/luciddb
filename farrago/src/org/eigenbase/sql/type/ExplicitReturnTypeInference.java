/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.util.*;


/**
 * A {@link SqlReturnTypeInference} which always returns the same SQL type.
 *
 * @author Wael Chatila
 * @version $Id$
 */
public class ExplicitReturnTypeInference
    implements SqlReturnTypeInference
{
    //~ Instance fields --------------------------------------------------------

    private final int argCount;
    private final SqlTypeName typeName;
    private final int length;
    private final int scale;
    private final RelDataType type;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates an inference rule which always returns the same type object.
     *
     * <p>If the requesting type factory is different, returns a copy of the
     * type object made using {@link RelDataTypeFactory#copyType(RelDataType)}
     * within the requesting type factory.
     *
     * <p>REVIEW jvs 6-Aug-2006: Under what circumstances is a copy of the type
     * required?
     *
     * @param type Type object
     */
    public ExplicitReturnTypeInference(RelDataType type)
    {
        this.type = type;
        this.typeName = null;
        this.length = -1;
        this.scale = -1;
        this.argCount = 0;
    }

    /**
     * Creates an inference rule which always returns a given SQL type with zero
     * parameters (such as <code>DATE</code>).
     *
     * @param typeName Name of the type
     */
    public ExplicitReturnTypeInference(SqlTypeName typeName)
    {
        this.argCount = 1;
        this.typeName = typeName;
        this.length = -1;
        this.scale = -1;
        this.type = null;
    }

    /**
     * Creates an inference rule which always returns a given SQL type with a
     * precision/length parameter (such as <code>VARCHAR(10)</code> and <code>
     * NUMBER(5)</code>).
     *
     * @param typeName Name of the type
     * @param length Length or precision of the type
     */
    public ExplicitReturnTypeInference(SqlTypeName typeName, int length)
    {
        this.argCount = 2;
        this.typeName = typeName;
        this.length = length;
        this.scale = -1;
        this.type = null;
    }

    /**
     * Creates an inference rule which always returns a given SQL type with a
     * precision and scale parameters (such as <code>DECIMAL(8, 3)</code>).
     *
     * @param typeName Name of the type
     * @param length Precision of the type
     */
    public ExplicitReturnTypeInference(
        SqlTypeName typeName,
        int length,
        int scale)
    {
        this.argCount = 3;
        this.typeName = typeName;
        this.length = length;
        this.scale = scale;
        this.type = null;
    }

    //~ Methods ----------------------------------------------------------------

    public RelDataType inferReturnType(
        SqlOperatorBinding opBinding)
    {
        if (type != null) {
            return opBinding.getTypeFactory().copyType(type);
        }
        return createType(opBinding.getTypeFactory());
    }

    protected RelDataType getExplicitType()
    {
        return type;
    }

    private RelDataType createType(RelDataTypeFactory typeFactory)
    {
        switch (argCount) {
        case 1:
            return typeFactory.createSqlType(typeName);
        case 2:
            return typeFactory.createSqlType(typeName, length);
        case 3:
            return typeFactory.createSqlType(typeName, length, scale);
        default:
            throw Util.newInternal("unexpected argCount " + argCount);
        }
    }
}

// End ExplicitReturnTypeInference.java
