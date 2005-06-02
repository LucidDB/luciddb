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
import org.eigenbase.sql.validate.*;

/**
 * SqlTypeTransforms defines a number of reusable instances of
 * {@link SqlTypeTransform}.
 *
 * @author Wael Chatila
 * @version $Id$
 */
public abstract class SqlTypeTransforms
{
    /**
     * Parameter type-inference transform strategy where a derived type is
     * transformed into the same type but nullable if any of a calls operands
     * is nullable
     */
    public static final SqlTypeTransform toNullable =
        new SqlTypeTransform()
        {
            public RelDataType getType(
                SqlValidator validator,
                SqlValidatorScope scope,
                RelDataTypeFactory typeFactory,
                CallOperands callOperands,
                RelDataType typeToTransform)
            {
                return SqlTypeUtil.makeNullableIfOperandsAre(
                    typeFactory,
                    callOperands.collectTypes(),
                    typeToTransform);
            }
        };

    /**
     * Parameter type-inference transform strategy where a derived INTERVAL type
     * is transformed into the same type but possible with a different
     * {@link org.eigenbase.sql.SqlIntervalQualifier}.
     * If the type to transform is not of a INTERVAL type, this transformation
     * does nothing.
     * @see {@link IntervalSqlType}
     */
    public static final SqlTypeTransform toLeastRestrictiveInterval =
        new SqlTypeTransform()
        {
            public RelDataType getType(
                SqlValidator validator,
                SqlValidatorScope scope,
                RelDataTypeFactory typeFactory,
                CallOperands callOperands,
                RelDataType typeToTransform)
            {
                if (typeToTransform instanceof IntervalSqlType) {
                    IntervalSqlType it =
                       (IntervalSqlType) typeToTransform;
                    for (int i = 0; i < callOperands.size(); i++) {
                        it = it.combine((IntervalSqlType)
                                            callOperands.getType(i));
                    }
                    return it;
                }
                return typeToTransform;
            }
        };
    /**
     * Type-inference strategy whereby the result type of a call is VARYING
     * the type given.
     * The length returned is the same as length of the
     * first argument.
     * Return type will have same nullablilty as input type nullablility.
     * First Arg must be of string type.
     */
    public static final SqlTypeTransform toVarying =
        new SqlTypeTransform()
        {
            public RelDataType getType(
                SqlValidator validator,
                SqlValidatorScope scope,
                RelDataTypeFactory fac,
                CallOperands callOperands,
                RelDataType typeToTransform)
            {
                switch (typeToTransform.getSqlTypeName().getOrdinal()) {
                case SqlTypeName.Varchar_ordinal:
                case SqlTypeName.Varbinary_ordinal:
                    return typeToTransform;
                }

                SqlTypeName retTypeName = toVar(typeToTransform);

                RelDataType ret =
                    fac.createSqlType(
                        retTypeName,
                        typeToTransform.getPrecision());
                if (SqlTypeUtil.inCharFamily(typeToTransform)) {
                    ret = fac.createTypeWithCharsetAndCollation(
                            ret,
                            typeToTransform.getCharset(),
                            typeToTransform.getCollation());
                }
                return fac.createTypeWithNullability(
                    ret,
                    typeToTransform.isNullable());
            }

            private SqlTypeName toVar(RelDataType type)
            {
                final SqlTypeName sqlTypeName = type.getSqlTypeName();
                switch (sqlTypeName.getOrdinal()) {
                case SqlTypeName.Char_ordinal:
                    return SqlTypeName.Varchar;
                case SqlTypeName.Binary_ordinal:
                    return SqlTypeName.Varbinary;
                default:
                    throw sqlTypeName.unexpected();
                }
            }
        };
    
    /**
     * Parameter type-inference transform strategy where a derived type
     * must be a multiset type and the returned type is the multiset's
     * element type.
     * @see {@link MultisetSqlType#getComponentType}
     */
    public static final SqlTypeTransform toMultisetElementType =
        new SqlTypeTransform()
        {
            public RelDataType getType(
                SqlValidator validator,
                SqlValidatorScope scope,
                RelDataTypeFactory typeFactory,
                CallOperands callOperands,
                RelDataType typeToTransform)
            {
                return typeToTransform.getComponentType();
            }
        };
}

// End SqlTypeTransforms.java
