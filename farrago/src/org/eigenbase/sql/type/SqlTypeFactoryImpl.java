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
package org.eigenbase.sql.type;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.util.*;

import java.nio.charset.*;

/**
 * SqlTypeFactoryImpl provides a default implementation
 * of {@link RelDataTypeFactory} which supports SQL types.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class SqlTypeFactoryImpl extends RelDataTypeFactoryImpl
{
    public SqlTypeFactoryImpl()
    {
    }

    // implement RelDataTypeFactory
    public RelDataType createSqlType(SqlTypeName typeName)
    {
        assertBasic(typeName);
        RelDataType newType = new BasicSqlType(typeName);
        return canonize(newType);
    }

    // implement RelDataTypeFactory
    public RelDataType createSqlType(
        SqlTypeName typeName,
        int precision)
    {
        assertBasic(typeName);
        Util.pre(precision >= 0, "precision >= 0");
        RelDataType newType = new BasicSqlType(typeName, precision);
        return canonize(newType);
    }

    // implement RelDataTypeFactory
    public RelDataType createSqlType(
        SqlTypeName typeName,
        int precision,
        int scale)
    {
        assertBasic(typeName);
        Util.pre(precision >= 0, "precision >= 0");
        RelDataType newType = new BasicSqlType(typeName, precision, scale);
        return canonize(newType);
    }
    
    // implement RelDataTypeFactory
    public RelDataType createMultisetType(RelDataType type, long maxCardinality)
    {
        assert(maxCardinality == -1);
        RelDataType newType = new MultisetSqlType(type, false);
        return canonize(newType);
    }

    // implement RelDataTypeFactory
    public RelDataType createSqlIntervalType(
        SqlIntervalQualifier intervalQualifier)
    {
        RelDataType newType = new IntervalSqlType(intervalQualifier, false);
        return canonize(newType);
    }
    
    // implement RelDataTypeFactory
    public RelDataType createTypeWithCharsetAndCollation(
        RelDataType type,
        Charset charset,
        SqlCollation collation)
    {
        Util.pre(SqlTypeUtil.inCharFamily(type), "Not a chartype");
        Util.pre(charset != null, "charset!=null");
        Util.pre(collation != null, "collation!=null");
        RelDataType newType;
        if (type instanceof BasicSqlType) {
            BasicSqlType sqlType = (BasicSqlType) type;
            newType = sqlType.createWithCharsetAndCollation(charset, collation);
        } else if (type instanceof JavaType) {
            JavaType javaType = (JavaType) type;
            newType = new JavaType(
                javaType.clazz,
                javaType.isNullable(),
                charset,
                collation);
        } else {
            throw Util.needToImplement("need to implement " + type);
        }
        return canonize(newType);
    }

    // implement RelDataTypeFactory
    public RelDataType leastRestrictive(RelDataType [] types)
    {
        assert (types != null);
        assert (types.length >= 1);
        
        RelDataType type0 = types[0];
        if (type0.getSqlTypeName() != null) {
            RelDataType resultType = leastRestrictiveSqlType(types);
            if (resultType != null) {
                return resultType;
            }
            return leastRestrictiveByCast(types);
        }

        return super.leastRestrictive(types);
    }
    
    private RelDataType leastRestrictiveByCast(RelDataType [] types)
    {
        RelDataType resultType = types[0];
        for (int i = 1; i < types.length; i++) {
            RelDataType type = types[i];
            if (type.getSqlTypeName() == SqlTypeName.Null) {
                continue;
            }

            if (SqlTypeUtil.canCastFrom(type, resultType, false)) {
                resultType = type;
            } else {
                if (!SqlTypeUtil.canCastFrom(resultType, type, false)) {
                    return null;
                }
            }
        }
        return resultType;
    }
    
    // implement RelDataTypeFactory
    public RelDataType createTypeWithNullability(
        final RelDataType type,
        final boolean nullable)
    {
        RelDataType newType;
        if (type instanceof BasicSqlType) {
            BasicSqlType sqlType = (BasicSqlType) type;
            newType = sqlType.createWithNullability(nullable);
        } else if (type instanceof MultisetSqlType) {
            newType = copyMultisetType(type, nullable);
        } else if (type instanceof IntervalSqlType) {
            newType = copyIntervalType(type, nullable);
        } else if (type instanceof ObjectSqlType) {
            newType = copyObjectType(type, nullable);
        } else {
            return super.createTypeWithNullability(type, nullable);
        }
        return canonize(newType);
    }
    
    private void assertBasic(SqlTypeName typeName)
    {
        assert(typeName != null);
        assert(typeName != SqlTypeName.Multiset) :
            "use createMultisetType() instead";
        assert(typeName != SqlTypeName.IntervalDayTime) :
            "use createSqlIntervalType() instead";
        assert(typeName != SqlTypeName.IntervalYearMonth) :
            "use createSqlIntervalType() instead";
    }
    
    private RelDataType leastRestrictiveSqlType(RelDataType [] types)
    {
        RelDataType resultType = types[0];
        boolean anyNullable = resultType.isNullable();

        for (int i = 1; i < types.length; ++i) {
            RelDataTypeFamily resultFamily = resultType.getFamily();
            RelDataType type = types[i];
            RelDataTypeFamily family = type.getFamily();

            SqlTypeName typeName = type.getSqlTypeName();
            SqlTypeName resultTypeName = resultType.getSqlTypeName();

            if (typeName == null) {
                return null;
            }

            if (type.isNullable()) {
                anyNullable = true;
            }

            if (SqlTypeUtil.inCharOrBinaryFamilies(type)) {
                // TODO:  character set, collation
                if (resultFamily != family) {
                    return null;
                }
                int precision =
                    Math.max(
                        resultType.getPrecision(),
                        type.getPrecision());

                // If either type is LOB, then result is LOB with no precision.
                // Otherwise, if either is variable width, result is variable
                // width.  Otherwise, result is fixed width.
                if (SqlTypeUtil.isLob(resultType)) {
                    resultType = createSqlType(resultType.getSqlTypeName());
                } else if (SqlTypeUtil.isLob(type)) {
                    resultType = createSqlType(type.getSqlTypeName());
                } else if (SqlTypeUtil.isBoundedVariableWidth(resultType)) {
                    resultType =
                        createSqlType(
                            resultType.getSqlTypeName(),
                            precision);
                } else {
                    // this catch-all case covers type variable, and both fixed
                    resultType =
                        createSqlType(
                            type.getSqlTypeName(),
                            precision);
                }
            } else if (SqlTypeUtil.isExactNumeric(type)) {
                if (SqlTypeUtil.isExactNumeric(resultType)) {
                    if (!type.equals(resultType)) {
                        if (!typeName.allowsPrec()
                            && !resultTypeName.allowsPrec())
                        {
                            // use the bigger primitive
                            if (type.getPrecision() > resultType.getPrecision()) {
                                resultType = type;
                            }
                        } else {
                            // TODO:  the real thing for numerics;
                            // for now we let leastRestrictiveGenericType
                            // handle it
                            return null;
                        }
                    }
                } else if (SqlTypeUtil.isApproximateNumeric(resultType)) {
                    // already approximate; promote to double just in case
                    // TODO:  only promote when required
                    resultType = createDoublePrecisionType();
                } else {
                    return null;
                }
            } else if (SqlTypeUtil.isApproximateNumeric(type)) {
                if (type !=resultType) {
                    resultType = createDoublePrecisionType();
                }
            } else {
                // TODO:  datetime precision details; for now we let
                // leastRestrictiveGenericType handle it
                return null;
            }
        }
        if (anyNullable) {
            return createTypeWithNullability(resultType, true);
        } else {
            return resultType;
        }
    }

    private RelDataType createDoublePrecisionType()
    {
        return createSqlType(SqlTypeName.Double);
    }
    
    private RelDataType copyMultisetType(RelDataType type, boolean nullable)
    {
        MultisetSqlType mt = (MultisetSqlType) type;
        RelDataType elementType = copyType(mt.getComponentType());
        return new MultisetSqlType(elementType, nullable);
    }

    private RelDataType copyIntervalType(RelDataType type, boolean nullable)
    {
        return new IntervalSqlType(type.getIntervalQualifier(), nullable);
    }

    private RelDataType copyObjectType(RelDataType type, boolean nullable)
    {
        return new ObjectSqlType(
            type.getSqlTypeName(),
            type.getSqlIdentifier(),
            nullable,
            type.getFields());
    }
}

// End SqlTypeFactoryImpl.java
