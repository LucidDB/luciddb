/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2003-2005 John V. Sichi
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
//
*/
package org.eigenbase.sql.type;

import org.eigenbase.reltype.*;
import org.eigenbase.util.Util;
import org.eigenbase.sql.*;
import org.eigenbase.rex.RexNode;
import org.eigenbase.resource.EigenbaseResource;
import org.eigenbase.rel.RelNode;

import java.util.*;

/**
 * Contains utility methods used during SQL validation or type derivation.
 *
 * @author Wael Chatila
 * @since Sep 3, 2004
 * @version $Id$
 */
public abstract class SqlTypeUtil
{
    /**
     * Checks if two types or more are char comparable.
     * @pre argTypes != null
     * @pre argTypes.length >= 2
     * @return Returns true if all operands are of char type
     *         and if they are comparable, i.e. of the same charset and
     *         collation of same charset
     */
    public static boolean isCharTypeComparable(RelDataType [] argTypes)
    {
        Util.pre(null != argTypes, "null!=argTypes");
        Util.pre(2 <= argTypes.length, "2<=argTypes.length");

        for (int j = 0; j < (argTypes.length - 1); j++) {
            RelDataType t0 = argTypes[j];
            RelDataType t1 = argTypes[j + 1];

            if (!inCharFamily(t0) || !inCharFamily(t1)) {
                return false;
            }

            if (null == t0.getCharset()) {
                throw Util.newInternal(
                    "RelDataType object should have been assigned a "
                    + "(default) charset when calling deriveType");
            } else if (!t0.getCharset().equals(t1.getCharset())) {
                return false;
            }

            if (null == t0.getCollation()) {
                throw Util.newInternal(
                    "RelDataType object should have been assigned a "
                    + "(default) collation when calling deriveType");
            } else if (!t0.getCollation().getCharset().equals(
                        t1.getCollation().getCharset())) {
                return false;
            }
        }

        return true;
    }

    /**
     * @param start zero based index
     * @param stop zero based index
     */
    public static boolean isCharTypeComparable(
        RelDataType [] argTypes,
        int start,
        int stop)
    {
        int n = stop - start + 1;
        RelDataType [] subset = new RelDataType[n];
        System.arraycopy(argTypes, start, subset, 0, n);
        return isCharTypeComparable(subset);
    }

    /**
     * @pre null != operands
     * @pre 2 <= operands.length
     */
    public static boolean isCharTypeComparable(
        SqlValidator validator,
        SqlValidator.Scope scope,
        SqlNode [] operands,
        boolean throwOnFailure)
    {
        Util.pre(null != operands, "null!=operands");
        Util.pre(2 <= operands.length, "2<=operands.length");

        if (!isCharTypeComparable(collectTypes(validator, scope, operands))) {
            if (throwOnFailure) {
                String msg = "";
                for (int i = 0; i < operands.length; i++) {
                    if (i > 0) {
                        msg += ", ";
                    }
                    msg += operands[i].toString();
                }
                throw EigenbaseResource.instance().newOperandNotComparable(msg);
            }
            return false;
        }
        return true;
    }

    /**
     * Iterates over all operands and collect their type.
     */
    public static RelDataType [] collectTypes(
        SqlValidator validator,
        SqlValidator.Scope scope,
        SqlNode [] operands)
    {
        RelDataType [] types = new RelDataType[operands.length];
        for (int i = 0; i < operands.length; i++) {
            types[i] = validator.deriveType(scope, operands[i]);
        }
        return types;
    }

    public static RelDataType [] collectTypes(RexNode [] exprs)
    {
        RelDataType [] types = new RelDataType[exprs.length];
        for (int i = 0; i < types.length; i++) {
            types[i] = exprs[i].getType();
        }
        return types;
    }

    public static RelDataType [] collectTypes(RelNode [] rels)
    {
        RelDataType [] types = new RelDataType[rels.length];
        for (int i = 0; i < types.length; i++) {
            types[i] = rels[i].getRowType();
        }
        return types;
    }

    /**
     * Helper function that recreates a given RelDataType with nullablility
     * iff any of a calls operand types are nullable.
     */
    public final static RelDataType makeNullableIfOperandsAre(
        final SqlValidator validator,
        final SqlValidator.Scope scope,
        final SqlCall call,
        RelDataType type)
    {
        for (int i = 0; i < call.operands.length; ++i) {
            if (call.operands[i] instanceof SqlSymbol) {
                continue;
            }

            RelDataType operandType =
                validator.deriveType(scope, call.operands[i]);

            if (operandType.isNullable()) {
                type =
                    validator.typeFactory.createTypeWithNullability(type, true);
                break;
            }
        }
        return type;
    }

    /**
     * Helper function that recreates a given RelDataType with nullablility
     * iff any of the {@param argTypes} are nullable.
     */
    public final static RelDataType makeNullableIfOperandsAre(
        final RelDataTypeFactory typeFactory,
        final RelDataType [] argTypes,
        RelDataType type)
    {
        for (int i = 0; i < argTypes.length; ++i) {
            if (argTypes[i].isNullable()) {
                type = typeFactory.createTypeWithNullability(type, true);
                break;
            }
        }
        return type;
    }

    public static void isCharTypeComparableThrows(RelDataType [] argTypes)
    {
        if (!isCharTypeComparable(argTypes)) {
            String msg = "";
            for (int i = 0; i < argTypes.length; i++) {
                if (i > 0) {
                    msg += ", ";
                }
                RelDataType argType = argTypes[i];
                msg += argType.toString();
            }
            throw EigenbaseResource.instance().newTypeNotComparableEachOther(msg);
        }
    }

    /**
     * @return Returns typeName.equals(type.getSqlTypeName())
     *         If typeName.equals(SqlTypeName.Any) true is always returned
     */
    public static boolean isOfSameTypeName(SqlTypeName typeName,
        RelDataType type) {
        return SqlTypeName.Any.equals(typeName) ||
               typeName.equals(type.getSqlTypeName());
    }

    /**
     * @return Returns true if any element in typeNames
     * matches type.getSqlTypeName()
     * @see {@link #isOfSameTypeName(SqlTypeName, RelDataType)}
     */
    public static boolean isOfSameTypeName(SqlTypeName[] typeNames,
        RelDataType type) {
        for (int i = 0; i < typeNames.length; i++) {
            SqlTypeName typeName = typeNames[i];
            if (isOfSameTypeName(typeName, type)) {
                return true;
            }
        }
        return false;
    }

    /**
     * @return true if type is DATE, TIME, or TIMESTAMP
     */
    public static boolean isDatetime(RelDataType type) {
        return isOfSameTypeName(SqlTypeName.datetimeTypes, type);
    }

    /**
     * @return true if type is some kind of INTERVAL
     */
    public static boolean isInterval(RelDataType type) {
        return isOfSameTypeName(SqlTypeName.timeIntervalTypes, type);
    }

    /**
     * @return true if type is in SqlTypeFamily.Character
     */
    public static boolean inCharFamily(RelDataType type)
    {
        return type.getFamily() == SqlTypeFamily.Character;
    }

    /**
     * @return true if type is in SqlTypeFamily.Character
     */
    public static boolean inCharFamily(SqlTypeName typeName)
    {
        return SqlTypeFamily.getFamilyForSqlType(typeName) ==
               SqlTypeFamily.Character;
    }

    /**
     * @return true if type is in SqlTypeFamily.Boolean
     */
    public static boolean inBooleanFamily(RelDataType type)
    {
        return type.getFamily() == SqlTypeFamily.Boolean;
    }

    /**
     * @return true if two types are in same type family
     */
    public static boolean inSameFamily(RelDataType t1, RelDataType t2)
    {
        return t1.getFamily() == t2.getFamily();
    }

    /**
     * @return true if two types are in same type family,
     * or one or the other is of type SqlTypeName.Null
     */
    public static boolean inSameFamilyOrNull(RelDataType t1, RelDataType t2)
    {
        if (t1.getSqlTypeName() == SqlTypeName.Null) {
            return true;
        }
        if (t2.getSqlTypeName() == SqlTypeName.Null) {
            return true;
        }
        return t1.getFamily() == t2.getFamily();
    }

    /**
     * @return true if type family is either character or binary
     */
    public static boolean inCharOrBinaryFamilies(RelDataType type)
    {
        return type.getFamily() == SqlTypeFamily.Character
            || type.getFamily() == SqlTypeFamily.Binary;
    }

    /**
     * @return true if type is a LOB of some kind
     */
    public static boolean isLob(RelDataType type)
    {
        // TODO jvs 9-Dec-2004:  once we support LOB types
        return false;
    }

    /**
     * @return true if type is variable width with bounded precision
     */
    public static boolean isBoundedVariableWidth(RelDataType type)
    {
        SqlTypeName typeName = type.getSqlTypeName();
        if (typeName == null) {
            return false;
        }
        switch (typeName.getOrdinal()) {
        case SqlTypeName.Varchar_ordinal:
        case SqlTypeName.Varbinary_ordinal:
            return true;
        default:
            return false;
        }
    }

    /**
     * @return true if type is numeric with exact precision
     */
    public static boolean isExactNumeric(RelDataType type)
    {
        SqlTypeName typeName = type.getSqlTypeName();
        if (typeName == null) {
            return false;
        }
        switch (typeName.getOrdinal()) {
        case SqlTypeName.Tinyint_ordinal:
        case SqlTypeName.Smallint_ordinal:
        case SqlTypeName.Integer_ordinal:
        case SqlTypeName.Bigint_ordinal:
        case SqlTypeName.Decimal_ordinal:
            return true;
        default:
            return false;
        }
    }

    /**
     * @return true if type is numeric with approximate precision
     */
    public static boolean isApproximateNumeric(RelDataType type)
    {
        SqlTypeName typeName = type.getSqlTypeName();
        if (typeName == null) {
            return false;
        }
        switch (typeName.getOrdinal()) {
        case SqlTypeName.Float_ordinal:
        case SqlTypeName.Real_ordinal:
        case SqlTypeName.Double_ordinal:
            return true;
        default:
            return false;
        }
    }


    /**
     * @return true if type is numeric
     */
    public static boolean isNumeric(RelDataType type) {
        return isExactNumeric(type) || isApproximateNumeric(type);
    }

    /**
     * Tests whether two types have the same name and structure, possibly
     * with differing modifiers.  For example, VARCHAR(1) and VARCHAR(10)
     * are considered the same, while VARCHAR(1) and CHAR(1) are
     * considered different.  Likewise, VARCHAR(1) MULTISET and
     * VARCHAR(10) MULTISET are considered the same.
     *
     * @return true if types have same name and structure
     */
    public static boolean sameNamedType(RelDataType t1, RelDataType t2)
    {
        if (t1.isStruct() || t2.isStruct()) {
            if (!t1.isStruct() || !t2.isStruct()) {
                return false;
            }
            if (t1.getFieldList().size() != t2.getFieldList().size()) {
                return false;
            }
            RelDataTypeField [] fields1 = t1.getFields();
            RelDataTypeField [] fields2 = t2.getFields();
            for (int i = 0; i < fields1.length; ++i) {
                if (!sameNamedType(fields1[i].getType(), fields2[i].getType()))
                {
                    return false;
                }
            }
            return true;
        }
        RelDataType comp1 = t1.getComponentType();
        RelDataType comp2 = t2.getComponentType();
        if ((comp1 != null) || (comp2 != null)) {
            if ((comp1 == null) || (comp2 == null)) {
                return false;
            }
            if (!sameNamedType(comp1, comp2)) {
                return false;
            }
        }
        return t1.getSqlTypeName() == t2.getSqlTypeName();
    }

    /**
     * Computes the maximum number of bytes required to represent a value of a
     * type having user-defined precision.  This computation assumes no
     * overhead such as length indicators and NUL-terminators.  Complex types
     * for which multiple representations are possible (e.g. DECIMAL or
     * TIMESTAMP) return 0.
     *
     * @param type type for which to compute storage
     *
     * @return maximum bytes, or 0 for a fixed-width type or type
     * with unknown maximum
     */
    public static int getMaxByteSize(RelDataType type)
    {
        SqlTypeName typeName = type.getSqlTypeName();

        if (typeName == null) {
            return 0;
        }

        switch (typeName.getOrdinal()) {
        case SqlTypeName.Char_ordinal:
        case SqlTypeName.Varchar_ordinal:
            return (int) Math.ceil(
                (((double) type.getPrecision())
                    * type.getCharset().newEncoder().maxBytesPerChar()));

        case SqlTypeName.Binary_ordinal:
        case SqlTypeName.Varbinary_ordinal:
            return type.getPrecision();

        case SqlTypeName.Multiset_ordinal:
            // TODO Wael Jan-24-2005: Need a better way to tell fennel this
            // number. This a very generic place and implementation details like
            // this doesnt belong here. Waiting to change this once we have
            // blob support
            return 4096;

        default:
            return 0;
        }
    }

    /**
     * @return true if type has a representation as a Java primitive
     * (ignoring nullability)
     */
    public static boolean isJavaPrimitive(RelDataType type)
    {
        SqlTypeName typeName = type.getSqlTypeName();
        if (typeName == null) {
            return false;
        }

        switch (typeName.getOrdinal()) {
        case SqlTypeName.Boolean_ordinal:
        case SqlTypeName.Tinyint_ordinal:
        case SqlTypeName.Smallint_ordinal:
        case SqlTypeName.Integer_ordinal:
        case SqlTypeName.Bigint_ordinal:
        case SqlTypeName.Float_ordinal:
        case SqlTypeName.Real_ordinal:
        case SqlTypeName.Double_ordinal:
            return true;
        default:
            return false;
        }
    }

    /**
     * Calls {@link ReturnTypeInferenceImpl#useNullableBiggest} by wrapping
     * the argTypes parameter in a {@link CallOperands.RelDataTypesCallOperands}
     * object
     */
    public static RelDataType getNullableBiggest(RelDataTypeFactory typeFactory,
        RelDataType[] argTypes)
    {
        CallOperands.RelDataTypesCallOperands types =
            new CallOperands.RelDataTypesCallOperands(argTypes);
        return ReturnTypeInferenceImpl.useNullableBiggest.getType(
                 null, null, typeFactory, types);
    }

    /**
     * Tests assignability of a value to a site.
     *
     * @param toType type of the target site
     *
     * @param fromType type of the source value
     *
     * @return true iff assignable
     */
    public static boolean canAssignFrom(
        RelDataType toType,
        RelDataType fromType)
    {
        // TODO jvs 2-Jan-2005:  handle all the other cases like
        // rows, collections, UDT's
        return toType.getFamily() == fromType.getFamily();
    }

    /**
     * Compares two types and returns true if fromType can
     * be cast to toType.
     *
     *<p>
     *
     * REVIEW jvs 17-Dec-2004:  the coerce param below shouldn't really be
     * necessary.  We're using it as a hack because
     * SqlTypeFactoryImpl.leastRestrictiveSqlType isn't complete enough
     * yet.  Once it is, this param (and the non-coerce rules of
     * SqlTypeAssignmentRules) should go away.
     *
     * @param toType target of assignment
     *
     * @param fromType source of assignment
     *
     * @param coerce if true, the SQL rules for CAST are used; if
     * false, the rules are similar to Java; e.g. you can't assign
     * short x = (int) y, and you can't assign int x = (String) z.
     *
     * @return true iff cast is legal
     */
    public static boolean canCastFrom(
        RelDataType toType,
        RelDataType fromType,
        boolean coerce)
    {
        if (toType == fromType) {
            return true;
        }
        if (toType.isStruct() || fromType.isStruct()) {
            if (toType.getSqlTypeName() == SqlTypeName.Distinct) {
                if (fromType.getSqlTypeName() == SqlTypeName.Distinct) {
                    // can't cast between different distinct types
                    return false;
                }
                return canCastFrom(
                    toType.getFields()[0].getType(),
                    fromType,
                    coerce);
            } else if (fromType.getSqlTypeName() == SqlTypeName.Distinct) {
                return canCastFrom(
                    toType,
                    fromType.getFields()[0].getType(),
                    coerce);
            } else {
                return toType.getFamily() == fromType.getFamily();
            }
        }
        RelDataType c1 = toType.getComponentType();
        if (c1 != null) {
            RelDataType c2 = fromType.getComponentType();
            if (c2 == null) {
                return false;
            }
            return canCastFrom(c1, c2, coerce);
        }
        SqlTypeName tn1 = toType.getSqlTypeName();
        SqlTypeName tn2 = fromType.getSqlTypeName();
        if ((tn1 == null) || (tn2 == null)) {
            return false;
        }
        SqlTypeAssignmentRules rules = SqlTypeAssignmentRules.instance();
        return rules.canCastFrom(tn1, tn2, coerce);
    }

    /**
     * @return the field names of a struct type
     */
    public static String[] getFieldNames(RelDataType type)
    {
        RelDataTypeField[] fields = type.getFields();
        String[] ret = new String[fields.length];
        for (int i = 0; i < fields.length; i++) {
            ret[i] = fields[i].getName();
        }
        return ret;
    }

    /**
     * @return the field types of a struct type
     */
    public static RelDataType[] getFieldTypes(RelDataType type)
    {
        RelDataTypeField[] fields = type.getFields();
        RelDataType[] ret = new RelDataType[fields.length];
        for (int i = 0; i < fields.length; i++) {
            ret[i] = fields[i].getType();
        }
        return ret;
    }

    /**
     * Flattens a record type by recursively expanding any
     * fields which are themselves record types.
     *
     * @param typeFactory factory which should produced flattened type
     *
     * @param recordType type with possible nesting
     *
     * @param flatteningMap if non-null, receives map from unflattened
     * ordinal to flattened ordinal (must have length at least
     * recordType.getFieldList().size())
     *
     * @return flattened equivalent
     */
    public static RelDataType flattenRecordType(
        RelDataTypeFactory typeFactory,
        RelDataType recordType,
        int [] flatteningMap)
    {
        if (!recordType.isStruct()) {
            return recordType;
        }
        List fieldList = new ArrayList();
        boolean nested =
            flattenFields(
                typeFactory,
                recordType.getFields(),
                fieldList,
                flatteningMap);
        if (!nested) {
            return recordType;
        }
        RelDataType [] types = new RelDataType[fieldList.size()];
        String [] fieldNames = new String[types.length];
        for (int i = 0; i < types.length; ++i) {
            RelDataTypeField field = (RelDataTypeField) fieldList.get(i);
            types[i] = field.getType();
            fieldNames[i] = field.getName() + "_" + i;
        }
        return typeFactory.createStructType(types, fieldNames);
    }

    private static boolean flattenFields(
        RelDataTypeFactory typeFactory,
        RelDataTypeField [] fields,
        List list,
        int [] flatteningMap)
    {
        boolean nested = false;
        for (int i = 0; i < fields.length; ++i) {
            if (flatteningMap != null) {
                flatteningMap[i] = list.size();
            }
            if (fields[i].getType().isStruct()) {
                nested = true;
                flattenFields(
                    typeFactory,
                    fields[i].getType().getFields(),
                    list,
                    null);
            } else if (fields[i].getType().getComponentType() != null) {
                // TODO jvs 14-Feb-2005:  generalize to any kind of
                // collection type
                RelDataType flattenedCollectionType =
                    typeFactory.createMultisetType(
                        flattenRecordType(
                            typeFactory,
                            fields[i].getType().getComponentType(),
                            null),
                        -1);
                RelDataTypeField field = new RelDataTypeFieldImpl(
                    fields[i].getName(),
                    fields[i].getIndex(),
                    flattenedCollectionType);
                list.add(field);
            } else {
                list.add(fields[i]);
            }
        }
        return nested;
    }

    /**
     * Converts an instance of RelDataType to an instance of SqlDataTypeSpec.
     *
     * @param type type descriptor
     *
     * @return corresponding parse representation
     */
    public static SqlDataTypeSpec convertTypeToSpec(RelDataType type)
    {
        SqlTypeName typeName = type.getSqlTypeName();

        // TODO jvs 28-Dec-2004:  support row types, user-defined types,
        // interval types, multiset types, etc
        assert(typeName != null);
        SqlIdentifier typeIdentifier = new SqlIdentifier(
            typeName.getName(), null);

        String charSetName = null;

        if (inCharFamily(type)) {
            charSetName = type.getCharset().name();
            // TODO jvs 28-Dec-2004:  collation
        }

        // REVIEW jvs 28-Dec-2004:  discriminate between precision/scale
        // zero and unspecified?

        if (typeName.allowsScale()) {
            return new SqlDataTypeSpec(
                typeIdentifier,
                type.getPrecision(),
                type.getScale(),
                charSetName,
                null);
        } else if (typeName.allowsPrec()) {
            return new SqlDataTypeSpec(
                typeIdentifier,
                type.getPrecision(),
                0,
                charSetName,
                null);
        } else {
            return new SqlDataTypeSpec(
                typeIdentifier,
                0,
                0,
                charSetName,
                null);
        }
    }

    public static RelDataType createMultisetType(RelDataTypeFactory typeFactory,
                                             RelDataType type,
                                             boolean nullable) {
        RelDataType ret = typeFactory.createMultisetType(type, -1);
        return typeFactory.createTypeWithNullability(ret, nullable);
    }
}


// End ValidationUtil.java
