/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2002-2004 Disruptive Tech
// Copyright (C) 2003-2004 John V. Sichi
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

import java.io.PrintWriter;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.eigenbase.sql.SqlCollation;
import org.eigenbase.sql.SqlIntervalQualifier;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.util.Util;
import org.eigenbase.oj.util.*;


/**
 * Skeletal implementation of {@link RelDataTypeFactory}.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since May 31, 2003
 */
public class RelDataTypeFactoryImpl implements RelDataTypeFactory
{
    //~ Static fields/initializers --------------------------------------------

    private static ThreadLocal threadInstances = new ThreadLocal();

    //~ Instance fields -------------------------------------------------------

    private HashMap map = new HashMap();

    //~ Constructors ----------------------------------------------------------

    public RelDataTypeFactoryImpl()
    {
    }

    //~ Methods ---------------------------------------------------------------

    public static void setThreadInstance(RelDataTypeFactory typeFactory)
    {
        threadInstances.set(typeFactory);
    }

    public RelDataType createJavaType(Class clazz)
    {
        return canonize(new JavaType(clazz));
    }

    public RelDataType createJoinType(RelDataType [] types)
    {
        final RelDataType [] flattenedTypes = getTypeArray(types);
        return canonize(new CrossType(flattenedTypes));
    }

    public RelDataType createProjectType(
        RelDataType [] types,
        String [] fieldNames)
    {
        final RelDataTypeField [] fields = new RelDataTypeField[types.length];
        for (int i = 0; i < fields.length; i++) {
            fields[i] = new FieldImpl(fieldNames[i], i, types[i]);
        }
        return canonize(new RecordType(fields));
    }

    public RelDataType createProjectType(
        RelDataTypeFactory.FieldInfo fieldInfo)
    {
        final int fieldCount = fieldInfo.getFieldCount();
        final RelDataTypeField [] fields = new RelDataTypeField[fieldCount];
        for (int i = 0; i < fields.length; i++) {
            fields[i] =
                new FieldImpl(
                    fieldInfo.getFieldName(i),
                    i,
                    fieldInfo.getFieldType(i));
        }
        return canonize(new RecordType(fields));
    }

    public RelDataType getComponentType(RelDataType type)
    {
        return null;
    }

    public RelDataType leastRestrictive(RelDataType [] types)
    {
        assert (types != null);
        assert (types.length >= 1);
        RelDataType type0 = types[0];
        if (type0.isProject()) {
            // recursively compute column-wise least restrictive
            int nFields = type0.getFieldCount();
            RelDataType [] inputTypes = new RelDataType[types.length];
            RelDataType [] outputTypes = new RelDataType[nFields];
            String [] fieldNames = new String[nFields];
            for (int j = 0; j < nFields; ++j) {
                // REVIEW jvs 22-Jan-2004:  Always use the field name from the
                // first type?
                fieldNames[j] = type0.getFields()[j].getName();
                for (int i = 0; i < types.length; ++i) {
                    inputTypes[i] = types[i].getFields()[j].getType();
                }
                outputTypes[j] = leastRestrictive(inputTypes);
            }
            return createProjectType(outputTypes, fieldNames);
        } else {
            // REVIEW jvs 1-Mar-2004: I adapted this from
            // SqlOperatorTable.useNullableBiggest to keep tests happy.  But at
            // some point need to pull up Farrago's implementation instead, at
            // least for SQL types.
            for (int i = 1; i < types.length; i++) {
                // For now, we demand that types are assignable, and assume
                // some asymmetry in the definition of isAssignableFrom.
                RelDataType type = types[i];
                RelDataType nullType =
                    type.getFactory().createSqlType(SqlTypeName.Null);
                if (type.equals(nullType)) {
                    continue;
                }

                if (type.isAssignableFrom(type0, false)) {
                    type0 = type;
                } else {
                    if (!type0.isAssignableFrom(type, false)) {
                        return null;
                    }
                }
            }
            return type0;
        }
    }

    private RelDataType copyMultisetType(RelDataType type, boolean nullable) {
        MultisetSqlType mt = (MultisetSqlType) type;
        RelDataType elementType = copyType(mt.getComponentType());
        return new MultisetSqlType(elementType, nullable);
    }

    private RelDataType copyIntervalType(RelDataType type, boolean nullable) {
        IntervalSqlType it = (IntervalSqlType) type;
        return new IntervalSqlType(it.getIntervalQualifer(), nullable);
    }

    // copy a non-record type, setting nullability
    private RelDataType copySimpleType(
        RelDataType type,
        boolean nullable)
    {
        if (type instanceof SqlType) {
            SqlType sqlType = (SqlType) type;
            return sqlType.createWithNullability(nullable);
        } else if (type instanceof JavaType) {
            JavaType javaType = (JavaType) type;
            if (javaType.isCharType()) {
                return new JavaType(javaType.clazz, nullable,
                    javaType.charset, javaType.collation);
            } else {
                return new JavaType(javaType.clazz, nullable);
            }
        } else {
            // REVIEW: CrossType if it stays around; otherwise get rid of this
            // comment
            return type;
        }
    }

    // Recursively copy a record type.
    // This is distinct from copyRecordTypeWithNullability() below simply
    // because the recurrence is different.
    private RelDataType copyRecordType(final RecordType type)
    {
        return createProjectType(
            new FieldInfo() {
                public int getFieldCount()
                {
                    return type.getFieldCount();
                }

                public String getFieldName(int index)
                {
                    return type.getFields()[index].getName();
                }

                public RelDataType getFieldType(int index)
                {
                    RelDataType fieldType = type.getFields()[index].getType();
                    return copyType(fieldType); // recur
                }
            });
    }

    // recursively copy a record type, setting nullability
    private RelDataType copyRecordTypeWithNullability(
        final RecordType type,
        final boolean nullable)
    {
        return createProjectType(
            new FieldInfo() {
                public int getFieldCount()
                {
                    return type.getFieldCount();
                }

                public String getFieldName(int index)
                {
                    return type.getFields()[index].getName();
                }

                public RelDataType getFieldType(int index)
                {
                    RelDataType fieldType = type.getFields()[index].getType();
                    return createTypeWithNullability(fieldType, nullable); // recur
                }
            });
    }

    // implement RelDataType
    public RelDataType createTypeWithNullability(
        final RelDataType type,
        final boolean nullable)
    {
        if (type instanceof RecordType) {
            return copyRecordTypeWithNullability((RecordType) type, nullable);
        } else if (type instanceof MultisetSqlType) {
            return copyMultisetType(type, nullable);
        } else if (type instanceof IntervalSqlType) {
            return copyIntervalType(type, nullable);
        } else {
            return copySimpleType(type, nullable);
        }
    }

    // implement RelDataType
    public RelDataType copyType(RelDataType type)
    {
        if (type instanceof RecordType) {
            return copyRecordType((RecordType) type);
        } else if (type instanceof MultisetSqlType) {
            return copyMultisetType(type, type.isNullable());
        } else if (type instanceof IntervalSqlType) {
            return copyIntervalType(type, type.isNullable());
        } else {
            return copySimpleType(
                type,
                type.isNullable());
        }
    }

    public RelDataType createTypeWithCharsetAndCollation(
        RelDataType type,
        Charset charset,
        SqlCollation collation)
    {
        Util.pre(type.isCharType() == true, "Not a chartype");
        Util.pre(charset != null, "charset!=null");
        Util.pre(collation != null, "collation!=null");
        if (type instanceof SqlType) {
            SqlType sqlType = (SqlType) type;
            return sqlType.createWithCharsetAndCollation(charset, collation);
        } else if (type instanceof JavaType) {
            JavaType javaType = (JavaType) type;
            return new JavaType(
                javaType.clazz,
                javaType.isNullable(),
                charset,
                collation);
        }
        throw Util.needToImplement("need to implement " + type);
    }

    public static RelDataTypeFactory threadInstance()
    {
        return (RelDataTypeFactory) threadInstances.get();
    }

    /**
     * Registers a type, or returns the existing type if it is already
     * registered.
     */
    protected synchronized RelDataType canonize(RelDataType type)
    {
        RelDataType type2 = (RelDataType) map.get(type);
        if (type2 != null) {
            return type2;
        } else {
            map.put(type, type);
            return type;
        }
    }

    /**
     * Returns an array of the fields in an array of types.
     */
    private static RelDataTypeField [] getFieldArray(RelDataType [] types)
    {
        ArrayList fieldList = new ArrayList();
        for (int i = 0; i < types.length; i++) {
            RelDataType type = types[i];
            addFields(type, fieldList);
        }
        return (RelDataTypeField []) fieldList.toArray(
            new RelDataTypeField[fieldList.size()]);
    }

    /**
     * Returns an array of all atomic types in an array.
     */
    private static RelDataType [] getTypeArray(RelDataType [] types)
    {
        ArrayList typeList = new ArrayList();
        getTypeArray(types, typeList);
        return (RelDataType []) typeList.toArray(
            new RelDataType[typeList.size()]);
    }

    private static void getTypeArray(
        RelDataType [] types,
        ArrayList typeList)
    {
        for (int i = 0; i < types.length; i++) {
            RelDataType type = types[i];
            if (type instanceof CrossType) {
                getTypeArray(((CrossType) type).types, typeList);
            } else {
                typeList.add(type);
            }
        }
    }

    /**
     * Adds all fields in <code>type</code> to <code>fieldList</code>.
     */
    private static void addFields(
        RelDataType type,
        ArrayList fieldList)
    {
        if (type instanceof CrossType) {
            final CrossType crossType = (CrossType) type;
            for (int i = 0; i < crossType.types.length; i++) {
                addFields(crossType.types[i], fieldList);
            }
        } else {
            RelDataTypeField [] fields = type.getFields();
            for (int j = 0; j < fields.length; j++) {
                RelDataTypeField field = fields[j];
                fieldList.add(field);
            }
        }
    }

    public static boolean isJavaType(RelDataType t)
    {
        return t instanceof JavaType;
    }

    public static boolean isSqlType(RelDataType t)
    {
        return t instanceof SqlType;
    }

    public static RelDataType createSqlTypeIgnorePrecOrScale(
        RelDataTypeFactory fac,
        SqlTypeName typeName)
    {
        if (typeName.allowsPrecScale(true, true)) {
            return fac.createSqlType(typeName, 0, 0);
        }

        if (typeName.allowsPrecNoScale()) {
            return fac.createSqlType(typeName, 0);
        }

        return fac.createSqlType(typeName);
    }

    private RelDataTypeField [] fieldsOf(Class clazz)
    {
        final Field [] fields = clazz.getFields();
        ArrayList list = new ArrayList();
        for (int i = 0; i < fields.length; i++) {
            Field field = fields[i];
            if (Modifier.isStatic(field.getModifiers())) {
                continue;
            }
            list.add(
                new FieldImpl(
                    field.getName(),
                    list.size(),
                    createJavaType(field.getType())));
        }
        return (RelDataTypeField []) list.toArray(
            new RelDataTypeField[list.size()]);
    }

    public RelDataType createSqlType(SqlTypeName typeName)
    {
        Util.pre(typeName != null, "typeName != null");
        assert(!SqlTypeName.Multiset.equals(typeName)) :
            "use createMultisetType() instead";
        return new SqlType(typeName);
    }

    public RelDataType createSqlType(
        SqlTypeName typeName,
        int length)
    {
        Util.pre(typeName != null, "typeName != null");
        Util.pre(length >= 0, "length >= 0");
        assert(!SqlTypeName.Multiset.equals(typeName)) :
            "use createMultisetType() instead";
        return new SqlType(typeName, length);
    }

    public RelDataType createSqlType(
        SqlTypeName typeName,
        int length,
        int scale)
    {
        Util.pre(typeName != null, "typeName != null");
        Util.pre(length >= 0, "length >= 0");
        assert(!SqlTypeName.Multiset.equals(typeName)) :
            "use createMultisetType() instead";
        return new SqlType(typeName, length, scale);
    }

    public RelDataType createMultisetType(RelDataType type) {
        return new MultisetSqlType(type, true);
    }

    public RelDataType createIntervalType(
        SqlIntervalQualifier intervalQualifier) {
        return new IntervalSqlType(intervalQualifier, true);
    }

    /**
     * Returns the maximum number of bytes storage required by each character
     * in a given character set. Currently always returns 1.
     *
     * @param charsetName Name of the character set
     * @return
     */
    public static int getMaxBytesPerChar(String charsetName)
    {
        assert charsetName.equals("ISO-8859-1") : charsetName;
        return 1;
    }

    /**
     * @pre to!=null && from !=null
     * @param to
     * @param from
     * @param coerce
     */
    public boolean assignableFrom(
        SqlTypeName to,
        SqlTypeName from,
        boolean coerce)
    {
        Util.pre((to != null) && (from != null), "to!=null && from !=null");

        AssignableFromRules assignableFromRules =
            AssignableFromRules.instance();
        return assignableFromRules.isAssignableFrom(to, from, coerce);
    }

    //~ Inner Classes ---------------------------------------------------------

    /**
     * Abstract implementation for {@link RelDataType}. Useful if the type
     * contains a set of fields.
     *
     * <p>
     * Identity is based upon the {@link #digest} field, which each derived
     * class should set during construction.
     * </p>
     */
    protected abstract class TypeImpl implements RelDataType
    {
        protected final RelDataTypeField [] fields;
        protected String digest;

        protected TypeImpl(RelDataTypeField [] fields)
        {
            this.fields = fields;
        }

        public RelDataTypeFactory getFactory()
        {
            return RelDataTypeFactoryImpl.this;
        }

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

        public int getFieldCount()
        {
            return fields.length;
        }

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

        public RelDataTypeField [] getFields()
        {
            return fields;
        }

        public RelDataType getComponentType()
        {
            // this is not a collection type
            return null;
        }

        public RelDataType getArrayType()
        {
            throw Util.needToImplement(this);
        }

        public boolean isJoin()
        {
            throw Util.newInternal("todo: remove isJoin");

            //return false;
        }

        public RelDataType [] getJoinTypes()
        {
            assert (isJoin());
            throw Util.newInternal("not reached");
        }

        public boolean isProject()
        {
            return false;
        }

        public boolean equals(Object obj)
        {
            if (obj instanceof TypeImpl) {
                final TypeImpl that = (TypeImpl) obj;
                return this.digest.equals(that.digest);
            }
            return false;
        }

        public int hashCode()
        {
            return digest.hashCode();
        }

        public String toString()
        {
            return digest;
        }

        public String getFullTypeString()
        {
            return digest;
        }

        protected abstract String computeDigest();

        public boolean equalsSansNullability(RelDataType type)
        {
            return equals(type);
        }

        public boolean isNullable()
        {
            return false;
        }

        public void format(
            Object value,
            PrintWriter pw)
        {
            if (value == null) {
                pw.print("<null>");
            } else {
                pw.print(value);
            }
        }

        public boolean isAssignableFrom(
            RelDataType t,
            boolean coerce)
        {
            return false;
        }

        public boolean isSameType(RelDataType t)
        {
            throw Util.needToImplement("need to implement");
        }

        public boolean isSameTypeFamily(RelDataType t)
        {
            return false;
        }

        public Charset getCharset()
        {
            throw Util.needToImplement("need to implement");
        }

        public SqlCollation getCollation()
            throws RuntimeException
        {
            throw Util.needToImplement("need to implement");
        }

        public boolean isCharType()
        {
            return false;
        }

        public int getMaxBytesStorage()
        {
            return -1;
        }

        public int getPrecision()
        {
            throw Util.needToImplement("need to implement");
        }

        public SqlTypeName getSqlTypeName()
        {
            throw Util.needToImplement("need to implement");
        }
    }

    /**
     * Type of the cartesian product of two or more sets of records.
     *
     * <p>
     * Its fields are those of its constituent records, but unlike a {@link
     * RelDataTypeFactoryImpl.RecordType}, those fields' names are not
     * necessarily distinct.
     * </p>
     */
    protected class CrossType extends TypeImpl
    {
        public final RelDataType [] types;

        /**
         * Creates a cartesian product type.
         *
         * @pre types != null
         * @pre types.length >= 1
         * @pre !(types[i] instanceof CrossType)
         */
        public CrossType(RelDataType [] types)
        {
            super(getFieldArray(types));
            this.types = types;
            assert (types != null);
            assert (types.length >= 1);
            for (int i = 0; i < types.length; i++) {
                assert (!(types[i] instanceof CrossType));
            }
            this.digest = computeDigest();
        }

        public RelDataTypeField getField(String fieldName)
        {
            throw new UnsupportedOperationException(
                "not applicable to a join type");
        }

        public int getFieldCount()
        {
            throw new UnsupportedOperationException(
                "not applicable to a join type");
        }

        public int getFieldOrdinal(String fieldName)
        {
            final int ordinal = OJSyntheticClass.getOrdinal(fieldName, false);
            if (ordinal >= 0) {
                return ordinal;
            }
            throw new UnsupportedOperationException(
                "not applicable to a join type");
        }

        public RelDataTypeField [] getFields()
        {
            throw new UnsupportedOperationException(
                "not applicable to a join type");
        }

        public boolean isJoin()
        {
            //return true;
            throw Util.newInternal("todo: remove isJoin");
        }

        public RelDataType [] getJoinTypes()
        {
            assert (isJoin());
            return types;
        }

        protected String computeDigest()
        {
            final StringBuffer sb = new StringBuffer();
            sb.append("CrossType(");
            for (int i = 0; i < types.length; i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                RelDataType type = types[i];
                sb.append(type.getFullTypeString());
            }
            sb.append(")");
            return sb.toString();
        }
    }

    /**
     * Simple implementation of {@link RelDataTypeField}
     */
    protected static class FieldImpl implements RelDataTypeField
    {
        private final RelDataType type;
        private final String name;
        private final int index;

        /**
         * @pre name != null
         * @pre type != null
         */
        public FieldImpl(
            String name,
            int index,
            RelDataType type)
        {
            assert (name != null);
            assert (type != null);
            this.name = name;
            this.index = index;
            this.type = type;
        }

        public String getName()
        {
            return name;
        }

        public int getIndex()
        {
            return index;
        }

        public RelDataType getType()
        {
            return type;
        }

        public Object get(Object o)
        {
            throw new UnsupportedOperationException();
        }

        public void set(
            Object o,
            Object value)
        {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Type which is a set of named fields.
     */
    protected class RecordType extends TypeImpl
    {
        /**
         * Creates a <code>RecordType</code>.
         * Field names doesnt need to be unique.
         */
        RecordType(RelDataTypeField [] fields)
        {
            super(fields);
            this.digest = computeDigest();
        }

        public boolean isProject()
        {
            return true;
        }

        public boolean isNullable()
        {
            // REVIEW:  maybe shouldn't even bother.  SQL structured types have
            // a nullable status independent of that of their fields.
            for (int i = 0; i < fields.length; ++i) {
                if (fields[i].getType().isNullable()) {
                    return true;
                }
            }
            return false;
        }

        protected String computeDigest()
        {
            final StringBuffer sb = new StringBuffer();
            sb.append("RecordType(");
            for (int i = 0; i < fields.length; i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                RelDataTypeField field = fields[i];
                sb.append(field.getType().getFullTypeString() + " "
                    + field.getName());
            }
            sb.append(")");
            return sb.toString();
        }

        public boolean isSameType(RelDataType t) {
            if (t instanceof RecordType) {
                RecordType that = (RecordType) t;
                if (that.fields.length!=this.fields.length) {
                    return false;
                }

                boolean ret = true;
                for (int i = 0; i < this.fields.length; i++) {
                    ret = ret && this.fields[i].getType().isSameType(
                        that.fields[i].getType());
                }
                return ret;
            }
            return false;
        }

        public boolean isSameTypeFamily(RelDataType t) {
            return isSameType(t);
        }
    }

    /**
     * Type which is based upon a Java class.
     *
     * <p>TODO: Make protected. (jhyde, 2004/5/26)
     */
    public class JavaType extends TypeImpl
    {
        public final Class clazz;
        private boolean isNullable;
        private SqlCollation collation;
        private Charset charset;

        public JavaType(Class clazz)
        {
            super(fieldsOf(clazz));
            this.clazz = clazz;
            this.digest = computeDigest();

            isNullable =
                clazz.equals(Integer.class) || clazz.equals(int.class)
                    || clazz.equals(Long.class) || clazz.equals(long.class)
                    || clazz.equals(Integer.class) || clazz.equals(int.class)
                    || clazz.equals(Byte.class) || clazz.equals(byte.class)
                    || clazz.equals(Double.class)
                    || clazz.equals(double.class)
                    || clazz.equals(Boolean.class)
                    || clazz.equals(boolean.class)
                    || clazz.equals(byte [].class)
                    || clazz.equals(String.class);
        }

        public JavaType(
            Class clazz,
            boolean nullable)
        {
            this(clazz);
            this.isNullable = nullable;
        }

        public JavaType(
            Class clazz,
            boolean nullable,
            Charset charset,
            SqlCollation collation)
        {
            this(clazz);
            Util.pre(
                isCharType(),
                "Need to be a chartype");
            this.isNullable = nullable;
            this.charset = charset;
            this.collation = collation;
        }

        public boolean isNullable()
        {
            return isNullable;
        }

        public boolean isAssignableFrom(
            RelDataType t,
            boolean coerce)
        {
            if (!(t instanceof JavaType) && !(t instanceof SqlType)) {
                return false;
            }
            SqlTypeName thisSqlTypeName = getSqlTypeName();
            if (null == thisSqlTypeName) {
                return false; //REVIEW wael 8/05/2004: shouldnt we assert instead?
            }
            SqlTypeName thatSqlTypeName;

            if (t instanceof SqlType) {
                thatSqlTypeName = ((SqlType) t).getTypeName();
            } else {
                thatSqlTypeName =
                    JavaToSqlTypeConversionRules.instance().lookup(t);
                if (null == thatSqlTypeName) {
                    return false;
                }
            }

            RelDataTypeFactory fac = getFactory();
            RelDataType thisType =
                createSqlTypeIgnorePrecOrScale(fac, thisSqlTypeName);
            RelDataType thatType =
                createSqlTypeIgnorePrecOrScale(fac, thatSqlTypeName);
            return thisType.isAssignableFrom(thatType, coerce);
        }

        public boolean isSameType(RelDataType t)
        {
            return t instanceof JavaType && clazz.equals(((JavaType) t).clazz);
        }

        public boolean isSameTypeFamily(RelDataType t)
        {
            //TODO implement real rules
            return isSameType(t);
        }

        protected String computeDigest()
        {
            return "JavaType(" + clazz + ")";
        }

        public RelDataType getArrayType()
        {
            final Class arrayClass = Array.newInstance(clazz, 0).getClass();
            return createJavaType(arrayClass);
        }

        public RelDataType getComponentType()
        {
            final Class componentType = clazz.getComponentType();
            if (componentType == null) {
                return null;
            } else {
                return createJavaType(componentType);
            }
        }

        public void format(
            Object value,
            PrintWriter pw)
        {
            if (value == null) {
                pw.print("null");
            } else if (String.class.isAssignableFrom(clazz)) {
                Util.printJavaString(pw, (String) value, true);
            } else {
                pw.print(value);
            }
        }

        public Charset getCharset()
            throws RuntimeException
        {
            if (!isCharType()) {
                throw Util.newInternal(computeDigest()
                    + " is not defined to carry a charset");
            }
            return this.charset;
        }

        public SqlCollation getCollation()
            throws RuntimeException
        {
            if (!isCharType()) {
                throw Util.newInternal(computeDigest()
                    + " is not defined to carry a collation");
            }
            return this.collation;
        }

        public SqlTypeName getSqlTypeName()
        {
            return JavaToSqlTypeConversionRules.instance().lookup(this);
        }

        public boolean isCharType()
        {
            return clazz.equals(String.class) || clazz.equals(char.class);
        }
    }

    /**
     * SQL scalar type.
     *
     * <p>TODO: Make protected. (jhyde, 2004/5/26)
     */
    public class SqlType implements RelDataType, Cloneable
    {
        public static final int SCALE_NOT_SPECIFIED = Integer.MIN_VALUE;
        public static final int PRECISION_NOT_SPECIFIED = -1;
        private final SqlTypeName typeName;
        private boolean isNullable = true;
        private final int precision;
        private final int scale;
        private final String digest;
        private SqlCollation collation;
        private Charset charset;

        /**
         * Constructs a type with no parameters.
         * @param typeName Type name
         * @pre typeName.allowsNoPrecNoScale(false,false)
         */
        SqlType(SqlTypeName typeName)
        {
            Util.pre(
                typeName.allowsPrecScale(false, false),
                "typeName.allowsPrecScale(false,false), typeName="
                + typeName.name);
            this.typeName = typeName;
            this.precision = PRECISION_NOT_SPECIFIED;
            this.scale = SCALE_NOT_SPECIFIED;
            this.digest = typeName.name;
        }

        /**
         * Constructs a type with precision/length but no scale.
         * @param typeName Type name
         * @pre typeName.allowsPrecNoScale(true,false)
         */
        SqlType(
            SqlTypeName typeName,
            int precision)
        {
            Util.pre(
                typeName.allowsPrecScale(true, false),
                "typeName.allowsPrecScale(true,false)");
            this.typeName = typeName;
            this.precision = precision;
            this.scale = SCALE_NOT_SPECIFIED;
            this.digest = typeName.name + "(" + precision + ")";
        }

        /**
         * Constructs a type with precision/length and scale.
         * @param typeName Type name
         * @pre typeName.allowsPrecScale(true,true)
         */
        SqlType(
            SqlTypeName typeName,
            int precision,
            int scale)
        {
            Util.pre(
                typeName.allowsPrecScale(true, true),
                "typeName.allowsPrecScale(true,true)");
            this.typeName = typeName;
            this.precision = precision;
            this.scale = scale;
            this.digest =
                typeName.name + "(" + precision + ", " + scale + ")";
        }

        /**
         * Constructs a type with nullablity
         */
        public SqlType createWithNullability(boolean nullable)
        {
            SqlType ret = null;
            try {
                ret = (SqlType) this.clone();
            } catch (CloneNotSupportedException e) {
                throw Util.newInternal(e);
            }
            ret.isNullable = nullable;
            return ret;
        }

        /**
         * Constructs a typ charset and collation
         * @pre isCharType == true
         */
        public SqlType createWithCharsetAndCollation(
            Charset charset,
            SqlCollation collation)
        {
            Util.pre(this.isCharType() == true, "Not an chartype");
            SqlType ret;
            try {
                ret = (SqlType) this.clone();
            } catch (CloneNotSupportedException e) {
                throw Util.newInternal(e);
            }
            ret.charset = charset;
            ret.collation = collation;
            return ret;
        }

        //implement RelDataType
        public int getPrecision()
        {
            return precision;
        }

        public SqlTypeName getTypeName()
        {
            return typeName;
        }

        public String toString()
        {
            return digest;
        }

        public String getFullTypeString()
        {
            return digest;
        }

        public int hashCode()
        {
            return digest.hashCode();
        }

        public boolean equals(Object obj)
        {
            return obj instanceof SqlType
                && ((SqlType) obj).digest.equals(digest);
        }

        public boolean isSameType(RelDataType t)
        {
            if (t instanceof JavaType) {
                SqlTypeName thatSqlTypeName =
                    JavaToSqlTypeConversionRules.instance().lookup(t);
                if (null == thatSqlTypeName) {
                    return false;
                }
                return this.getSqlTypeName().equals(thatSqlTypeName);
            }
            return t instanceof SqlType
                && (((SqlType) t).typeName.getOrdinal() ==
                    this.typeName.getOrdinal());
        }

        public boolean isSameTypeFamily(RelDataType t)
        {
            //TODO Implement real rules
            return isSameType(t);
        }

        public RelDataTypeFactory getFactory()
        {
            return RelDataTypeFactoryImpl.this;
        }

        public RelDataTypeField getField(String fieldName)
        {
            throw new UnsupportedOperationException();
        }

        public int getFieldCount()
        {
            throw new UnsupportedOperationException();
        }

        public int getFieldOrdinal(String fieldName)
        {
            throw new UnsupportedOperationException();
        }

        public RelDataTypeField [] getFields()
        {
            throw new UnsupportedOperationException();
        }

        public RelDataType getComponentType()
        {
            return null;
        }

        public RelDataType getArrayType()
        {
            throw Util.needToImplement(this);
        }

        public boolean isJoin()
        {
            return false;
        }

        public RelDataType [] getJoinTypes()
        {
            throw new UnsupportedOperationException();
        }

        public boolean isProject()
        {
            return false;
        }

        public boolean equalsSansNullability(RelDataType type)
        {
            return equals(type);
        }

        public boolean isNullable()
        {
            return isNullable;
        }

        public boolean isAssignableFrom(
            RelDataType t,
            boolean coerce)
        {
            SqlTypeName thatSqlTypeName;

            AssignableFromRules assignableFromRules =
                AssignableFromRules.instance();
            if (t instanceof JavaType) {
                thatSqlTypeName =
                    JavaToSqlTypeConversionRules.instance().lookup(t);
                if (null == thatSqlTypeName) {
                    return false;
                }
                return assignableFromRules.isAssignableFrom(this.typeName,
                    thatSqlTypeName, coerce);
            } else {
                return t instanceof SqlType
                    && assignableFromRules.isAssignableFrom(this.typeName,
                        ((SqlType) t).typeName, coerce);
            }
        }

        public Charset getCharset()
            throws RuntimeException
        {
            if (!isCharType()) {
                throw Util.newInternal(typeName.toString()
                    + " is not defined to carry a charset");
            }
            return this.charset;
        }

        public SqlCollation getCollation()
            throws RuntimeException
        {
            if (!isCharType()) {
                throw Util.newInternal(typeName.toString()
                    + " is not defined to carry a collation");
            }
            return this.collation;
        }

        public boolean isCharType()
        {
            switch (typeName.getOrdinal()) {
            case SqlTypeName.Char_ordinal:
            case SqlTypeName.Varchar_ordinal:
                return true;
            default:
                return false;
            }
        }

        public int getMaxBytesStorage()
        {
            switch (typeName.getOrdinal()) {
            case SqlTypeName.Char_ordinal:
            case SqlTypeName.Varchar_ordinal:

                // FIXME (jhyde, 2004/5/26): Assumes UCS-2 encoding, 2 bytes
                // per character.
                return precision * 2;
            case SqlTypeName.Binary_ordinal:
            case SqlTypeName.Varbinary_ordinal:
                return precision;
            case SqlTypeName.Bit_ordinal:

                // One byte for each 8 bits.
                return (precision + 7) / 8;
            case SqlTypeName.Null_ordinal:

                // Null is tricky. By the time null values are used in actual
                // programs they have generally been converted to another type,
                // with an implicit cast. So CAST(NULL AS VARCHAR(3)) should
                // have a type VARCHAR(0) and require 0 bytes; but
                // CAST(NULL AS INTEGER) should require -1 bytes, because
                // INTEGER is a fixed-size type.
                return 0;
            default:
                return -1;
            }
        }

        /**
         * get the SqlTypeName for this RelDataType.
         */
        public SqlTypeName getSqlTypeName()
        {
            return this.typeName;
        }
    }


    /**
     * IntervalSqlType represents SQL builtin type of INTERVAL
     */
    public class IntervalSqlType implements RelDataType, Cloneable
    {
        private SqlIntervalQualifier intervalQualifer;
        private boolean isNullable;
        private String digest;
        private SqlTypeName typeName;

        public IntervalSqlType(
            SqlIntervalQualifier intervalQualifer,
            boolean isNullable) {
            this.isNullable = isNullable;
            this.intervalQualifer = intervalQualifer;
            typeName = intervalQualifer.isYearMonth() ?
                SqlTypeName.IntervalYearMonth :
                SqlTypeName.IntervalDayTime;
            digest = "INTERVAL "+intervalQualifer.toString();
        }

        public SqlIntervalQualifier getIntervalQualifer() {
            return intervalQualifer;
        }

        public String toString() {
            return getFullTypeString();
        }

        /**
         * Combines two IntervalTypes and returns the result.
         * E.g. the result of combining <br>
         * <code>INTERVAL DAY TO HOUR</code> <br>
         * with <br>
         * <code>INTERVAL SECOND</code> is <br>
         * <code>INTERVAL DAY TO SECOND</code>
         */
        public IntervalSqlType combine(IntervalSqlType that) {
            assert(this.intervalQualifer.isYearMonth()==
                   that.intervalQualifer.isYearMonth());
            boolean  nullable = isNullable || that.isNullable;
            SqlIntervalQualifier.TimeUnit thisStart =
                                     this.intervalQualifer.getStartUnit();
            SqlIntervalQualifier.TimeUnit thisEnd =
                                     this.intervalQualifer.getEndUnit();
            SqlIntervalQualifier.TimeUnit thatStart =
                                     that.intervalQualifer.getStartUnit();
            SqlIntervalQualifier.TimeUnit thatEnd =
                                     that.intervalQualifer.getEndUnit();

            assert(null!=thisStart);
            assert(null!=thatStart);

            int secondPrec = intervalQualifer.getStartPrecision();
            int fracPrec = Math.max(
                this.intervalQualifer.getFractionalSecondPrecision(),
                that.intervalQualifer.getFractionalSecondPrecision());

            if (thisStart.getOrdinal() > thatStart.
                getOrdinal()) {
                thisEnd = thisStart;
                thisStart = thatStart;
                secondPrec = that.intervalQualifer.getStartPrecision();
            } else if (thisStart.getOrdinal() == thatStart.getOrdinal()) {
                secondPrec = Math.max(secondPrec,
                    that.intervalQualifer.getStartPrecision());
            } else  if ((null == thisEnd) || (thisEnd.getOrdinal() < thatStart.
                getOrdinal())) {
                thisEnd = thatStart;
            }

            if (null!=thatEnd) {
                if ((null==thisEnd) ||
                    (thisEnd.getOrdinal() < thatEnd.getOrdinal())) {
                    thisEnd = thatEnd;
                }
            }

            return new IntervalSqlType(
                new SqlIntervalQualifier(
                    thisStart, secondPrec, thisEnd, fracPrec, null), nullable);
        }

        public RelDataTypeFactory getFactory() {
            return RelDataTypeFactoryImpl.this;
        }

        public RelDataTypeField getField(String fieldName) {
            return null;
        }

        public int getFieldCount() {
            return 0;
        }

        public int getFieldOrdinal(String fieldName) {
            return 0;
        }

        public RelDataTypeField[] getFields() {
            return new RelDataTypeField[0];
        }

        public boolean isJoin() {
            return false;
        }

        public RelDataType[] getJoinTypes() {
            return new RelDataType[0];
        }

        public boolean isProject() {
            return false;
        }

        public boolean equalsSansNullability(RelDataType type) {
            return isSameType(type);
        }

        public boolean isNullable() {
            return isNullable;
        }

        public RelDataType getComponentType() {
            return null;
        }

        public RelDataType getArrayType() {
            return null;
        }

        public boolean isAssignableFrom(
            RelDataType t,
            boolean coerce) {
            Util.discard(coerce);
            return isSameType(t);
        }

        public boolean isSameType(RelDataType t) {
            return getSqlTypeName().equals(t.getSqlTypeName());
        }

        public boolean isSameTypeFamily(RelDataType t) {
            return isSameType(t);
        }

        public boolean isCharType() {
            return false;
        }

        public Charset getCharset() {
            throw new RuntimeException();
        }

        public SqlCollation getCollation()
            throws RuntimeException {
            throw new RuntimeException();
        }

        public int getMaxBytesStorage() {
            return 0;
        }

        public int getPrecision() {
            return intervalQualifer.getStartPrecision();
        }

        public SqlTypeName getSqlTypeName() {
            return typeName;
        }

        public String getFullTypeString() {
            return digest;
        }

    }
    /**
     * MultisetSqlType is used to reperesent the type of the SQL builtin
     * MULTISET construct.
     */
    public class MultisetSqlType implements RelDataType, Cloneable {

        private RelDataType elementType;
        private boolean isNullable;
        private String digest;

        /**
         * @pre null!=elementType
         */
        public MultisetSqlType(RelDataType elementType, boolean isNullable) {
            Util.pre(null!=elementType,"null!=elementType");
            this.elementType = elementType;
            this.isNullable = isNullable;
            digest = elementType.toString() + " MULTISET";
        }

        public RelDataTypeFactory getFactory() {
            return RelDataTypeFactoryImpl.this;
        }

        public String toString() {
            return getFullTypeString();
        }

        public String getFullTypeString() {
            return digest;
        }

        public int hashCode()
        {
            return digest.hashCode();
        }

        public boolean equals(Object obj)
        {
            // if obj is a multiset then elementType!=null for sure since its
            //enforced in ctor.
            return (obj instanceof MultisetSqlType) &&
               ((MultisetSqlType) obj).elementType.isSameType(this.elementType);
        }

        public boolean isSameType(RelDataType t)
        {
            return equals(t);
        }

        public boolean isSameTypeFamily(RelDataType t)
        {
            return isSameType(t);
        }

        public RelDataTypeField getField(String fieldName)
        {
            throw new UnsupportedOperationException();
        }

        public int getFieldCount()
        {
            throw new UnsupportedOperationException();
        }

        public int getFieldOrdinal(String fieldName)
        {
            throw new UnsupportedOperationException();
        }

        public RelDataTypeField [] getFields()
        {
            throw new UnsupportedOperationException();
        }

        public RelDataType getComponentType() {
            return elementType;
        }

        public boolean isJoin()
        {
            return false;
        }

        public RelDataType [] getJoinTypes()
        {
            throw new UnsupportedOperationException();
        }

        public RelDataType getArrayType()
        {
            throw Util.needToImplement(this);
        }

        public boolean isProject()
        {
            return elementType.isProject();
        }

        public boolean equalsSansNullability(RelDataType type)
        {
            return equals(type);
        }

        public boolean isNullable()
        {
            return isNullable;
        }

        public boolean isAssignableFrom(
            RelDataType t,
            boolean coerce) {
            return (t instanceof MultisetSqlType) &&
                ((MultisetSqlType) t).elementType.isAssignableFrom(
                    elementType, coerce);
        }

        public boolean isCharType() {
            return false;
        }

        public Charset getCharset() {
            throw new UnsupportedOperationException();
        }

        public SqlCollation getCollation() throws RuntimeException {
            throw new UnsupportedOperationException();
        }

        public int getMaxBytesStorage() {
            throw new UnsupportedOperationException();
        }

        public SqlTypeName getSqlTypeName() {
            return SqlTypeName.Multiset;
        }

        public int getPrecision() {
            throw new UnsupportedOperationException();
        }

    }

    /**
     * Class to hold conversion rules from JavaType to SqlType
     */
    public static class JavaToSqlTypeConversionRules
    {
        private static final JavaToSqlTypeConversionRules instance =
            new JavaToSqlTypeConversionRules();
        private final HashMap rules = new HashMap();

        private JavaToSqlTypeConversionRules()
        {
            rules.put(Integer.class, SqlTypeName.Integer);
            rules.put(int.class, SqlTypeName.Integer);
            rules.put(Long.class, SqlTypeName.Bigint);
            rules.put(long.class, SqlTypeName.Bigint);
            rules.put(byte.class, SqlTypeName.Tinyint);
            rules.put(Byte.class, SqlTypeName.Tinyint);

            rules.put(Float.class, SqlTypeName.Real);
            rules.put(float.class, SqlTypeName.Real);
            rules.put(Double.class, SqlTypeName.Double);
            rules.put(double.class, SqlTypeName.Double);

            rules.put(boolean.class, SqlTypeName.Boolean);
            rules.put(byte [].class, SqlTypeName.Varbinary);
            rules.put(String.class, SqlTypeName.Varchar);

            rules.put(Date.class, SqlTypeName.Date);
            rules.put(Timestamp.class, SqlTypeName.Timestamp);
            rules.put(Time.class, SqlTypeName.Time);
        }

        /**
         * Returns the {@link org.eigenbase.util.Glossary#SingletonPattern
         * singleton} instance.
         */
        public static JavaToSqlTypeConversionRules instance()
        {
            return instance;
        }

        /**
         * Returns a (if there's one) corresponding {@link SqlTypeName}  for a given (java) class
         * @param t The Java class to lookup
         * @return a corresponding SqlTypeName if found, otherwise null is returned
         */
        public SqlTypeName lookup(RelDataType t)
        {
            JavaType javaType = (JavaType) t;
            return (SqlTypeName) rules.get(javaType.clazz);
        }
    }

    /**
     * REVIEW 7/05/04 Wael: We should split this up in
     * Cast rules, symmetric and asymmetric assignable rules
     *
     * Class to hold rules to determine if a type is assignalbe from another
     * type.
     */
    public static class AssignableFromRules
    {
        private static AssignableFromRules instance = null;
        private static HashMap rules = null;
        private static HashMap coerceRules = null;

        private AssignableFromRules()
        {
            rules = new HashMap();

            HashSet rule;

            //IntervalYearMonth is assignable from...
            rule = new HashSet();
            rule.add(SqlTypeName.IntervalYearMonth);
            rules.put(SqlTypeName.IntervalYearMonth, rule);

            //IntervalDayTime is assignable from...
            rule = new HashSet();
            rule.add(SqlTypeName.IntervalDayTime);
            rules.put(SqlTypeName.IntervalDayTime, rule);

            //Multiset is assignable from...
            rule = new HashSet();
            rule.add(SqlTypeName.Multiset);
            rules.put(SqlTypeName.Multiset, rule);

            // Tinyint is assignable from...
            rule = new HashSet();
            rule.add(SqlTypeName.Tinyint);
            rules.put(SqlTypeName.Tinyint, rule);

            // Smallint is assignable from...
            rule = new HashSet();
            rule.add(SqlTypeName.Tinyint);
            rule.add(SqlTypeName.Smallint);
            rules.put(SqlTypeName.Smallint, rule);

            // Int is assignable from...
            rule = new HashSet();
            rule.add(SqlTypeName.Smallint);
            rule.add(SqlTypeName.Integer);
            rules.put(SqlTypeName.Integer, rule);

            // BigInt is assignable from...
            rule = new HashSet();
            rule.add(SqlTypeName.Smallint);
            rule.add(SqlTypeName.Integer);
            rule.add(SqlTypeName.Bigint);
            rules.put(SqlTypeName.Bigint, rule);

            // Float is assignable from...
            rule = new HashSet();
            rule.add(SqlTypeName.Smallint);
            rule.add(SqlTypeName.Integer);
            rule.add(SqlTypeName.Bigint);
            rule.add(SqlTypeName.Decimal);
            rule.add(SqlTypeName.Float);
            rules.put(SqlTypeName.Float, rule);

            // Real is assignable from...
            rule = new HashSet();
            rule.add(SqlTypeName.Smallint);
            rule.add(SqlTypeName.Integer);
            rule.add(SqlTypeName.Bigint);
            rule.add(SqlTypeName.Decimal);
            rule.add(SqlTypeName.Float);
            rule.add(SqlTypeName.Real);
            rules.put(SqlTypeName.Real, rule);

            // Double is assignable from...
            rule = new HashSet();
            rule.add(SqlTypeName.Smallint);
            rule.add(SqlTypeName.Integer);
            rule.add(SqlTypeName.Bigint);
            rule.add(SqlTypeName.Decimal);
            rule.add(SqlTypeName.Float);
            rule.add(SqlTypeName.Real);
            rule.add(SqlTypeName.Double);
            rules.put(SqlTypeName.Double, rule);

            // Decimal is assignable from...
            rule = new HashSet();
            rule.add(SqlTypeName.Smallint);
            rule.add(SqlTypeName.Integer);
            rule.add(SqlTypeName.Bigint);
            rule.add(SqlTypeName.Real);
            rule.add(SqlTypeName.Double);
            rule.add(SqlTypeName.Decimal);
            rules.put(SqlTypeName.Decimal, rule);

            // Bit is assignable from...
            rule = new HashSet();
            rule.add(SqlTypeName.Varbinary);
            rule.add(SqlTypeName.Bit);
            rules.put(SqlTypeName.Bit, rule);

            // VarBinary is assignable from...
            rule = new HashSet();
            rule.add(SqlTypeName.Bit);
            rule.add(SqlTypeName.Varbinary);
            rules.put(SqlTypeName.Varbinary, rule);

            // Char is assignable from...
            rule = new HashSet();
            rule.add(SqlTypeName.Char);
            rules.put(SqlTypeName.Char, rule);

            // VarChar is assignable from...
            rule = new HashSet();
            rule.add(SqlTypeName.Char);
            rule.add(SqlTypeName.Varchar);
            rules.put(SqlTypeName.Varchar, rule);

            // Boolean is assignable from...
            rule = new HashSet();
            rule.add(SqlTypeName.Boolean);
            rules.put(SqlTypeName.Boolean, rule);

            // Binary is assignable from...
            rule = new HashSet();
            rule.add(SqlTypeName.Binary);
            rules.put(SqlTypeName.Binary, rule);

            // Date is assignable from ...
            rule = new HashSet();
            rule.add(SqlTypeName.Date);
            rule.add(SqlTypeName.Timestamp);
            rules.put(SqlTypeName.Date, rule);

            // Time is assignable from ...
            rule = new HashSet();
            rule.add(SqlTypeName.Time);
            rule.add(SqlTypeName.Timestamp);
            rules.put(SqlTypeName.Time, rule);

            // Timestamp is assignable from ...
            rule = new HashSet();
            rule.add(SqlTypeName.Timestamp);
            rules.put(SqlTypeName.Timestamp, rule);

            // we use coerceRules when we're casting
            coerceRules = (HashMap) rules.clone();

            // Make numbers symmetrical and
            // make varchar/char castable to/from numbers
            rule = new HashSet();
            rule.add(SqlTypeName.Tinyint);
            rule.add(SqlTypeName.Smallint);
            rule.add(SqlTypeName.Integer);
            rule.add(SqlTypeName.Bigint);
            rule.add(SqlTypeName.Decimal);
            rule.add(SqlTypeName.Float);
            rule.add(SqlTypeName.Real);
            rule.add(SqlTypeName.Double);

            rule.add(SqlTypeName.Char);
            rule.add(SqlTypeName.Varchar);

            coerceRules.put(SqlTypeName.Tinyint, rule);
            coerceRules.put(SqlTypeName.Smallint, rule);
            coerceRules.put(SqlTypeName.Integer, rule);
            coerceRules.put(SqlTypeName.Bigint, rule);
            coerceRules.put(SqlTypeName.Float, rule);
            coerceRules.put(SqlTypeName.Real, rule);
            coerceRules.put(SqlTypeName.Decimal, rule);
            coerceRules.put(SqlTypeName.Double, rule);
            coerceRules.put(SqlTypeName.Char, rule);
            coerceRules.put(SqlTypeName.Varchar, rule);

            // binary is castable from varbinary
            rule = (HashSet) coerceRules.get(SqlTypeName.Binary);
            rule.add(SqlTypeName.Varbinary);

            // varchar is castable from Date, time and timestamp and numbers
            rule = (HashSet) coerceRules.get(SqlTypeName.Varchar);
            rule.add(SqlTypeName.Date);
            rule.add(SqlTypeName.Time);
            rule.add(SqlTypeName.Timestamp);

            // char is castable from Date, time and timestamp and numbers
            rule = (HashSet) coerceRules.get(SqlTypeName.Char);
            rule.add(SqlTypeName.Date);
            rule.add(SqlTypeName.Time);
            rule.add(SqlTypeName.Timestamp);

            // Date, time, and timestamp are castable from
            // char and varchar
            rule = (HashSet) coerceRules.get(SqlTypeName.Date);
            rule.add(SqlTypeName.Char);
            rule.add(SqlTypeName.Varchar);

            rule = (HashSet) coerceRules.get(SqlTypeName.Time);
            rule.add(SqlTypeName.Char);
            rule.add(SqlTypeName.Varchar);

            rule = (HashSet) coerceRules.get(SqlTypeName.Timestamp);
            rule.add(SqlTypeName.Char);
            rule.add(SqlTypeName.Varchar);

            // for getting the milliseconds.
            rule = (HashSet) coerceRules.get(SqlTypeName.Bigint);
            rule.add(SqlTypeName.Date);
            rule.add(SqlTypeName.Time);
            rule.add(SqlTypeName.Timestamp);
        }

        public synchronized static AssignableFromRules instance()
        {
            if (null == instance) {
                instance = new AssignableFromRules();
            }
            return instance;
        }

        public boolean isAssignableFrom(
            SqlTypeName to,
            SqlTypeName from,
            boolean coerce)
        {
            HashMap ruleset = coerce ? coerceRules : rules;
            return isAssignableFrom(to, from, ruleset);
        }

        public boolean isAssignableFrom(
            SqlTypeName to,
            SqlTypeName from)
        {
            return isAssignableFrom(to, from, false);
        }

        private boolean isAssignableFrom(
            SqlTypeName to,
            SqlTypeName from,
            HashMap ruleset)
        {
            assert (null != to);
            assert (null != from);

            if (to.equals(SqlTypeName.Null)) {
                return false;
            } else if (from.equals(SqlTypeName.Null)) {
                return true;
            }

            HashSet rule = (HashSet) ruleset.get(to);
            if (null == rule) {
                //if you hit this assert, see the constructor of this class on how to add new rule
                throw Util.newInternal("No assign rules for " + to
                    + " defined");
            }

            return rule.contains(from);
        }
    }
}


// End RelDataTypeFactoryImpl.java
