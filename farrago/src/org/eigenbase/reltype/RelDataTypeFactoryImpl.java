/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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
package org.eigenbase.reltype;

import java.lang.reflect.*;

import java.nio.charset.*;

import java.util.*;

import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;


/**
 * Abstract base for implementations of {@link RelDataTypeFactory}.
 *
 * @author jhyde
 * @version $Id$
 * @since May 31, 2003
 */
public abstract class RelDataTypeFactoryImpl
    implements RelDataTypeFactory
{

    //~ Instance fields --------------------------------------------------------

    private HashMap map = new HashMap();

    //~ Constructors -----------------------------------------------------------

    protected RelDataTypeFactoryImpl()
    {
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelDataTypeFactory
    public RelDataType createJavaType(Class clazz)
    {
        return canonize(new JavaType(clazz));
    }

    // implement RelDataTypeFactory
    public RelDataType createJoinType(RelDataType [] types)
    {
        final RelDataType [] flattenedTypes = getTypeArray(types);
        return
            canonize(
                new RelCrossType(
                    flattenedTypes,
                    getFieldArray(flattenedTypes)));
    }

    // implement RelDataTypeFactory
    public RelDataType createStructType(
        RelDataType [] types,
        String [] fieldNames)
    {
        final RelDataTypeField [] fields = new RelDataTypeField[types.length];
        for (int i = 0; i < fields.length; i++) {
            fields[i] = new RelDataTypeFieldImpl(fieldNames[i], i, types[i]);
        }
        return canonize(new RelRecordType(fields));
    }

    // implement RelDataTypeFactory
    public RelDataType createStructType(List<RelDataType> typeList,
        List<String> fieldNameList)
    {
        final RelDataTypeField [] fields =
            new RelDataTypeField[typeList.size()];
        for (int i = 0; i < fields.length; i++) {
            fields[i] =
                new RelDataTypeFieldImpl(
                    fieldNameList.get(i),
                    i,
                    typeList.get(i));
        }
        return canonize(new RelRecordType(fields));
    }

    // implement RelDataTypeFactory
    public RelDataType createStructType(
        RelDataTypeFactory.FieldInfo fieldInfo)
    {
        final int fieldCount = fieldInfo.getFieldCount();
        final RelDataTypeField [] fields = new RelDataTypeField[fieldCount];
        for (int i = 0; i < fields.length; i++) {
            fields[i] =
                new RelDataTypeFieldImpl(
                    fieldInfo.getFieldName(i),
                    i,
                    fieldInfo.getFieldType(i));
        }
        return canonize(new RelRecordType(fields));
    }

    // implement RelDataTypeFactory
    public RelDataType leastRestrictive(RelDataType [] types)
    {
        assert (types != null);
        assert (types.length >= 1);
        RelDataType type0 = types[0];
        if (type0.isStruct()) {
            return leastRestrictiveStructuredType(types);
        }
        return null;
    }

    protected RelDataType leastRestrictiveStructuredType(RelDataType [] types)
    {
        RelDataType type0 = types[0];
        int nFields = type0.getFieldList().size();

        // precheck that all types are structs with same number of fields
        for (int i = 0; i < types.length; ++i) {
            if (!types[i].isStruct()) {
                return null;
            }
            if (types[i].getFieldList().size() != nFields) {
                return null;
            }
        }

        // recursively compute column-wise least restrictive
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
        return createStructType(outputTypes, fieldNames);
    }

    // copy a non-record type, setting nullability
    private RelDataType copySimpleType(
        RelDataType type,
        boolean nullable)
    {
        if (type instanceof JavaType) {
            JavaType javaType = (JavaType) type;
            if (SqlTypeUtil.inCharFamily(javaType)) {
                return
                    new JavaType(
                        javaType.clazz,
                        nullable,
                        javaType.charset,
                        javaType.collation);
            } else {
                return new JavaType(
                        javaType.clazz,
                        nullable);
            }
        } else {
            // REVIEW: RelCrossType if it stays around; otherwise get rid of
            // this comment
            return type;
        }
    }

    // recursively copy a record type
    private RelDataType copyRecordType(
        final RelRecordType type,
        final boolean ignoreNullable,
        final boolean nullable)
    {
        // REVIEW: angel 18-Aug-2005 dtbug336
        // Shouldn't null refer to the nullability of the record type
        // not the individual field types?
        // For flattening and outer joins, it is desirable to change
        // the nullability of the individual fields.

        return createStructType(new FieldInfo() {
                    public int getFieldCount()
                    {
                        return type.getFieldList().size();
                    }

                    public String getFieldName(int index)
                    {
                        return type.getFields()[index].getName();
                    }

                    public RelDataType getFieldType(int index)
                    {
                        RelDataType fieldType =
                            type.getFields()[index].getType();

                        if (ignoreNullable) {
                            return copyType(fieldType);
                        } else {
                            return
                                createTypeWithNullability(fieldType, nullable);
                        }
                    }
                });
    }

    // implement RelDataTypeFactory
    public RelDataType copyType(RelDataType type)
    {
        if (type instanceof RelRecordType) {
            return copyRecordType((RelRecordType) type, true, false);
        } else {
            return createTypeWithNullability(
                    type,
                    type.isNullable());
        }
    }

    // implement RelDataTypeFactory
    public RelDataType createTypeWithNullability(
        final RelDataType type,
        final boolean nullable)
    {
        RelDataType newType;
        if (type instanceof RelRecordType) {
            // REVIEW: angel 18-Aug-2005 dtbug 336 workaround
            // Changed to ignore nullable parameter if nullable is false since
            // copyRecordType implementation is doubtful
            if (nullable) {
                // Do a deep copy, setting all fields of the record type
                // to be nullable regardless of initial nullability
                newType = copyRecordType((RelRecordType) type, false, true);
            } else {
                // Keep same type as before, ignore nullable parameter
                // RelRecordType currently always returns a nullability of false
                newType = copyRecordType((RelRecordType) type, true, false);
            }
        } else {
            newType = copySimpleType(type, nullable);
        }
        return canonize(newType);
    }

    /**
     * Registers a type, or returns the existing type if it is already
     * registered.
     */
    protected RelDataType canonize(RelDataType type)
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
        return
            (RelDataTypeField []) fieldList.toArray(
                new RelDataTypeField[fieldList.size()]);
    }

    /**
     * Returns an array of all atomic types in an array.
     */
    private static RelDataType [] getTypeArray(RelDataType [] types)
    {
        ArrayList typeList = new ArrayList();
        getTypeArray(types, typeList);
        return
            (RelDataType []) typeList.toArray(new RelDataType[typeList.size()]);
    }

    private static void getTypeArray(
        RelDataType [] types,
        ArrayList typeList)
    {
        for (int i = 0; i < types.length; i++) {
            RelDataType type = types[i];
            if (type instanceof RelCrossType) {
                getTypeArray(((RelCrossType) type).types, typeList);
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
        if (type instanceof RelCrossType) {
            final RelCrossType crossType = (RelCrossType) type;
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
                new RelDataTypeFieldImpl(
                    field.getName(),
                    list.size(),
                    createJavaType(field.getType())));
        }

        if (list.isEmpty()) {
            return null;
        }

        return
            (RelDataTypeField []) list.toArray(
                new RelDataTypeField[list.size()]);
    }

    // implement RelDataTypeFactory
    public RelDataType createArrayType(
        RelDataType elementType,
        long maxCardinality)
    {
        if (elementType instanceof JavaType) {
            JavaType javaType = (JavaType) elementType;
            Class arrayClass = Array.newInstance(javaType.clazz, 0).getClass();
            return createJavaType(arrayClass);
        }
        throw Util.newInternal("array of non-Java type unsupported");
    }

    //~ Inner Classes ----------------------------------------------------------

    // TODO jvs 13-Dec-2004:  move to OJTypeFactoryImpl?
    /**
     * Type which is based upon a Java class.
     */
    protected class JavaType
        extends RelDataTypeImpl
    {
        private final Class clazz;
        private boolean isNullable;
        private SqlCollation collation;
        private Charset charset;

        public JavaType(Class clazz)
        {
            super(fieldsOf(clazz));
            this.clazz = clazz;

            isNullable =
                clazz.equals(Integer.class)
                || clazz.equals(Long.class)
                || clazz.equals(Short.class)
                || clazz.equals(Integer.class)
                || clazz.equals(Byte.class)
                || clazz.equals(Double.class)
                || clazz.equals(Float.class)
                || clazz.equals(Boolean.class)
                || clazz.equals(byte [].class)
                || clazz.equals(String.class);
            computeDigest();
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
                SqlTypeUtil.inCharFamily(this),
                "Need to be a chartype");
            this.isNullable = nullable;
            this.charset = charset;
            this.collation = collation;
        }

        public Class getJavaClass()
        {
            return clazz;
        }

        public boolean isNullable()
        {
            return isNullable;
        }

        protected void generateTypeString(StringBuffer sb, boolean withDetail)
        {
            sb.append("JavaType(");
            sb.append(clazz);
            sb.append(")");
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

        public Charset getCharset()
            throws RuntimeException
        {
            return this.charset;
        }

        public SqlCollation getCollation()
            throws RuntimeException
        {
            return this.collation;
        }

        public SqlTypeName getSqlTypeName()
        {
            return JavaToSqlTypeConversionRules.instance().lookup(clazz);
        }
    }
}

// End RelDataTypeFactoryImpl.java
