/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// You must accept the terms in LICENSE.html to use this software.
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

package net.sf.saffron.core;

import net.sf.saffron.sql.type.SqlTypeName;
import net.sf.saffron.sql.SqlLiteral;
import net.sf.saffron.util.Util;
import openjava.ptree.util.SyntheticClass;

import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.lang.reflect.Array;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.math.BigInteger;


/**
 * Skeletal implementation of {@link SaffronTypeFactory}.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since May 31, 2003
 */
public class SaffronTypeFactoryImpl implements SaffronTypeFactory
{
    //~ Static fields/initializers --------------------------------------------

    private static ThreadLocal threadInstances = new ThreadLocal();

    //~ Instance fields -------------------------------------------------------

    private HashMap map = new HashMap();

    //~ Constructors ----------------------------------------------------------

    public SaffronTypeFactoryImpl()
    {
    }

    //~ Methods ---------------------------------------------------------------

    public static void setThreadInstance(SaffronTypeFactory typeFactory)
    {
        threadInstances.set(typeFactory);
    }

    public SaffronType createJavaType(Class clazz)
    {
        return canonize(new JavaType(clazz));
    }

    public SaffronType createJoinType(SaffronType [] types)
    {
        final SaffronType [] flattenedTypes = getTypeArray(types);
        return canonize(new CrossType(flattenedTypes));
    }

    public SaffronType createProjectType(
        SaffronType [] types,
        String [] fieldNames)
    {
        final SaffronField [] fields = new SaffronField[types.length];
        for (int i = 0; i < fields.length; i++) {
            fields[i] = new FieldImpl(fieldNames[i], i, types[i]);
        }
        return canonize(new RecordType(fields));
    }

    public SaffronType createProjectType(
        SaffronTypeFactory.FieldInfo fieldInfo)
    {
        final int fieldCount = fieldInfo.getFieldCount();
        final SaffronField [] fields = new SaffronField[fieldCount];
        for (int i = 0; i < fields.length; i++) {
            fields[i] =
                new FieldImpl(
                    fieldInfo.getFieldName(i),
                    i, fieldInfo.getFieldType(i));
        }
        return canonize(new RecordType(fields));
    }

    public SaffronType getComponentType(SaffronType type) {
        return null;
    }

    public SaffronType leastRestrictive(SaffronType [] types)
    {
        assert (types != null);
        assert (types.length >= 1);
        SaffronType type0 = types[0];
        if (type0.isProject()) {
            // recursively compute column-wise least restrictive
            int nFields = type0.getFieldCount();
            SaffronType [] inputTypes = new SaffronType[types.length];
            SaffronType [] outputTypes = new SaffronType[nFields];
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
            return createProjectType(
                outputTypes,
                fieldNames);
        } else {
            // REVIEW jvs 1-Mar-2004: I adapted this from
            // SqlOperatorTable.useBiggest to keep Saffron tests happy.  But at
            // some point need to pull up Farrago's implementation instead, at
            // least for SQL types.
            for (int i = 1; i < types.length; i++) {
                // For now, we demand that types are assignable, and assume
                // some asymmetry in the definition of isAssignableFrom.
                SaffronType type = types[i];
                SaffronType nullType = type.getFactory().createSqlType(SqlTypeName.Null);
                if (type.equals(nullType)) {
                    continue;
                }

                if (type.isAssignableFrom(type0)) {
                    type0 = type;
                } else {
                    if (!type0.isAssignableFrom(type)) {
                        return null;
                    }
                }
            }
            return type0;
        }
    }

    public SaffronType createTypeWithNullability(
        final SaffronType type,final boolean nullable)
    {
        if (type instanceof RecordType) {
            return createProjectType(
                new FieldInfo()
                {
                    public int getFieldCount()
                    {
                        return type.getFieldCount();
                    }

                    public String getFieldName(int index)
                    {
                        return type.getFields()[index].getName();
                    }

                    public SaffronType getFieldType(int index)
                    {
                        SaffronType fieldType =
                            type.getFields()[index].getType();
                        return createTypeWithNullability(fieldType,nullable);
                    }
                });
        } else {
            // REVIEW: CrossType if it stays around; otherwise get rid of this
            // comment
            return type;
        }
    }

    public static SaffronTypeFactory threadInstance()
    {
        return (SaffronTypeFactory) threadInstances.get();
    }

    /**
     * Registers a type, or returns the existing type if it is already
     * registered.
     */
    protected synchronized SaffronType canonize(SaffronType type)
    {
        SaffronType type2 = (SaffronType) map.get(type);
        if (type2 != null) {
            return type2;
        } else {
            map.put(type,type);
            return type;
        }
    }

    /**
     * Returns an array of the fields in an array of types.
     */
    private static SaffronField [] getFieldArray(SaffronType [] types)
    {
        ArrayList fieldList = new ArrayList();
        for (int i = 0; i < types.length; i++) {
            SaffronType type = types[i];
            addFields(type,fieldList);
        }
        return (SaffronField []) fieldList.toArray(
            new SaffronField[fieldList.size()]);
    }

    /**
     * Returns an array of all atomic types in an array.
     */
    private static SaffronType [] getTypeArray(SaffronType [] types)
    {
        ArrayList typeList = new ArrayList();
        getTypeArray(types,typeList);
        return (SaffronType []) typeList.toArray(
            new SaffronType[typeList.size()]);
    }

    private static void getTypeArray(SaffronType [] types,ArrayList typeList)
    {
        for (int i = 0; i < types.length; i++) {
            SaffronType type = types[i];
            if (type instanceof CrossType) {
                getTypeArray(((CrossType) type).types,typeList);
            } else {
                typeList.add(type);
            }
        }
    }

    /**
     * Adds all fields in <code>type</code> to <code>fieldList</code>.
     */
    private static void addFields(SaffronType type,ArrayList fieldList)
    {
        if (type instanceof CrossType) {
            final CrossType crossType = (CrossType) type;
            for (int i = 0; i < crossType.types.length; i++) {
                addFields(crossType.types[i],fieldList);
            }
        } else {
            SaffronField [] fields = type.getFields();
            for (int j = 0; j < fields.length; j++) {
                SaffronField field = fields[j];
                fieldList.add(field);
            }
        }
    }

    //~ Inner Classes ---------------------------------------------------------

    /**
     * Abstract implementation for {@link SaffronType}. Useful if the type
     * contains a set of fields.
     *
     * <p>
     * Identity is based upon the {@link #digest} field, which each derived
     * class should set during construction.
     * </p>
     */
    protected abstract class TypeImpl implements SaffronType
    {
        protected final SaffronField [] fields;
        protected String digest;

        protected TypeImpl(SaffronField [] fields)
        {
            this.fields = fields;
        }

        public SaffronTypeFactory getFactory()
        {
            return SaffronTypeFactoryImpl.this;
        }

        public SaffronField getField(String fieldName)
        {
            for (int i = 0; i < fields.length; i++) {
                SaffronField field = fields[i];
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
                SaffronField field = fields[i];
                if (field.getName().equals(fieldName)) {
                    return i;
                }
            }
            return -1;
        }

        public SaffronField [] getFields()
        {
            return fields;
        }

        public SaffronType getComponentType()
        {
            // this is not a collection type
            return null;
        }

        public SaffronType getArrayType()
        {
            throw Util.needToImplement(this);
        }

        public boolean isJoin()
        {
            throw Util.newInternal("todo: remove isJoin");
            //return false;
        }

        public SaffronType [] getJoinTypes()
        {
            assert(isJoin());
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

        protected abstract String computeDigest();

        public boolean equalsSansNullability(SaffronType type) {
            return equals(type);
        }

        public boolean isNullable()
        {
            return false;
        }

        public void format(Object value, PrintWriter pw) {
            if (value == null) {
                pw.print("<null>");
            } else {
                pw.print(value);
            }
        }

        public boolean isAssignableFrom(SaffronType t) {
            return false;
        }

        public boolean isSameTypeFamily(SaffronType t) {
            return false;
        }
    }

    /**
     * Type of the cartesian product of two or more sets of records.
     *
     * <p>
     * Its fields are those of its constituent records, but unlike a {@link
     * SaffronTypeFactoryImpl.RecordType}, those fields' names are not
     * necessarily distinct.
     * </p>
     */
    protected class CrossType extends TypeImpl
    {
        public final SaffronType [] types;

        /**
         * Creates a cartesian product type.
         *
         * @pre types != null
         * @pre types.length >= 1
         * @pre !(types[i] instanceof CrossType)
         */
        public CrossType(SaffronType [] types)
        {
            super(getFieldArray(types));
            this.types = types;
            assert(types != null);
            assert(types.length >= 1);
            for (int i = 0; i < types.length; i++) {
                assert(!(types[i] instanceof CrossType));
            }
            this.digest = computeDigest();
        }

        public SaffronField getField(String fieldName)
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
            final int ordinal = SyntheticClass.getOrdinal(fieldName,false);
            if (ordinal >= 0) {
                return ordinal;
            }
            throw new UnsupportedOperationException(
                "not applicable to a join type");
        }

        public SaffronField [] getFields()
        {
            throw new UnsupportedOperationException(
                "not applicable to a join type");
        }

        public boolean isJoin()
        {
            //return true;
            throw Util.newInternal("todo: remove isJoin");
        }

        public SaffronType [] getJoinTypes()
        {
            assert(isJoin());
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
                SaffronType type = types[i];
                sb.append(type);
            }
            sb.append(")");
            return sb.toString();
        }
    }

    /**
     * Simple implementation of {@link SaffronField}
     */
    protected static class FieldImpl implements SaffronField
    {
        private final SaffronType type;
        private final String name;
        private final int index;

        /**
         * @pre name != null
         * @pre type != null
         */
        public FieldImpl(String name, int index, SaffronType type)
        {
            assert(name != null);
            assert(type != null);
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

        public SaffronType getType()
        {
            return type;
        }

        public Object get(Object o)
        {
            throw new UnsupportedOperationException();
        }

        public void set(Object o,Object value)
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
         * Creates a <code>RecordType</code>. Field names must be unique.
         */
        RecordType(SaffronField [] fields)
        {
            super(fields);
            for (int i = 0; i < fields.length; i++) {
                SaffronField field = fields[i];
                assert(field != null);
                for (int j = 0; j < i; j++) {
                    SaffronField field2 = fields[j];
                    if (field.getName().equals(field2.getName())) {
                        assert(false) :
                            "i != j implies "
                            + "!fields[i].getName().equals(fields[j].getName())";
                    }
                }
            }
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
                SaffronField field = fields[i];
                sb.append(field.getType() + " " + field.getName());
            }
            sb.append(")");
            return sb.toString();
        }
    }

    public static boolean isJavaType(SaffronType t){
        return t instanceof JavaType;
    }

    public static SaffronType createSqlTypeIgnorePrecOrScale(SaffronTypeFactory fac, SqlTypeName typeName) {
        if (typeName.allowsNoPrecNoScale()) {
            return fac.createSqlType(typeName);
        }
        else if (typeName.allowsPrecNoScale()) {
            return fac.createSqlType(typeName,0);
        }
        return fac.createSqlType(typeName,0,0);
    }

    /**
     * Type which is based upon a Java class.
     */
    protected class JavaType extends TypeImpl
    {
        public final Class clazz;

        public JavaType(Class clazz)
        {
            super(fieldsOf(clazz));
            this.clazz = clazz;
            this.digest = computeDigest();
        }

        public boolean isAssignableFrom(SaffronType t) {
            if (!(t instanceof JavaType) && !(t instanceof SqlType)) {
                return false;
            }
            SqlTypeName thisSqlTypeName = JavaToSqlTypeConversionRules.instance().lookup(this);
            SqlTypeName thatSqlTypeName;

            if (t instanceof SqlType) {
                thatSqlTypeName = ((SqlType) t).getTypeName();
            } else {
                thatSqlTypeName= JavaToSqlTypeConversionRules.instance().lookup(t);
            }

            SaffronTypeFactory  fac = getFactory();
            SaffronType thisType = createSqlTypeIgnorePrecOrScale(fac, thisSqlTypeName);
            SaffronType thatType = createSqlTypeIgnorePrecOrScale(fac, thatSqlTypeName);
            return thisType.isAssignableFrom(thatType);
        }


        public boolean isSameTypeFamily(SaffronType t) {
            return t instanceof JavaType && clazz.equals(((JavaType) t).clazz);
        }

        protected String computeDigest()
        {
            return "JavaType(" + clazz + ")";
        }

        public SaffronType getArrayType() {
            final Class arrayClass = Array.newInstance(clazz,0).getClass();
            return createJavaType(arrayClass);
        }

        public SaffronType getComponentType() {
            final Class componentType = clazz.getComponentType();
            if (componentType == null) {
                return null;
            } else {
                return createJavaType(componentType);
            }
        }

        public void format(Object value, PrintWriter pw) {
            if (value == null) {
                pw.print("null");
            } else if (String.class.isAssignableFrom(clazz)) {
                Util.printJavaString(pw, (String) value, true);
            } else {
                pw.print(value);
            }
        }
    }

    private SaffronField[] fieldsOf(Class clazz) {
        final Field[] fields = clazz.getFields();
        ArrayList list = new ArrayList();
        for (int i = 0; i < fields.length; i++) {
            Field field = fields[i];
            if (Modifier.isStatic(field.getModifiers())) {
                continue;
            }
            list.add(new FieldImpl(field.getName(),
                    list.size(), createJavaType(field.getType())));
        }
        return (SaffronField[]) list.toArray(new SaffronField[list.size()]);
    }

    public SaffronType createSqlType(SqlTypeName typeName) {
        Util.pre(typeName != null, "typeName != null");
        return new SqlType(typeName);
    }

    public SaffronType createSqlType(SqlTypeName typeName, int length) {
        Util.pre(typeName != null, "typeName != null");
        Util.pre(length >= 0, "length >= 0");
        return new SqlType(typeName, length);
    }

    public SaffronType createSqlType(SqlTypeName typeName, int length, int scale) {
        Util.pre(typeName != null, "typeName != null");
        Util.pre(length >= 0, "length >= 0");
        return new SqlType(typeName, length, scale);
    }

    /**
     * SQL scalar type.
     */
    protected class SqlType implements SaffronType {
        private final SqlTypeName typeName;
        private final int precision;
        private final int scale;
        private final String digest;
        public static final int SCALE_NOT_SPECIFIED = Integer.MIN_VALUE;
        public static final int PRECISION_NOT_SPECIFIED = -1;

        /**
         * Constructs a type with no parameters.
         * @param typeName Type name
         * @pre typeName.allowsNoPrecNoScale(false,false)
         */
        SqlType(SqlTypeName typeName) {
            Util.pre(typeName.allowsPrecScale(false,false),
                     "typeName.allowsPrecScale(false,false), typeName="+typeName.name_);
            this.typeName = typeName;
            this.precision = PRECISION_NOT_SPECIFIED;
            this.scale = SCALE_NOT_SPECIFIED;
            this.digest = typeName.name_;
        }

        /**
         * Constructs a type with precision/length but no scale.
         * @param typeName Type name
         * @pre typeName.allowsPrecNoScale(true,false)
         */
        SqlType(SqlTypeName typeName, int precision) {
            Util.pre(typeName.allowsPrecScale(true,false), "typeName.allowsPrecScale(true,false)");
            this.typeName = typeName;
            this.precision = precision;
            this.scale = SCALE_NOT_SPECIFIED;
            this.digest = typeName.name_ + "(" + precision + ")";
        }
        /**
         * Constructs a type with precision/length and scale.
         * @param typeName Type name
         * @pre typeName.allowsPrecScale(true,true)
         */
        SqlType(SqlTypeName typeName, int precision, int scale) {
            Util.pre(typeName.allowsPrecScale(true,true), "typeName.allowsPrecScale(true,true)");
            this.typeName = typeName;
            this.precision = precision;
            this.scale = scale;
            this.digest = typeName.name_ + "(" + precision + ", " + scale + ")";
        }

        public SqlTypeName getTypeName()
        {
            return typeName;
        }

        public String toString() {
            return digest;
        }

        public int hashCode() {
            return digest.hashCode();
        }

        public boolean equals(Object obj) {
            return obj instanceof SqlType &&
                    ((SqlType) obj).digest.equals(digest);
        }

        public boolean isSameTypeFamily(SaffronType t) {
            return t instanceof SqlType &&
                    ((SqlType) t).typeName.ordinal_==this.typeName.ordinal_;
        }

        public SaffronTypeFactory getFactory() {
            return SaffronTypeFactoryImpl.this;
        }

        public SaffronField getField(String fieldName) {
            throw new UnsupportedOperationException();
        }

        public int getFieldCount() {
            throw new UnsupportedOperationException();
        }

        public int getFieldOrdinal(String fieldName) {
            throw new UnsupportedOperationException();
        }

        public SaffronField[] getFields() {
            throw new UnsupportedOperationException();
        }

        public SaffronType getComponentType() {
            return null;
        }

        public SaffronType getArrayType() {
            throw Util.needToImplement(this);
        }

        public boolean isJoin() {
            return false;
        }

        public SaffronType[] getJoinTypes() {
            throw new UnsupportedOperationException();
        }

        public boolean isProject() {
            return false;
        }

        public boolean equalsSansNullability(SaffronType type) {
            return equals(type);
        }

        public boolean isNullable()
        {
            // TODO
            return typeName.equals(SqlTypeName.Null);
        }

        public void format(Object value, PrintWriter pw) {
            switch (this.typeName.ordinal_) {
            case SqlTypeName.Varchar_ordinal:
                if (value instanceof SqlLiteral.StringLiteral) {
                    value = ((SqlLiteral.StringLiteral) value).getValue();
                } else {
                    assert value instanceof String;
                }
                Util.printJavaString(pw, (String) value, true);
                break;
            case SqlTypeName.Boolean_ordinal:
                assert value instanceof Boolean;
                pw.print(((Boolean) value).booleanValue() ? "true" : "false");
                break;
            case SqlTypeName.Integer_ordinal:
                assert value instanceof BigInteger;
                pw.print(value.toString());
                break;
            case SqlTypeName.Varbinary_ordinal:
                assert value instanceof byte[];
                pw.print(Util.toStringFromByteArray((byte[]) value,16));
                break;
            case SqlTypeName.Bit_ordinal:
                assert value instanceof SqlLiteral.BitString;
                pw.print(value.toString());
                break;
            case SqlTypeName.Null_ordinal:
                assert value == null;
                pw.print("null");
                break;
            default:
                throw Util.needToImplement(this);
            }
        }

        public boolean isAssignableFrom(SaffronType t) {
            return t instanceof SqlType
                    && AssignableFromRules.instance().isAssignableFrom(this.typeName,((SqlType) t).typeName);
        }
    }

    public static class JavaToSqlTypeConversionRules {
        private static JavaToSqlTypeConversionRules instance = null;
        private static HashMap rules = null;

        private JavaToSqlTypeConversionRules(){
            HashSet rule;

            rules.put(Integer.class, SqlTypeName.Integer);
            rules.put(int.class, SqlTypeName.Integer);
            rules.put(Long.class, SqlTypeName.Bigint);
            rules.put(long.class, SqlTypeName.Bigint);
            rules.put(byte.class, SqlTypeName.Tinyint);
            rules.put(Byte.class, SqlTypeName.Tinyint);

            rules.put(Double.class, SqlTypeName.Double);
            rules.put(double.class, SqlTypeName.Double);

            rules.put(boolean.class, SqlTypeName.Boolean);
            rules.put(byte[].class, SqlTypeName.Varbinary);
            rules.put(String.class, SqlTypeName.Varchar);


        }

        public static JavaToSqlTypeConversionRules instance() {
            if (null==instance){
                 rules = new HashMap();
                instance = new JavaToSqlTypeConversionRules();
            }
            return instance;
        }

        /**
         * Returns a (if there's one) corresponding {@link SqlTypeName}  for a given (java) class
         * @param t The Java class to lookup
         * @return a corresponding SqlTypeName if found, otherwise null is returned
         */
        public SqlTypeName lookup(SaffronType t) {
            JavaType javaType = (JavaType) t;
            return (SqlTypeName) rules.get(javaType.clazz);
        }
    }

    public static class AssignableFromRules {
        private static AssignableFromRules instance = null;
        private static HashMap rules = null;

        private AssignableFromRules(){
            HashSet rule;

            //Smallint is assignable from...
            rule = new HashSet();
            rule.add(SqlTypeName.Smallint);
            rules.put(SqlTypeName.Smallint, rule);

            //Int is assignable from...
            rule = new HashSet();
            rule.add(SqlTypeName.Smallint);
            rule.add(SqlTypeName.Integer);
            rules.put(SqlTypeName.Integer, rule);

            //BigInt is assignable from...
            rule = new HashSet();
            rule.add(SqlTypeName.Smallint);
            rule.add(SqlTypeName.Integer);
            rule.add(SqlTypeName.Bigint);
            rules.put(SqlTypeName.Bigint, rule);

            //Real is assignable from...
            rule = new HashSet();
            rule.add(SqlTypeName.Smallint);
            rule.add(SqlTypeName.Integer);
            rule.add(SqlTypeName.Bigint);
            rule.add(SqlTypeName.Decimal);
            rule.add(SqlTypeName.Real);
            rules.put(SqlTypeName.Real, rule);

            //Double is assignable from...
            rule = new HashSet();
            rule.add(SqlTypeName.Smallint);
            rule.add(SqlTypeName.Integer);
            rule.add(SqlTypeName.Bigint);
            rule.add(SqlTypeName.Decimal);
            rule.add(SqlTypeName.Real);
            rule.add(SqlTypeName.Double);
            rules.put(SqlTypeName.Double, rule);

            //Decimal is assignable from...
            rule = new HashSet();
            rule.add(SqlTypeName.Smallint);
            rule.add(SqlTypeName.Integer);
            rule.add(SqlTypeName.Bigint);
            rule.add(SqlTypeName.Real);
            rule.add(SqlTypeName.Double);
            rule.add(SqlTypeName.Decimal);
            rules.put(SqlTypeName.Decimal, rule);

            //Bit is assignable from...
            rule = new HashSet();
            rule.add(SqlTypeName.Varbinary);
            rule.add(SqlTypeName.Bit);
            rules.put(SqlTypeName.Bit, rule);

            //VarBinary is assignable from...
            rule = new HashSet();
            rule.add(SqlTypeName.Bit);
            rule.add(SqlTypeName.Varbinary);
            rules.put(SqlTypeName.Varbinary, rule);

            //VarChar is assignable from...
            rule = new HashSet();
            rule.add(SqlTypeName.Char);
            rule.add(SqlTypeName.Varchar);
            rules.put(SqlTypeName.Varchar, rule);

            //Boolean is assignable from...
            rule = new HashSet();
            rule.add(SqlTypeName.Boolean);
            rules.put(SqlTypeName.Boolean, rule);
        }

        public static AssignableFromRules instance() {
            if (null==instance){
                 rules = new HashMap();
                instance = new AssignableFromRules();
            }
            return instance;
        }

        public boolean isAssignableFrom(SqlTypeName to, SqlTypeName from)
        {
            if (to.equals(SqlTypeName.Null)) {
                return false;
            } else if (from.equals(SqlTypeName.Null)) {
                return true;
            }

            HashSet rule = (HashSet) rules.get(to);
            if (null==rule){
                //if you hit this assert, see the constructor of this class on how to add new rule
                throw Util.newInternal("No assign rule between from="+from+" and "+to+" defined");
            }

            return rule.contains(from);
        }
    }

}


// End SaffronTypeFactoryImpl.java
