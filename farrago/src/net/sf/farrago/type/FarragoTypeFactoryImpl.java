/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
// Copyright (C) 2003-2004 Disruptive Tech
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

package net.sf.farrago.type;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.cwm.relational.enumerations.*;
import net.sf.farrago.runtime.*;
import net.sf.farrago.type.runtime.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.util.*;

import net.sf.saffron.core.*;
import net.sf.saffron.oj.*;
import net.sf.saffron.rel.*;
import net.sf.saffron.util.*;
import net.sf.saffron.sql.type.*;

import openjava.mop.*;

import java.util.*;
import java.sql.*;
import java.sql.Date;

import javax.jmi.model.*;
import javax.jmi.reflect.*;


// REVIEW:  should FarragoTypeFactoryImpl even have to subclass
// OJTypeFactoryImpl?

/**
 * FarragoTypeFactoryImpl is the Farrago-specific implementation of the
 * Saffron TypeFactory interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoTypeFactoryImpl extends OJTypeFactoryImpl
    implements FarragoTypeFactory
{
    //~ Instance fields -------------------------------------------------------

    /** Catalog for type object definitions. */
    private final FarragoCatalog catalog;

    /** Map of OJ types corresponding to FarragoPrimitiveType instances. */
    private Map ojPrimitiveToFarragoType = new HashMap();

    /**
     * Map of SQL type numbers to corresponding FarragoType prototypes.  This
     * implements the prototype pattern, in which a prototype instance
     * represents the generic CwmSqlsimpleType, and knows how to produce a
     * Column-specific clone of itself.
     */
    private Map sqlTypeNumberToPrototype = new HashMap();

    /**
     * Map of MOF Classifier names to corresponding SQL type.
     */
    private Map classifierNameToSqlType = new HashMap();

    //~ Constructors ----------------------------------------------------------

    // TODO: avoid reinitializing static information for each new factory
    // REVIEW: LES 6-7-2004 - note that date/time prototypes hold a ref to this
    // factory, making the above TODO problematic.
    // instance
    public FarragoTypeFactoryImpl(FarragoCatalog catalog)
    {
        this.catalog = catalog;

        addPrimitivePrototype(
            new FarragoPrimitiveType(
                getSimpleType("BOOLEAN"),
                false,
                Boolean.TYPE));
        addPrimitivePrototype(
            new FarragoPrimitiveType(
                getSimpleType("BOOLEAN"),
                true,
                NullablePrimitive.NullableBoolean.class));

        addPrimitivePrototype(
            new FarragoPrimitiveType(getSimpleType("TINYINT"),false,Byte.TYPE));
        addPrimitivePrototype(
            new FarragoPrimitiveType(
                getSimpleType("TINYINT"),
                true,
                NullablePrimitive.NullableByte.class));

        addPrimitivePrototype(
            new FarragoPrimitiveType(
                getSimpleType("SMALLINT"),
                false,
                Short.TYPE));
        addPrimitivePrototype(
            new FarragoPrimitiveType(
                getSimpleType("SMALLINT"),
                true,
                NullablePrimitive.NullableShort.class));

        addPrimitivePrototype(
            new FarragoPrimitiveType(
                getSimpleType("INTEGER"),
                false,
                Integer.TYPE));
        addPrimitivePrototype(
            new FarragoPrimitiveType(
                getSimpleType("INTEGER"),
                true,
                NullablePrimitive.NullableInteger.class));

        addPrimitivePrototype(
            new FarragoPrimitiveType(getSimpleType("BIGINT"),false,Long.TYPE));
        addPrimitivePrototype(
            new FarragoPrimitiveType(
                getSimpleType("BIGINT"),
                true,
                NullablePrimitive.NullableLong.class));

        addPrimitivePrototype(
            new FarragoPrimitiveType(getSimpleType("REAL"),false,Float.TYPE));
        addPrimitivePrototype(
            new FarragoPrimitiveType(
                getSimpleType("REAL"),
                true,
                NullablePrimitive.NullableFloat.class));

        addPrimitivePrototype(
            new FarragoPrimitiveType(
                getSimpleType("DOUBLE"),
                false,
                Double.TYPE));
        addPrimitivePrototype(
            new FarragoPrimitiveType(
                getSimpleType("DOUBLE"),
                true,
                NullablePrimitive.NullableDouble.class));

        addPrecisionPrototype(
            new FarragoPrecisionType(
                getSimpleType("VARCHAR"),false,0,0,null));
        addPrecisionPrototype(
            new FarragoPrecisionType(
                getSimpleType("VARBINARY"),false,0,0,null));
        addPrecisionPrototype(
            new FarragoPrecisionType(
                getSimpleType("CHAR"),false,0,0,null));
        addPrecisionPrototype(
            new FarragoPrecisionType(
                getSimpleType("BINARY"),false,0,0,null));
        addPrecisionPrototype(
            new FarragoPrecisionType(
                getSimpleType("BIT"),false,0,0,null));
        addPrecisionPrototype(
            new FarragoPrecisionType(
                getSimpleType("DECIMAL"),false,0,0,null));

        // Date/time types
        addPrecisionPrototype(
                new FarragoDateTimeType(getSimpleType("DATE"),
                        false,false, 0, this));
        addPrecisionPrototype(
                new FarragoDateTimeType(getSimpleType("TIME"),
                        false,false,0, this));
        addPrecisionPrototype(
                new FarragoDateTimeType(getSimpleType("TIMESTAMP"),
                        false,false,0, this));
    }



    //~ Methods ---------------------------------------------------------------

    // implement FarragoTypeFactory
    public FarragoCatalog getCatalog()
    {
        return catalog;
    }

    // override SaffronTypeFactoryImpl
    public SaffronType createJoinType(SaffronType [] types)
    {
        assert(types.length == 2);
        return JoinRel.createJoinType(this,types[0],types[1]);
    }

    // override SaffronTypeFactoryImpl
    public SaffronType createSqlType(SqlTypeName typeName)
    {
        if (typeName.equals(SqlTypeName.Null)
            || typeName.equals(SqlTypeName.Any))
        {
            return super.createSqlType(typeName);
        }
        CwmSqlsimpleType simpleType = (CwmSqlsimpleType)
            catalog.getModelElement(
                catalog.relationalPackage.getCwmSqlsimpleType().refAllOfClass(),
                typeName.getName());
        assert(simpleType != null) : "Type named " + typeName.getName() + " not found.";
        FarragoType prototype =
            (FarragoType) sqlTypeNumberToPrototype.get(
                getTypeHashKey(simpleType,false));
        assert(prototype != null);
        return prototype;
    }

    // override SaffronTypeFactoryImpl
    public SaffronType createSqlType(SqlTypeName typeName, int length)
    {
        return createSqlType(typeName, length, 0);
    }

    // override SaffronTypeFactoryImpl
    public SaffronType createSqlType(
        SqlTypeName typeName, int length, int scale)
    {
        SaffronType type = createSqlType(typeName);
        assert(type instanceof FarragoPrecisionType);
        FarragoPrecisionType precisionType =
            (FarragoPrecisionType) type;
        switch (precisionType.getSimpleType().getTypeNumber().intValue()) {
        case Types.DATE:
        case Types.TIME:
        case Types.TIMESTAMP:
            assert (scale == 0) : "Non-zero scale for date/time type " + typeName;
            precisionType = new FarragoDateTimeType(
                    precisionType.getSimpleType(),
                    false,
                    false,
                    length, null);
            break;
        default:
            precisionType = new FarragoPrecisionType(
                    precisionType.getSimpleType(),
                    false,
                    length,
                    scale,
                    catalog.getDefaultCharsetName());
        }
        precisionType.factory = this;
        return canonize(precisionType);
    }

    // implement FarragoTypeFactory
    public FarragoType createColumnType(CwmColumn column,boolean validated)
    {
        FarragoAtomicType prototype = getPrototype(column);
        if (!validated) {
            return prototype;
        }
        if (prototype instanceof FarragoPrimitiveType) {
            // no specialization required, so canonization would be
            // redundant
            return prototype;
        }
        Integer pPrecision = column.getLength();
        if (pPrecision == null) {
            pPrecision = new Integer(0);
        }
        if (prototype instanceof FarragoDateTimeType) {
            FarragoType dateTimeType =
                    new FarragoDateTimeType(prototype.getSimpleType(),
                            getCatalog().isNullable(column),
                            false /* fixme - Timezone */,
                            pPrecision.intValue(), this);
            return (FarragoType) canonize(dateTimeType);
        }

        // assert (pPrecision != null);
        Integer pScale = column.getScale();
        String charsetName = column.getCharacterSetName();
        if (charsetName.equals("")) {
            charsetName = null;
        }
        FarragoType specializedType =
            new FarragoPrecisionType(
                prototype.getSimpleType(),
                getCatalog().isNullable(column),
                pPrecision.intValue(),
                (pScale == null) ? 0 : pScale.intValue(),
                charsetName);
        specializedType.factory = this;
        return (FarragoType) canonize(specializedType);
    }

    // implement FarragoTypeFactory
    public SaffronType createColumnSetType(CwmColumnSet columnSet)
    {
        final List featureList = columnSet.getFeature();
        if (featureList.isEmpty()) {
            return null;
        }
        return createProjectType(
            new SaffronTypeFactory.FieldInfo() {
                public int getFieldCount()
                {
                    return featureList.size();
                }

                public String getFieldName(int index)
                {
                    final CwmColumn column =
                        (CwmColumn) featureList.get(index);
                    return column.getName();
                }

                public SaffronType getFieldType(int index)
                {
                    final CwmColumn column =
                        (CwmColumn) featureList.get(index);
                    return createColumnType(column,true);
                }
            });
    }

    // implement FarragoTypeFactory
    public SaffronType createResultSetType(final ResultSetMetaData metaData)
    {
        final FarragoTypeFactoryImpl factory = this;
        return createProjectType(
            new SaffronTypeFactory.FieldInfo() {
                public int getFieldCount()
                {
                    try {
                        return metaData.getColumnCount();
                    } catch (SQLException ex) {
                        throw newSqlTypeException(ex);
                    }
                }

                public String getFieldName(int index)
                {
                    int iOneBased = index + 1;
                    try {
                        return metaData.getColumnName(iOneBased);
                    } catch (SQLException ex) {
                        throw newSqlTypeException(ex);
                    }
                }

                public SaffronType getFieldType(int index)
                {
                    int iOneBased = index + 1;
                    try {
                        boolean isNullable =
                            (metaData.isNullable(iOneBased)
                             != ResultSetMetaData.columnNoNulls);
                        FarragoAtomicType prototype =
                            (FarragoAtomicType) sqlTypeNumberToPrototype.get(
                                getTypeHashKey(
                                    metaData.getColumnType(iOneBased),
                                    isNullable));
                        if (prototype instanceof FarragoPrimitiveType) {
                            // no specialization required, so canonization
                            // would be redundant
                            return prototype;
                        }
                        if (prototype instanceof FarragoDateTimeType) {
                            FarragoType dateTimeType =
                                    new FarragoDateTimeType(prototype.getSimpleType(),
                                            isNullable,
                                            false /* fixme - chech TZ*/, metaData.getPrecision(iOneBased), factory);
                            return (FarragoType) canonize(dateTimeType);
                        }
                        int precision = metaData.getPrecision(iOneBased);
                        if (precision == 0) {
                            // REVIEW jvs 4-Mar-2004:  Need a good way to
                            // handle drivers like hsqldb which return 0
                            // to indicated unlimited precision.
                            precision = 2048;
                        }
                        FarragoType specializedType =
                            new FarragoPrecisionType(
                                prototype.getSimpleType(),
                                isNullable,
                                precision,
                                metaData.getScale(iOneBased),
                                catalog.getDefaultCharsetName());
                        specializedType.factory = factory;
                        return (FarragoType) canonize(specializedType);
                    } catch (SQLException ex) {
                        throw newSqlTypeException(ex);
                    }
                }
            });
    }

    private FarragoException newSqlTypeException(SQLException ex)
    {
        return FarragoResource.instance().newJdbcDriverTypeInfoFailed(ex);
    }

    // implement FarragoTypeFactory
    public FarragoType createMofType(StructuralFeature feature)
    {
        boolean isNullable = true;

        FarragoType prototype;
        if (feature != null) {
            Classifier classifier = feature.getType();
            isNullable = (feature.getMultiplicity().getLower() == 0);
            prototype = createTypeForPrimitiveByName(
                classifier.getName(),isNullable);
        } else {
            // just a hack to allow for generated mofId field
            prototype = null;
        }

        if (prototype == null) {
            // TODO:  cleanup
            prototype = new FarragoPrecisionType(
                getSimpleType("VARCHAR"),isNullable,128,0,
                catalog.getDefaultCharsetName());
            prototype.factory = this;
            prototype = (FarragoType) canonize(prototype);
        }
        return prototype;
    }

    // override SaffronTypeFactoryImpl
    public SaffronType createTypeWithNullability(
        SaffronType type,boolean nullable)
    {
        if (!(type instanceof FarragoAtomicType)) {
            return super.createTypeWithNullability(type,nullable);
        }
        FarragoAtomicType atomicType = (FarragoAtomicType) type;
        if (atomicType.isNullable() == nullable) {
            return atomicType;
        }
        if (atomicType instanceof FarragoDateTimeType) {
            FarragoDateTimeType dtType = (FarragoDateTimeType) type;
            dtType = new FarragoDateTimeType(
                    dtType.getSimpleType(),
                    nullable,
                    dtType.hasTimeZone(),
                    dtType.getPrecision(), this);
            return canonize(dtType);
        }
        if (atomicType instanceof FarragoPrecisionType) {
            FarragoPrecisionType precisionType;
            precisionType = (FarragoPrecisionType) type;
            precisionType = new FarragoPrecisionType(
                precisionType.getSimpleType(),
                nullable,
                precisionType.getPrecision(),
                precisionType.getScale(),
                precisionType.getCharsetName());
            precisionType.factory = this;
            return canonize(precisionType);
        } else {
            return createTypeForPrimitiveBySqlsimpleType(
                atomicType.getSimpleType(),nullable);
        }
    }

    private FarragoType createTypeForPrimitive(
        Class boxingClass,boolean isNullable)
    {
        return createTypeForPrimitiveByName(
            ReflectUtil.getUnqualifiedClassName(boxingClass),isNullable);
    }

    private FarragoType createTypeForPrimitiveByName(
        String boxingClassName,boolean isNullable)
    {
        CwmSqlsimpleType sqlType = (CwmSqlsimpleType)
            classifierNameToSqlType.get(boxingClassName);
        if (sqlType == null) {
            return null;
        }
        return createTypeForPrimitiveBySqlsimpleType(sqlType,isNullable);
    }

    FarragoType createTypeForPrimitiveBySqlsimpleType(
        CwmSqlsimpleType simpleType,boolean isNullable)
    {
        // no clone required since it's primitive
        FarragoType prototype = (FarragoType)
            sqlTypeNumberToPrototype.get(
                getTypeHashKey(simpleType,isNullable));
        return prototype;
    }

    // override OJTypeFactoryImpl
    public OJClass toOJClass(OJClass declarer,SaffronType type)
    {
        if (type instanceof FarragoType) {
            FarragoType farragoType = (FarragoType) type;
            return farragoType.getOjClass(declarer);
        } else if (type instanceof SqlType) {
            SqlType sqlType = (SqlType) type;
            assert(sqlType.getTypeName().equals(SqlTypeName.Null));
            return OJSystem.NULLTYPE;
        } else {
            return super.toOJClass(declarer,type);
        }
    }

    // override OJTypeFactoryImpl
    public SaffronType toType(final OJClass ojClass)
    {
        if (ojClass instanceof OJTypedClass) {
            return ((OJTypedClass) ojClass).type;
        } else {
            SaffronType type =
                (SaffronType) ojPrimitiveToFarragoType.get(ojClass);
            if (type != null) {
                return type;
            }
            return super.toType(ojClass);
        }
    }

    // REVIEW jvs 27-May-2004:  no longer using the code below for Java row
    // manipulation.  But perhaps it will be useful for flattening before going
    // into Fennel?
    
    // disabled override OJTypeFactoryImpl
    protected OJClass disabled_createOJClassForRecordType(
        OJClass declarer,RecordType recordType)
    {
        List fieldList = new ArrayList();
        if (flattenFields(recordType.getFields(),fieldList)) {
            SaffronType [] types = new SaffronType[fieldList.size()];
            String [] fieldNames = new String[types.length];
            for (int i = 0; i < types.length; ++i) {
                SaffronField field = (SaffronField) fieldList.get(i);
                types[i] = field.getType();
                // FIXME jvs 27-May-2004:  uniquify
                fieldNames[i] = field.getName();
            }
            recordType = (RecordType) createProjectType(types,fieldNames);
        }
        return super.createOJClassForRecordType(declarer,recordType);
    }

    private boolean flattenFields(SaffronField [] fields,List list)
    {
        boolean nested = false;
        for (int i = 0; i < fields.length; ++i) {
            if (fields[i].getType().isProject()) {
                nested = true;
                flattenFields(fields[i].getType().getFields(),list);
            } else {
                list.add(fields[i]);
            }
        }
        return nested;
    }
    
    // override SaffronTypeFactoryImpl
    public SaffronType leastRestrictive(SaffronType [] types)
    {
        assert(types.length > 0);
        if (types[0].isProject()) {
            return super.leastRestrictive(types);
        }
        if (!(types[0] instanceof FarragoAtomicType)) {
            return super.leastRestrictive(types);
        }
        FarragoAtomicType resultType = (FarragoAtomicType) types[0];
        boolean anyNullable = resultType.isNullable();

        for (int i = 1; i < types.length; ++i) {

            if (!(types[i] instanceof FarragoAtomicType)) {
                return super.leastRestrictive(types);
            }

            FarragoTypeFamily resultFamily = resultType.getFamily();
            FarragoAtomicType type = (FarragoAtomicType) types[i];
            FarragoTypeFamily family = type.getFamily();

            if (type.isNullable()) {
                anyNullable = true;
            }

            if (family.equals(FarragoTypeFamily.CHARACTER)
                || (family.equals(FarragoTypeFamily.BINARY)))
            {
                // TODO:  character set, collation
                if (!resultFamily.equals(family)) {
                    return null;
                }
                FarragoPrecisionType type1 =
                    (FarragoPrecisionType) resultType;
                FarragoPrecisionType type2 =
                    (FarragoPrecisionType) type;
                int precision = Math.max(
                    type1.getPrecision(),type2.getPrecision());
                // If either type is LOB, then result is LOB with no precision.
                // Otherwise, if either is variable width, result is variable
                // width.  Otherwise, result is fixed width.
                SaffronType saffronType;
                if (type1.isLob()) {
                    saffronType = createSqlType(getSqlTypeName(type1));
                } else if (type2.isLob()) {
                    saffronType = createSqlType(getSqlTypeName(type2));
                } else if (type1.isBoundedVariableWidth()) {
                    saffronType = createSqlType(
                        getSqlTypeName(type1),precision);
                } else {
                    // this catch-all case covers type2 variable, and both fixed
                    saffronType = createSqlType(
                        getSqlTypeName(type2),precision);
                }
                resultType = (FarragoAtomicType) saffronType;
            } else if (type.isExactNumeric()) {
                if (resultType.isExactNumeric()) {
                    if (!type.equals(resultType)) {
                        if (!type.takesPrecision() && !type.takesScale()
                            && !resultType.takesPrecision()
                            && !resultType.takesScale())
                        {
                            // use the bigger primitive
                            if (type.getPrecision() > resultType.getPrecision())
                            {
                                resultType = type;
                            }
                        } else {
                            // TODO:  the real thing for numerics
                            resultType = createDoublePrecisionType();
                        }
                    }
                } else if (resultType.isApproximateNumeric()) {
                    // already approximate; promote to double just in case
                    // TODO:  only promote when required
                    resultType = createDoublePrecisionType();
                } else {
                    return null;
                }
            } else if (type.isApproximateNumeric()) {
                if (!(type.equals(resultType))) {
                    resultType = createDoublePrecisionType();
                }
            } else {
                if (!family.equals(resultFamily)) {
                    return null;
                }
                // TODO:  datetime precision details
            }
        }
        if (anyNullable) {
            return createTypeWithNullability(resultType,true);
        } else {
            return resultType;
        }
    }

    // implement FarragoTypeFactory
    public void convertFieldToCwmColumn(
        SaffronField field,
        CwmColumn column)
    {
        FarragoAtomicType type = (FarragoAtomicType) field.getType();
        if (column.getName() == null) {
            column.setName(field.getName());
        }
        column.setType(type.getSimpleType());
        if (type.takesPrecision()) {
            column.setPrecision(new Integer(type.getPrecision()));
            if (type.takesScale()) {
                column.setScale(new Integer(type.getScale()));
            }
        }
        if (type.isNullable()) {
            column.setIsNullable(NullableTypeEnum.COLUMN_NULLABLE);
        } else {
            column.setIsNullable(NullableTypeEnum.COLUMN_NO_NULLS);
        }
    }

    private SqlTypeName getSqlTypeName(FarragoAtomicType type)
    {
        return SqlTypeName.get(type.getSimpleType().getName());
    }

    private FarragoAtomicType createDoublePrecisionType()
    {
        return (FarragoAtomicType) createTypeForPrimitive(
            Double.class,false);
    }

    private FarragoAtomicType getPrototype(CwmColumn column)
    {
        CwmClassifier classifier = column.getType();
        assert (classifier instanceof CwmSqlsimpleType);
        CwmSqlsimpleType simpleType = (CwmSqlsimpleType) classifier;
        FarragoAtomicType prototype =
            (FarragoAtomicType) sqlTypeNumberToPrototype.get(
                getTypeHashKey(simpleType,catalog.isNullable(column)));
        assert (prototype != null);
        return prototype;
    }

    private CwmSqlsimpleType getSimpleType(String typeName)
    {
        Collection types =
            catalog.relationalPackage.getCwmSqlsimpleType().refAllOfClass();
        return (CwmSqlsimpleType) catalog.getModelElement(types,typeName);
    }

    private Object getTypeHashKey(
        CwmSqlsimpleType simpleType,
        boolean isNullable)
    {
        return getTypeHashKey(simpleType.getTypeNumber().intValue(),isNullable);
    }

    private Object getTypeHashKey(
        int typeNumber,
        boolean isNullable)
    {
        int NULL_TYPE_BIAS = 100000;
        assert (Math.abs(typeNumber) < NULL_TYPE_BIAS);

        if (!isNullable) {
            return new Integer(typeNumber);
        } else {
            return new Integer(typeNumber + (2 * NULL_TYPE_BIAS));
        }
    }

    private void addAtomicPrototype(
        FarragoAtomicType prototype,
        boolean isNullable)
    {
        addPrototype(prototype);
        sqlTypeNumberToPrototype.put(
            getTypeHashKey(prototype.getSimpleType(),isNullable),
            prototype);
    }

    private void addPrecisionPrototype(FarragoPrecisionType prototype)
    {
        // add both nullable and NOT NULL variants
        addAtomicPrototype(prototype,false);
        addAtomicPrototype(prototype,true);
    }

    private void addPrimitivePrototype(FarragoPrimitiveType prototype)
    {
        addAtomicPrototype(prototype,prototype.isNullable());
        ojPrimitiveToFarragoType.put(
            OJClass.forClass(prototype.getClassForValue()),
            prototype);
        // Java boxing types happen to match the MOF names for primitive
        // types, isn't that nice?
        classifierNameToSqlType.put(
            ReflectUtil.getUnqualifiedClassName(
                ReflectUtil.getBoxingClass(
                    prototype.getClassForPrimitive())),
            prototype.getSimpleType());
    }

    private void addPrototype(FarragoType prototype)
    {
        prototype.factory = this;
        canonize(prototype);
    }

    //~ Inner Classes ---------------------------------------------------------

    /**
     * Make FieldImpl accessible to FarragoTypeImpl.
     */
    static class ExposedFieldImpl extends FieldImpl
    {
        /**
         * Creates a new ExposedFieldImpl object.
         *
         * @param name .
         * @param type .
         */
        ExposedFieldImpl(String name, int index, SaffronType type)
        {
            super(name,index,type);
        }
    }

    /**
     * Registers a type, or returns the existing type if it is already
     * registered.
     * Protect against bogus factory values.
     */
    protected synchronized SaffronType canonize(SaffronType type) {
        SaffronType saffronType = super.canonize(type);
        if (saffronType instanceof FarragoType) {
            assert ((FarragoType)saffronType).factory == this;
        }
        return saffronType;
    }
}


// End FarragoTypeFactoryImpl.java
