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

import java.nio.charset.Charset;
import java.sql.*;
import java.sql.Date;
import java.util.*;

import javax.jmi.model.*;
import javax.jmi.reflect.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.datatypes.CwmTypeAlias;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.cwm.relational.enumerations.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.runtime.*;
import net.sf.farrago.type.runtime.*;
import net.sf.farrago.util.*;

import openjava.mop.*;

import org.eigenbase.oj.*;
import org.eigenbase.rel.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.SqlCollation;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;


// REVIEW:  should FarragoTypeFactoryImpl even have to subclass
// OJTypeFactoryImpl?

/**
 * FarragoTypeFactoryImpl is the Farrago-specific implementation of the
 * RelDataTypeFactory interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoTypeFactoryImpl extends OJTypeFactoryImpl
    implements FarragoTypeFactory
{
    //~ Instance fields -------------------------------------------------------

    /** Repos for type object definitions. */
    private final FarragoRepos repos;

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
    private int nextGeneratedClassId;

    //~ Constructors ----------------------------------------------------------

    // TODO: avoid reinitializing static information for each new factory
    // REVIEW: LES 6-7-2004 - note that date/time prototypes hold a ref to this
    // factory, making the above TODO problematic.
    // instance
    public FarragoTypeFactoryImpl(FarragoRepos repos)
    {
        this.repos = repos;

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
            new FarragoPrimitiveType(
                getSimpleType("TINYINT"),
                false,
                Byte.TYPE));
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
            new FarragoPrimitiveType(
                getSimpleType("BIGINT"),
                false,
                Long.TYPE));
        addPrimitivePrototype(
            new FarragoPrimitiveType(
                getSimpleType("BIGINT"),
                true,
                NullablePrimitive.NullableLong.class));

        addPrimitivePrototype(
            new FarragoPrimitiveType(
                getSimpleType("REAL"),
                false,
                Float.TYPE));
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
                getSimpleType("VARCHAR"),
                false,
                0,
                0,
                null,
                null));
        addPrecisionPrototype(
            new FarragoPrecisionType(
                getSimpleType("VARBINARY"),
                false,
                0,
                0,
                null,
                null));
        addPrecisionPrototype(
            new FarragoPrecisionType(
                getSimpleType("CHAR"),
                false,
                0,
                0,
                null,
                null));
        addPrecisionPrototype(
            new FarragoPrecisionType(
                getSimpleType("BINARY"),
                false,
                0,
                0,
                null,
                null));
        addPrecisionPrototype(
            new FarragoPrecisionType(
                getSimpleType("BIT"),
                false,
                0,
                0,
                null,
                null));
        addPrecisionPrototype(
            new FarragoPrecisionType(
                getSimpleType("DECIMAL"),
                false,
                0,
                0,
                null,
                null));

        // Date/time types
        addPrecisionPrototype(
            new FarragoDateTimeType(
                getSimpleType("DATE"),
                false,
                false,
                0,
                this));
        addPrecisionPrototype(
            new FarragoDateTimeType(
                getSimpleType("TIME"),
                false,
                false,
                0,
                this));
        addPrecisionPrototype(
            new FarragoDateTimeType(
                getSimpleType("TIMESTAMP"),
                false,
                false,
                0,
                this));
    }

    //~ Methods ---------------------------------------------------------------

    // implement FarragoTypeFactory
    public FarragoRepos getRepos()
    {
        return repos;
    }

    // override RelDataTypeFactoryImpl
    public RelDataType createJoinType(RelDataType [] types)
    {
        assert (types.length == 2);
        return JoinRel.createJoinType(this, types[0], types[1]);
    }

    // override RelDataTypeFactoryImpl
    public RelDataType createSqlType(SqlTypeName typeName)
    {
        if (typeName.isSpecial()) {
            return super.createSqlType(typeName);
        }
        CwmSqlsimpleType simpleType =
            (CwmSqlsimpleType) repos.getModelElement(
                repos.relationalPackage.getCwmSqlsimpleType().refAllOfType(),
                typeName.getName());

        // If type not found directly look in aliases
        if (null == simpleType) {
            Object typeAlias =
                repos.getModelElement(
                    repos.datatypesPackage.getCwmTypeAlias().refAllOfType(),
                    typeName.getName());
            simpleType =
                (CwmSqlsimpleType) ((CwmTypeAlias) typeAlias).getType();
        }
        assert (simpleType != null) : "Type named " + typeName.getName()
        + " not found.";
        FarragoType prototype =
            (FarragoType) sqlTypeNumberToPrototype.get(
                getTypeHashKey(simpleType, false));
        assert (prototype != null);
        return prototype;
    }

    // override RelDataTypeFactoryImpl
    public RelDataType createSqlType(
        SqlTypeName typeName,
        int length)
    {
        return createSqlType(typeName, length, 0);
    }

    // override RelDataTypeFactoryImpl
    public RelDataType createSqlType(
        SqlTypeName typeName,
        int length,
        int scale)
    {
        RelDataType type = createSqlType(typeName);
        assert (type instanceof FarragoPrecisionType);
        FarragoPrecisionType precisionType = (FarragoPrecisionType) type;
        switch (precisionType.getSimpleType().getTypeNumber().intValue()) {
        case Types.DATE:
        case Types.TIME:
        case Types.TIMESTAMP:
            assert (scale == 0) : "Non-zero scale for date/time type "
            + typeName;
            precisionType =
                new FarragoDateTimeType(
                    precisionType.getSimpleType(),
                    false,
                    false,
                    length,
                    null);
            break;
        default:
            String charsetName;
            SqlCollation collation;
            if (precisionType.isCharType()) {
                charsetName = repos.getDefaultCharsetName();
                collation =
                    new SqlCollation(SqlCollation.Coercibility.Coercible);
            } else {
                charsetName = null;
                collation = null;
            }
            precisionType =
                new FarragoPrecisionType(
                    precisionType.getSimpleType(),
                    false,
                    length,
                    scale,
                    charsetName,
                    collation);
        }
        precisionType.factory = this;
        return canonize(precisionType);
    }

    // implement FarragoTypeFactory
    public FarragoType createColumnType(
        CwmColumn column,
        boolean validated)
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
            pPrecision = column.getPrecision();
        }
        if (pPrecision == null) {
            pPrecision = new Integer(0);
        }
        if (prototype instanceof FarragoDateTimeType) {
            FarragoType dateTimeType =
                new FarragoDateTimeType(
                    prototype.getSimpleType(),
                    getRepos().isNullable(column),
                    false /* fixme - Timezone */,
                    pPrecision.intValue(),
                    this);
            return (FarragoType) canonize(dateTimeType);
        }

        // assert (pPrecision != null);
        Integer pScale = column.getScale();
        String charsetName = column.getCharacterSetName();
        SqlCollation collation;
        if (charsetName.equals("")) {
            charsetName = null;
            collation = null;
        } else {
            collation = new SqlCollation(SqlCollation.Coercibility.Implicit);
        }
        FarragoType specializedType =
            new FarragoPrecisionType(
                prototype.getSimpleType(),
                getRepos().isNullable(column),
                pPrecision.intValue(),
                (pScale == null) ? 0 : pScale.intValue(),
                charsetName,
                collation);
        specializedType.factory = this;
        return (FarragoType) canonize(specializedType);
    }

    // implement FarragoTypeFactory
    public RelDataType createColumnSetType(CwmColumnSet columnSet)
    {
        final List featureList = columnSet.getFeature();
        if (featureList.isEmpty()) {
            return null;
        }
        return createProjectType(
            new RelDataTypeFactory.FieldInfo() {
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

                public RelDataType getFieldType(int index)
                {
                    final CwmColumn column =
                        (CwmColumn) featureList.get(index);
                    return createColumnType(column, true);
                }
            });
    }

    // implement FarragoTypeFactory
    public RelDataType createResultSetType(final ResultSetMetaData metaData)
    {
        final FarragoTypeFactoryImpl factory = this;
        return createProjectType(
            new RelDataTypeFactory.FieldInfo() {
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

                public RelDataType getFieldType(int index)
                {
                    int iOneBased = index + 1;
                    try {
                        boolean isNullable =
                            (metaData.isNullable(iOneBased) != ResultSetMetaData.columnNoNulls);
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
                                new FarragoDateTimeType(
                                    prototype.getSimpleType(),
                                    isNullable,
                                    false /* fixme - chech TZ*/,
                                    metaData.getPrecision(iOneBased),
                                    factory);
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
                                null,
                                null);
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

    int generateClassId()
    {
        return nextGeneratedClassId++;
    }

    // implement FarragoTypeFactory
    public FarragoType createMofType(StructuralFeature feature)
    {
        boolean isNullable = true;

        FarragoType prototype;
        if (feature != null) {
            Classifier classifier = feature.getType();
            isNullable = (feature.getMultiplicity().getLower() == 0);
            prototype =
                createTypeForPrimitiveByName(
                    classifier.getName(),
                    isNullable);
        } else {
            // just a hack to allow for generated mofId field
            prototype = null;
        }

        if (prototype == null) {
            // TODO:  cleanup
            prototype =
                new FarragoPrecisionType(
                    getSimpleType("VARCHAR"),
                    isNullable,
                    128,
                    0,
                    repos.getDefaultCharsetName(),
                    new SqlCollation(SqlCollation.Coercibility.Coercible));
            prototype.factory = this;
            prototype = (FarragoType) canonize(prototype);
        }
        return prototype;
    }

    // copy a FarragoAtomicType, setting nullability
    private RelDataType copyFarragoAtomicType(
        FarragoAtomicType type,
        boolean nullable)
    {
        if (type instanceof FarragoDateTimeType) {
            FarragoDateTimeType dtType = (FarragoDateTimeType) type;
            dtType =
                new FarragoDateTimeType(
                    dtType.getSimpleType(),
                    nullable,
                    dtType.hasTimeZone(),
                    dtType.getPrecision(),
                    this);
            return canonize(dtType);
        }
        if (type instanceof FarragoPrecisionType) {
            String charsetName;
            SqlCollation collation;
            FarragoPrecisionType precisionType;
            precisionType = (FarragoPrecisionType) type;
            if (type.isCharType()) {
                charsetName = precisionType.getCharsetName();
                collation = precisionType.getCollation();
            } else {
                charsetName = null;
                collation = null;
            }
            precisionType =
                new FarragoPrecisionType(
                    precisionType.getSimpleType(),
                    nullable,
                    precisionType.getPrecision(),
                    precisionType.getScale(),
                    charsetName,
                    collation);
            precisionType.factory = this;
            return canonize(precisionType);
        } else {
            return createTypeForPrimitiveBySqlsimpleType(
                type.getSimpleType(),
                nullable);
        }
    }

    // override RelDataTypeFactoryImpl
    public RelDataType createTypeWithNullability(
        RelDataType type,
        boolean nullable)
    {
        if (type instanceof FarragoAtomicType) {
            if (type.isNullable() == nullable) {
                return type;
            } else {
                return copyFarragoAtomicType((FarragoAtomicType) type, nullable);
            }
        } else {
            return super.createTypeWithNullability(type, nullable);
        }
    }

    // override RelDataTypeFactoryImpl
    public RelDataType copyType(RelDataType type)
    {
        if (type instanceof FarragoAtomicType) {
            return copyFarragoAtomicType(
                (FarragoAtomicType) type,
                type.isNullable());
        } else {
            return super.copyType(type);
        }
    }

    public RelDataType createTypeWithCharsetAndCollation(
        RelDataType type,
        Charset charset,
        SqlCollation collation)
    {
        assert (type.isCharType()) : "type.isCharType()==true";
        if (!(type instanceof FarragoAtomicType)) {
            return super.createTypeWithCharsetAndCollation(type, charset,
                collation);
        }

        if (type instanceof FarragoPrecisionType) {
            FarragoPrecisionType precisionType;
            precisionType = (FarragoPrecisionType) type;
            precisionType =
                new FarragoPrecisionType(
                    precisionType.getSimpleType(),
                    precisionType.isNullable(),
                    precisionType.getPrecision(),
                    precisionType.getScale(),
                    charset.name(),
                    collation);
            precisionType.factory = this;
            return canonize(precisionType);
        }

        throw Util.needToImplement("Need to implement " + type);
    }

    private FarragoType createTypeForPrimitive(
        Class boxingClass,
        boolean isNullable)
    {
        return createTypeForPrimitiveByName(
            ReflectUtil.getUnqualifiedClassName(boxingClass),
            isNullable);
    }

    private FarragoType createTypeForPrimitiveByName(
        String boxingClassName,
        boolean isNullable)
    {
        CwmSqlsimpleType sqlType =
            (CwmSqlsimpleType) classifierNameToSqlType.get(boxingClassName);
        if (sqlType == null) {
            return null;
        }
        return createTypeForPrimitiveBySqlsimpleType(sqlType, isNullable);
    }

    FarragoType createTypeForPrimitiveBySqlsimpleType(
        CwmSqlsimpleType simpleType,
        boolean isNullable)
    {
        // no clone required since it's primitive
        FarragoType prototype =
            (FarragoType) sqlTypeNumberToPrototype.get(
                getTypeHashKey(simpleType, isNullable));
        return prototype;
    }

    // override OJTypeFactoryImpl
    public OJClass toOJClass(
        OJClass declarer,
        RelDataType type)
    {
        if (type instanceof FarragoType) {
            FarragoType farragoType = (FarragoType) type;
            return farragoType.getOjClass(declarer);
        } else if (type instanceof SqlType) {
            SqlType sqlType = (SqlType) type;
            assert (sqlType.getTypeName().equals(SqlTypeName.Null));
            return OJSystem.NULLTYPE;
        } else {
            return super.toOJClass(declarer, type);
        }
    }

    // override OJTypeFactoryImpl
    public RelDataType toType(final OJClass ojClass)
    {
        if (ojClass instanceof OJTypedClass) {
            return ((OJTypedClass) ojClass).type;
        } else {
            RelDataType type =
                (RelDataType) ojPrimitiveToFarragoType.get(ojClass);
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
        OJClass declarer,
        RecordType recordType)
    {
        List fieldList = new ArrayList();
        if (flattenFields(
                    recordType.getFields(),
                    fieldList)) {
            RelDataType [] types = new RelDataType[fieldList.size()];
            String [] fieldNames = new String[types.length];
            for (int i = 0; i < types.length; ++i) {
                RelDataTypeField field = (RelDataTypeField) fieldList.get(i);
                types[i] = field.getType();

                // FIXME jvs 27-May-2004:  uniquify
                fieldNames[i] = field.getName();
            }
            recordType = (RecordType) createProjectType(types, fieldNames);
        }
        return super.createOJClassForRecordType(declarer, recordType);
    }

    private boolean flattenFields(
        RelDataTypeField [] fields,
        List list)
    {
        boolean nested = false;
        for (int i = 0; i < fields.length; ++i) {
            if (fields[i].getType().isProject()) {
                nested = true;
                flattenFields(
                    fields[i].getType().getFields(),
                    list);
            } else {
                list.add(fields[i]);
            }
        }
        return nested;
    }

    // override RelDataTypeFactoryImpl
    public RelDataType leastRestrictive(RelDataType [] types)
    {
        assert (types.length > 0);
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
                    || (family.equals(FarragoTypeFamily.BINARY))) {
                // TODO:  character set, collation
                if (!resultFamily.equals(family)) {
                    return null;
                }
                FarragoPrecisionType type1 = (FarragoPrecisionType) resultType;
                FarragoPrecisionType type2 = (FarragoPrecisionType) type;
                int precision =
                    Math.max(
                        type1.getPrecision(),
                        type2.getPrecision());

                // If either type is LOB, then result is LOB with no precision.
                // Otherwise, if either is variable width, result is variable
                // width.  Otherwise, result is fixed width.
                RelDataType relDataType;
                if (type1.isLob()) {
                    relDataType = createSqlType(getSqlTypeName(type1));
                } else if (type2.isLob()) {
                    relDataType = createSqlType(getSqlTypeName(type2));
                } else if (type1.isBoundedVariableWidth()) {
                    relDataType =
                        createSqlType(
                            getSqlTypeName(type1),
                            precision);
                } else {
                    // this catch-all case covers type2 variable, and both fixed
                    relDataType =
                        createSqlType(
                            getSqlTypeName(type2),
                            precision);
                }
                resultType = (FarragoAtomicType) relDataType;
            } else if (type.isExactNumeric()) {
                if (resultType.isExactNumeric()) {
                    if (!type.equals(resultType)) {
                        if (!type.takesPrecision() && !type.takesScale()
                                && !resultType.takesPrecision()
                                && !resultType.takesScale()) {
                            // use the bigger primitive
                            if (type.getPrecision() > resultType.getPrecision()) {
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
            return createTypeWithNullability(resultType, true);
        } else {
            return resultType;
        }
    }

    // implement FarragoTypeFactory
    public void convertFieldToCwmColumn(
        RelDataTypeField field,
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
        return (FarragoAtomicType) createTypeForPrimitive(Double.class, false);
    }

    private FarragoAtomicType getPrototype(CwmColumn column)
    {
        CwmClassifier classifier = column.getType();
        assert (classifier instanceof CwmSqlsimpleType);
        CwmSqlsimpleType simpleType = (CwmSqlsimpleType) classifier;
        FarragoAtomicType prototype =
            (FarragoAtomicType) sqlTypeNumberToPrototype.get(
                getTypeHashKey(
                    simpleType,
                    repos.isNullable(column)));
        assert (prototype != null);
        return prototype;
    }

    private CwmSqlsimpleType getSimpleType(String typeName)
    {
        Collection types =
            repos.relationalPackage.getCwmSqlsimpleType().refAllOfClass();
        return (CwmSqlsimpleType) repos.getModelElement(types, typeName);
    }

    private Object getTypeHashKey(
        CwmSqlsimpleType simpleType,
        boolean isNullable)
    {
        return getTypeHashKey(
            simpleType.getTypeNumber().intValue(),
            isNullable);
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
            getTypeHashKey(
                prototype.getSimpleType(),
                isNullable),
            prototype);
    }

    private void addPrecisionPrototype(FarragoPrecisionType prototype)
    {
        // add both nullable and NOT NULL variants
        addAtomicPrototype(prototype, false);
        addAtomicPrototype(prototype, true);
    }

    private void addPrimitivePrototype(FarragoPrimitiveType prototype)
    {
        addAtomicPrototype(
            prototype,
            prototype.isNullable());
        ojPrimitiveToFarragoType.put(
            OJClass.forClass(prototype.getClassForValue()),
            prototype);

        // Java boxing types happen to match the MOF names for primitive
        // types, isn't that nice?
        classifierNameToSqlType.put(
            ReflectUtil.getUnqualifiedClassName(
                ReflectUtil.getBoxingClass(prototype.getClassForPrimitive())),
            prototype.getSimpleType());
    }

    private void addPrototype(FarragoType prototype)
    {
        prototype.factory = this;
        canonize(prototype);
    }

    /**
     * Registers a type, or returns the existing type if it is already
     * registered.
     * Protect against bogus factory values.
     */
    protected synchronized RelDataType canonize(RelDataType type)
    {
        RelDataType relDataType = super.canonize(type);
        if (relDataType instanceof FarragoType) {
            assert ((FarragoType) relDataType).factory == this;
        }
        return relDataType;
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
        ExposedFieldImpl(
            String name,
            int index,
            RelDataType type)
        {
            super(name, index, type);
        }
    }
}


// End FarragoTypeFactoryImpl.java
