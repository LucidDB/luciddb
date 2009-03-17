/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2003-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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
package net.sf.farrago.type;

import java.nio.charset.*;

import java.sql.*;

import java.util.*;
import java.util.List;

import javax.jmi.model.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.type.runtime.*;

import openjava.mop.*;

import openjava.ptree.*;

import org.eigenbase.oj.*;
import org.eigenbase.oj.util.*;
import org.eigenbase.rel.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;


/**
 * FarragoTypeFactoryImpl is the Farrago-specific implementation of the
 * RelDataTypeFactory interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoTypeFactoryImpl
    extends OJTypeFactoryImpl
    implements FarragoTypeFactory
{
    //~ Static fields/initializers ---------------------------------------------

    private static final int unknownCharPrecision = 1024;

    //~ Instance fields --------------------------------------------------------

    /**
     * Repos for type object definitions.
     */
    private final FarragoRepos repos;

    private int nextGeneratedClassId;

    private final Map<RelDataType, OJClass> mapTypeToOJClass;

    //~ Constructors -----------------------------------------------------------

    public FarragoTypeFactoryImpl(FarragoRepos repos)
    {
        super(new OJClassMap(FarragoSyntheticObject.class, false));

        this.repos = repos;

        mapTypeToOJClass = new HashMap<RelDataType, OJClass>();
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoTypeFactory
    public FarragoRepos getRepos()
    {
        return repos;
    }

    // override RelDataTypeFactoryImpl
    public RelDataType createJoinType(RelDataType [] types)
    {
        assert (types.length == 2);
        return JoinRel.createJoinType(this, types[0], types[1], null);
    }

    // implement FarragoTypeFactory
    public RelDataType createCwmElementType(
        FemAbstractTypedElement abstractElement)
    {
        FemSqltypedElement element =
            FarragoCatalogUtil.toFemSqltypedElement(
                abstractElement);

        CwmClassifier classifier = element.getType();
        RelDataType type = createCwmTypeImpl(classifier, element);

        boolean isNullable = true;
        if (abstractElement instanceof CwmColumn) {
            isNullable =
                FarragoCatalogUtil.isColumnNullable(
                    getRepos(),
                    (CwmColumn) abstractElement);
        }

        type = createTypeWithNullability(type, isNullable);
        return type;
    }

    // implement FarragoTypeFactory
    public RelDataType createCwmType(
        CwmSqldataType cwmType)
    {
        return createCwmTypeImpl(cwmType, null);
    }

    protected RelDataType createCwmTypeImpl(
        CwmClassifier classifier,
        FemSqltypedElement element)
    {
        SqlTypeName typeName = null;
        if (classifier instanceof CwmSqlsimpleType) {
            typeName = SqlTypeName.get(classifier.getName());
            assert (typeName != null);
        } else {
            // special case for system-defined pseudotypes like CURSOR
            if (classifier.getNamespace() == null) {
                typeName = SqlTypeName.get(classifier.getName());
            }
        }

        if (typeName != null) {
            Integer pPrecision = null;
            Integer pScale = null;

            if (element != null) {
                pPrecision = element.getLength();
                if (pPrecision == null) {
                    pPrecision = element.getPrecision();
                }
                pScale = element.getScale();
            }

            RelDataType type;
            if (pScale != null) {
                assert (pPrecision != null);
                type =
                    createSqlType(
                        typeName,
                        pPrecision.intValue(),
                        pScale.intValue());
            } else if (pPrecision != null) {
                type =
                    createSqlType(
                        typeName,
                        pPrecision.intValue());
            } else {
                type =
                    createSqlType(
                        typeName);
            }

            if (element != null) {
                String charsetName = element.getCharacterSetName();
                SqlCollation collation;
                if (!charsetName.equals("")) {
                    // TODO:  collation in CWM
                    collation =
                        new SqlCollation(
                            SqlCollation.Coercibility.Implicit);

                    charsetName =
                        SqlUtil.translateCharacterSetName(charsetName);
                    assert(charsetName != null);
                    Charset charSet = Charset.forName(charsetName);
                    type =
                        createTypeWithCharsetAndCollation(
                            type,
                            charSet,
                            collation);
                }
            }
            return type;
        } else if (classifier instanceof FemSqlcollectionType) {
            FemSqlcollectionType collectionType =
                (FemSqlcollectionType) classifier;
            assert (collectionType instanceof FemSqlmultisetType) : "todo array type creation not yet implemented";
            FemSqltypeAttribute femComponentType =
                (FemSqltypeAttribute) collectionType.getFeature().get(0);
            RelDataType componentType = createCwmElementType(femComponentType);

            if (!componentType.isStruct()) {
                // REVIEW jvs 12-Feb-2005:  what is this for?
                componentType =
                    createStructType(
                        new RelDataType[] { componentType },
                        new String[] { "EXP$0" });
            }
            return createMultisetType(componentType, -1);
        } else if (classifier instanceof FemSqldistinguishedType) {
            FemSqldistinguishedType type = (FemSqldistinguishedType) classifier;
            RelDataType predefinedType = createCwmElementType(type);
            RelDataTypeField field =
                new RelDataTypeFieldImpl(
                    "PREDEFINED",
                    0,
                    predefinedType);
            SqlIdentifier id = FarragoCatalogUtil.getQualifiedName(type);
            return canonize(
                new ObjectSqlType(
                    SqlTypeName.DISTINCT,
                    id,
                    false,
                    new RelDataTypeField[] { field },
                    getUserDefinedComparability(type)));
        } else if (classifier instanceof FemSqlobjectType) {
            FemSqlobjectType objectType = (FemSqlobjectType) classifier;

            // first, create an anonymous row type
            RelDataType structType =
                createStructTypeFromClassifier(
                    objectType);

            // then, christen it
            SqlIdentifier id = FarragoCatalogUtil.getQualifiedName(objectType);
            return canonize(
                new ObjectSqlType(
                    SqlTypeName.STRUCTURED,
                    id,
                    false,
                    structType.getFields(),
                    getUserDefinedComparability(objectType)));
        } else if (classifier instanceof FemSqlrowType) {
            FemSqlrowType rowType = (FemSqlrowType) classifier;
            RelDataType structType = createStructTypeFromClassifier(rowType);
            return canonize(structType);
        } else {
            throw Util.needToImplement(classifier);
        }
    }

    private RelDataTypeComparability getUserDefinedComparability(
        FemUserDefinedType type)
    {
        if (type.getOrdering().isEmpty()) {
            return RelDataTypeComparability.None;
        }
        assert (type.getOrdering().size() == 1);
        FemUserDefinedOrdering udo = type.getOrdering().iterator().next();
        if (udo.isFull()) {
            return RelDataTypeComparability.All;
        } else {
            return RelDataTypeComparability.Unordered;
        }
    }

    // implement FarragoTypeFactory
    public RelDataType createStructTypeFromClassifier(
        CwmClassifier classifier)
    {
        final List<FemAbstractTypedElement> elementList =
            Util.filter(
                classifier.getFeature(),
                FemAbstractTypedElement.class);
        if (elementList.isEmpty()) {
            return null;
        }
        return createStructType(
            new RelDataTypeFactory.FieldInfo() {
                public int getFieldCount()
                {
                    return elementList.size();
                }

                public String getFieldName(int index)
                {
                    final FemAbstractTypedElement element =
                        elementList.get(index);
                    return element.getName();
                }

                public RelDataType getFieldType(int index)
                {
                    final FemAbstractTypedElement element =
                        elementList.get(index);
                    return createCwmElementType(element);
                }
            });
    }

    // implement FarragoTypeFactory
    public RelDataType createResultSetType(
        final ResultSetMetaData metaData,
        final boolean substitute)
    {
        return createResultSetType(
            metaData,
            substitute,
            null);
    }

    // implement FarragoTypeFactory
    public RelDataType createResultSetType(
        final ResultSetMetaData metaData,
        final boolean substitute,
        final Properties typeMapping)
    {
        return createStructType(
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
                        int typeOrdinal = metaData.getColumnType(iOneBased);
                        String dbSpecTypeName =
                            metaData.getColumnTypeName(iOneBased);
                        int precision = metaData.getPrecision(iOneBased);
                        int scale = metaData.getScale(iOneBased);
                        boolean isNullable =
                            (metaData.isNullable(iOneBased)
                                != ResultSetMetaData.columnNoNulls);
                        try {
                            RelDataType type =
                                createJdbcType(
                                    typeOrdinal,
                                    dbSpecTypeName,
                                    precision,
                                    scale,
                                    isNullable,
                                    substitute,
                                    typeMapping);
                            return (RelDataType) type;
                        } catch (Throwable ex) {
                            throw newUnsupportedJdbcType(
                                metaData.getTableName(iOneBased),
                                metaData.getColumnName(iOneBased),
                                metaData.getColumnTypeName(iOneBased),
                                typeOrdinal,
                                precision,
                                scale,
                                ex);
                        }
                    } catch (Throwable ex) {
                        throw newSqlTypeException(ex);
                    }
                }
            });
    }

    // implement FarragoTypeFactory
    public RelDataType createJdbcColumnType(
        ResultSet getColumnsResultSet,
        boolean substitute)
    {
        return createJdbcColumnType(
            getColumnsResultSet,
            substitute,
            null);
    }

    // implement FarragoTypeFactory
    public RelDataType createJdbcColumnType(
        ResultSet getColumnsResultSet,
        boolean substitute,
        Properties typeMapping)
    {
        try {
            int typeOrdinal = getColumnsResultSet.getInt(5);
            String dbSpecTypeName = getColumnsResultSet.getString(6);
            int precision = getColumnsResultSet.getInt(7);

            int scale = getColumnsResultSet.getInt(9);
            boolean isNullable =
                getColumnsResultSet.getInt(11)
                != DatabaseMetaData.columnNoNulls;
            try {
                RelDataType type =
                    createJdbcType(
                        typeOrdinal,
                        dbSpecTypeName,
                        precision,
                        scale,
                        isNullable,
                        substitute,
                        typeMapping);
                return (RelDataType) type;
            } catch (Throwable ex) {
                throw newUnsupportedJdbcType(
                    getColumnsResultSet.getString(3),
                    getColumnsResultSet.getString(4),
                    getColumnsResultSet.getString(6),
                    typeOrdinal,
                    precision,
                    scale,
                    ex);
            }
        } catch (Throwable ex) {
            throw newSqlTypeException(ex);
        }
    }

    private RelDataType createJdbcType(
        int typeOrdinal,
        String dbSpecTypeName,
        int precision,
        int scale,
        boolean isNullable,
        boolean substitute,
        Properties typeMapping)
        throws Throwable
    {
        RelDataType type;

        // TODO jvs 1-Mar-2006: Avoid using try/catch for substitution; instead,
        // get lower levels to participate.  Particularly bad is catching
        // Throwable, which could be an assertion error which has nothing to do
        // with type construction.  Also, supply more information in cases where
        // we currently just throw a plain UnsupportedOperationException.
        try {
            int [] sqlTypeInfo =
                getMappedDataType(
                    typeOrdinal,
                    precision,
                    scale,
                    dbSpecTypeName,
                    typeMapping);
            SqlTypeName typeName =
                SqlTypeName.getNameForJdbcType(sqlTypeInfo[0]);
            if (isKnownUnsupportedJdbcType(typeName)) {
                typeName = null;
            }
            precision = sqlTypeInfo[1];
            scale = sqlTypeInfo[2];

            if (typeName == null) {
                if (!substitute) {
                    throw new UnsupportedOperationException();
                }
                typeName = SqlTypeName.VARCHAR;
                precision = unknownCharPrecision;
            }

            if (SqlTypeUtil.inCharFamily(typeName)) {
                // NOTE jvs 4-Mar-2004: This is for drivers like hsqldb which
                // return 0 or large numbers to indicate unlimited precision.
                if ((precision == 0) || (precision > 65535)) {
                    if (!substitute) {
                        throw new UnsupportedOperationException();
                    }
                    precision = unknownCharPrecision;
                }
            }

            // TODO jvs 7-Dec-2005: proper datetime precision lowering once we
            // support anything greater than 0 for datetime precision; for now
            // we just toss datetime precision.
            boolean isDatetime =
                SqlTypeFamily.DATETIME.getTypeNames().contains(typeName);

            if (typeName == SqlTypeName.DECIMAL) {
                // Limit DECIMAL precision and scale.
                int maxPrecision = SqlTypeName.MAX_NUMERIC_PRECISION;
                if (precision == 0) {
                    // Deal with bogus precision 0, e.g. from Oracle
                    // Change such a Decmial type to Double
                    type = createSqlType(SqlTypeName.DOUBLE);
                    typeName = SqlTypeName.DOUBLE;
                } else {
                    if ((precision > maxPrecision) || (scale > precision)) {
                        if (!substitute) {
                            throw new UnsupportedOperationException();
                        }
                        precision = maxPrecision;

                        // In the case where we lost precision, we cap the scale
                        // at 6.  This is an arbitrary decision just like the
                        // scale of division, and we expect to have to revisit
                        // it; perhaps we could allow it to be overridden via a
                        // column-level SQL/MED storage option.
                        int cappedScale = 6;
                        if (scale > cappedScale) {
                            scale = cappedScale;
                        }
                    }
                    if (scale < 0) {
                        if (!substitute) {
                            throw new UnsupportedOperationException();
                        }
                        scale = 0;
                    }
                    type =
                        createSqlType(
                            typeName,
                            precision,
                            scale);

                    // When external types support greater precision than native
                    // types we map them to nullable types. External data that
                    // would otherwise overflow can then be replaced with null.
                    // Note that we do not fully support our stated max
                    // precision.
                    if ((precision == maxPrecision) && !isNullable) {
                        if (!substitute) {
                            throw new UnsupportedOperationException();
                        }
                        isNullable = true;
                    }
                }
            } else if (typeName.allowsScale()) {
                // This is probably never used because Decimal is the
                // only type which supports scale.
                type =
                    createSqlType(
                        typeName,
                        precision,
                        scale);
            } else if (typeName.allowsPrec() && !isDatetime) {
                type =
                    createSqlType(
                        typeName,
                        precision);
            } else {
                type =
                    createSqlType(
                        typeName);
            }
        } catch (Throwable ex) {
            if (substitute) {
                // last resort
                type =
                    createSqlType(
                        SqlTypeName.VARCHAR,
                        unknownCharPrecision);
            } else {
                // Rethrow
                throw ex;
            }
        }
        type =
            createTypeWithNullability(
                type,
                isNullable);
        return type;
    }

    private EigenbaseException newSqlTypeException(Throwable ex)
    {
        return FarragoResource.instance().JdbcDriverTypeInfoFailed.ex(ex);
    }

    private EigenbaseException newUnsupportedJdbcType(
        String tableName,
        String columnName,
        String typeName,
        int typeOrdinal,
        int precision,
        int scale,
        Throwable ex)
    {
        if (ex instanceof UnsupportedOperationException) {
            // hide this because it's not a real excn
            ex = null;
        }
        return FarragoResource.instance().JdbcDriverTypeUnsupported.ex(
            repos.getLocalizedObjectName(tableName),
            repos.getLocalizedObjectName(columnName),
            repos.getLocalizedObjectName(typeName),
            typeOrdinal,
            precision,
            scale,
            ex);
    }

    int generateClassId()
    {
        return nextGeneratedClassId++;
    }

    // implement FarragoTypeFactory
    public RelDataType createMofType(StructuralFeature feature)
    {
        boolean isNullable = true;

        SqlTypeName typeName = null;
        if (feature != null) {
            Classifier classifier = feature.getType();
            String mofTypeName = classifier.getName();
            if (mofTypeName.equals("Boolean")) {
                typeName = SqlTypeName.BOOLEAN;
            } else if (mofTypeName.equals("Byte")) {
                typeName = SqlTypeName.TINYINT;
            } else if (mofTypeName.equals("Double")) {
                typeName = SqlTypeName.DOUBLE;
            } else if (mofTypeName.equals("Float")) {
                typeName = SqlTypeName.REAL;
            } else if (mofTypeName.equals("Integer")) {
                typeName = SqlTypeName.INTEGER;
            } else if (mofTypeName.equals("Long")) {
                typeName = SqlTypeName.BIGINT;
            } else if (mofTypeName.equals("Short")) {
                typeName = SqlTypeName.SMALLINT;
            }
            isNullable = (feature.getMultiplicity().getLower() == 0);
        }

        RelDataType type;
        if (typeName == null) {
            // TODO:  cleanup
            type = createSqlType(SqlTypeName.VARCHAR, unknownCharPrecision);
        } else {
            type = createSqlType(typeName);
        }
        type =
            createTypeWithNullability(
                type,
                isNullable);
        return type;
    }

    // override OJTypeFactoryImpl
    public OJClass toOJClass(
        OJClass declarer,
        RelDataType type)
    {
        if (type.getSqlTypeName() == SqlTypeName.NULL) {
            return OJSystem.OBJECT;
        } else if (type instanceof AbstractSqlType) {
            OJClass ojClass = (OJClass) mapTypeToOJClass.get(type);
            if (ojClass != null) {
                return ojClass;
            }
            ojClass = newOJClass(declarer, type);
            mapTypeToOJClass.put(type, ojClass);
            mapOJClassToType.put(ojClass, type);
            return ojClass;
        } else {
            return super.toOJClass(declarer, type);
        }
    }

    private OJClass newOJClass(
        OJClass declarer,
        RelDataType type)
    {
        switch (type.getSqlTypeName()) {
        case BOOLEAN:
            if (type.isNullable()) {
                return OJClass.forClass(
                    NullablePrimitive.NullableBoolean.class);
            } else {
                return OJSystem.BOOLEAN;
            }
        case TINYINT:
            if (type.isNullable()) {
                return OJClass.forClass(
                    NullablePrimitive.NullableByte.class);
            } else {
                return OJSystem.BYTE;
            }
        case SMALLINT:
            if (type.isNullable()) {
                return OJClass.forClass(
                    NullablePrimitive.NullableShort.class);
            } else {
                return OJSystem.SHORT;
            }
        case SYMBOL:
        case INTEGER:
            if (type.isNullable()) {
                return OJClass.forClass(
                    NullablePrimitive.NullableInteger.class);
            } else {
                return OJSystem.INT;
            }
        case BIGINT:
            if (type.isNullable()) {
                return OJClass.forClass(
                    NullablePrimitive.NullableLong.class);
            } else {
                return OJSystem.LONG;
            }
        case DECIMAL:
            return newDecimalOJClass(
                declarer,
                type);
        case REAL:
            if (type.isNullable()) {
                return OJClass.forClass(
                    NullablePrimitive.NullableFloat.class);
            } else {
                return OJSystem.FLOAT;
            }
        case FLOAT:
        case DOUBLE:
            if (type.isNullable()) {
                return OJClass.forClass(
                    NullablePrimitive.NullableDouble.class);
            } else {
                return OJSystem.DOUBLE;
            }
        case DATE:
            return newDatetimeOJClass(
                SqlDateTimeWithoutTZ.SqlDate.class,
                declarer,
                type);
        case TIME:
            return newDatetimeOJClass(
                SqlDateTimeWithoutTZ.SqlTime.class,
                declarer,
                type);
        case TIMESTAMP:
            return newDatetimeOJClass(
                SqlDateTimeWithoutTZ.SqlTimestamp.class,
                declarer,
                type);
        case INTERVAL_DAY_TIME:
            return newIntervalOJClass(
                EncodedSqlInterval.EncodedSqlIntervalDT.class,
                declarer,
                type);
        case INTERVAL_YEAR_MONTH:
            return newIntervalOJClass(
                EncodedSqlInterval.EncodedSqlIntervalYM.class,
                declarer,
                type);
        case CHAR:
        case VARCHAR:
        case BINARY:
        case VARBINARY:
        case MULTISET:
            return newStringOJClass(
                declarer,
                type);
        case STRUCTURED:
            return createOJClassForRecordType(declarer, type);
        default:
            throw new AssertionError();
        }
    }

    private OJClass newDecimalOJClass(
        OJClass declarer,
        RelDataType type)
    {
        Class superclass = EncodedSqlDecimal.class;
        MemberDeclarationList memberDecls = new MemberDeclarationList();
        memberDecls.add(
            new MethodDeclaration(
                new ModifierList(ModifierList.PROTECTED),
                OJUtil.typeNameForClass(int.class),
                EncodedSqlDecimal.GET_PRECISION_METHOD_NAME,
                new ParameterList(),
                new TypeName[0],
                new StatementList(
                    new ReturnStatement(
                        Literal.makeLiteral(type.getPrecision())))));
        memberDecls.add(
            new MethodDeclaration(
                new ModifierList(ModifierList.PROTECTED),
                OJUtil.typeNameForClass(int.class),
                EncodedSqlDecimal.GET_SCALE_METHOD_NAME,
                new ParameterList(),
                new TypeName[0],
                new StatementList(
                    new ReturnStatement(
                        Literal.makeLiteral(type.getScale())))));
        return newHolderOJClass(
            superclass,
            memberDecls,
            declarer,
            type);
    }

    private OJClass newDatetimeOJClass(
        Class superclass,
        OJClass declarer,
        RelDataType type)
    {
        return newHolderOJClass(
            superclass,
            new MemberDeclarationList(),
            declarer,
            type);
    }

    private OJClass newIntervalOJClass(
        Class superclass,
        OJClass declarer,
        RelDataType type)
    {
        SqlIntervalQualifier qualifier = type.getIntervalQualifier();
        TypeName timeUnitType =
            OJUtil.typeNameForClass(SqlIntervalQualifier.TimeUnit.class);

        MemberDeclarationList memberDecls = new MemberDeclarationList();
        memberDecls.add(
            generateGetter(
                timeUnitType,
                EncodedSqlInterval.GET_START_UNIT_METHOD_NAME,
                lookupTimeUnit(qualifier.getStartUnit())));

        if (qualifier.getEndUnit() != null) {
            memberDecls.add(
                generateGetter(
                    timeUnitType,
                    EncodedSqlInterval.GET_END_UNIT_METHOD_NAME,
                    lookupTimeUnit(qualifier.getEndUnit())));
        } else {
            memberDecls.add(
                generateGetter(
                    timeUnitType,
                    EncodedSqlInterval.GET_END_UNIT_METHOD_NAME,
                    Literal.constantNull()));
        }

        return newHolderOJClass(
            superclass,
            memberDecls,
            declarer,
            type);
    }

    /**
     * Generates an expression for a {@link
     * org.eigenbase.sql.SqlIntervalQualifier.TimeUnit}.
     */
    private Expression lookupTimeUnit(SqlIntervalQualifier.TimeUnit timeUnit)
    {
        TypeName timeUnitType =
            OJUtil.typeNameForClass(SqlIntervalQualifier.TimeUnit.class);

        return new MethodCall(
            timeUnitType,
            SqlIntervalQualifier.TimeUnit.GET_VALUE_METHOD_NAME,
            new ExpressionList(
                Literal.makeLiteral(
                    timeUnit.ordinal())));
    }

    private OJClass newStringOJClass(
        OJClass declarer,
        RelDataType type)
    {
        Class superclass;
        MemberDeclarationList memberDecls = new MemberDeclarationList();
        if (type.getCharset() == null) {
            superclass = BytePointer.class;
        } else {
            if (SqlTypeUtil.isUnicode(type)) {
                superclass = Ucs2CharPointer.class;
            } else {
                superclass = EncodedCharPointer.class;
            }
            String charsetName = type.getCharset().name();
            memberDecls.add(
                new MethodDeclaration(
                    new ModifierList(ModifierList.PROTECTED),
                    OJUtil.typeNameForClass(String.class),
                    "getCharsetName",
                    new ParameterList(),
                    new TypeName[0],
                    new StatementList(
                        new ReturnStatement(
                            Literal.makeLiteral(charsetName)))));
        }
        return newHolderOJClass(
            superclass,
            memberDecls,
            declarer,
            type);
    }

    private OJClass newHolderOJClass(
        Class superclass,
        MemberDeclarationList memberDecls,
        OJClass declarer,
        RelDataType type)
    {
        TypeName [] superDecl =
            new TypeName[] { OJUtil.typeNameForClass(superclass) };

        TypeName [] interfaceDecls = null;
        if (type.isNullable()) {
            interfaceDecls =
                new TypeName[] {
                    OJUtil.typeNameForClass(NullableValue.class)
                };
        }
        ClassDeclaration decl =
            new ClassDeclaration(
                new ModifierList(ModifierList.PUBLIC
                    | ModifierList.STATIC),
                "Oj_inner_" + generateClassId(),
                superDecl,
                interfaceDecls,
                memberDecls);
        OJClass ojClass = new OJTypedClass(declarer, decl, type);
        try {
            declarer.addClass(ojClass);
        } catch (CannotAlterException e) {
            throw Util.newInternal(e, "holder class must be OJClassSourceCode");
        }
        Environment env = declarer.getEnvironment();
        OJUtil.recordMemberClass(
            env,
            declarer.getName(),
            decl.getName());
        OJUtil.getGlobalEnvironment(env).record(
            ojClass.getName(),
            ojClass);
        return ojClass;
    }

    /**
     * Generates a protected getter method
     *
     * @param returnType type of value returned by the getter method
     * @param methodName the name of the getter method
     * @param value the value to be returned by the getter method
     *
     * @return getter method declaration
     */
    private MethodDeclaration generateGetter(
        TypeName returnType,
        String methodName,
        Expression value)
    {
        return new MethodDeclaration(
            new ModifierList(ModifierList.PROTECTED),
            returnType,
            methodName,
            new ParameterList(),
            new TypeName[0],
            new StatementList(new ReturnStatement(value)));
    }

    // implement FarragoTypeFactory
    public Expression getValueAccessExpression(
        RelDataType type,
        Expression expr)
    {
        if (SqlTypeUtil.isDatetime(type)) {
            return new FieldAccess(
                new FieldAccess(expr, NullablePrimitive.VALUE_FIELD_NAME),
                SqlDateTimeWithoutTZ.INTERNAL_TIME_FIELD_NAME);
        } else if (

            // REVIEW: angel 2006-08-27 added this for interval
            // so generated java code okay for most expression
            // but shouldn't be checking expr,
            // probably need to rules to reinterpret interval
            (SqlTypeUtil.isInterval(type)
                && ((expr instanceof Variable)
                    || (expr instanceof FieldAccess)
                    || (expr instanceof MethodCall)))
            || SqlTypeUtil.isDecimal(type)
            || ((getClassForPrimitive(type) != null) && type.isNullable()))
        {
            return new FieldAccess(expr, NullablePrimitive.VALUE_FIELD_NAME);
        } else {
            return expr;
        }
    }

    // implement FarragoTypeFactory
    public Class getClassForPrimitive(
        RelDataType type)
    {
        SqlTypeName typeName = type.getSqlTypeName();
        if (typeName == null) {
            return null;
        }
        switch (typeName) {
        case BOOLEAN:
            return boolean.class;
        case TINYINT:
            return byte.class;
        case SMALLINT:
            return short.class;
        case INTEGER:
            return int.class;
        case BIGINT:
        case DECIMAL:
            return long.class;
        case REAL:
            return float.class;
        case FLOAT:
        case DOUBLE:
            return double.class;
        case DATE:
        case TIME:
        case TIMESTAMP:
            return SqlDateTimeWithoutTZ.getPrimitiveClass();
        case INTERVAL_DAY_TIME:
        case INTERVAL_YEAR_MONTH:
            return EncodedSqlInterval.getPrimitiveClass();
        default:
            return null;
        }
    }

    // implement FarragoTypeFactory
    public Class getClassForJavaParamStyle(
        RelDataType type)
    {
        SqlTypeName typeName = type.getSqlTypeName();
        if (typeName == null) {
            return null;
        }

        // NOTE jvs 11-Jan-2005:  per
        // SQL:2003 Part 13 Section 4.5,
        // these mappings are based on Appendix B of the JDBC 3.0
        // spec
        switch (typeName) {
        case DECIMAL:
            return java.math.BigDecimal.class;
        case CHAR:
        case VARCHAR:
            return String.class;
        case BINARY:
        case VARBINARY:
            return byte [].class;
        case DATE:
            return java.sql.Date.class;
        case TIME:
            return java.sql.Time.class;
        case TIMESTAMP:
            return java.sql.Timestamp.class;
        case CURSOR:
            return java.sql.ResultSet.class;
        case COLUMN_LIST:
            return List.class;
        default:
            return getClassForPrimitive(type);
        }
    }

    // implement RelDataTypeFactory
    public RelDataType createJavaType(
        Class clazz)
    {
        RelDataType type = super.createJavaType(clazz);
        return addDefaultAttributes(type);
    }

    // implement RelDataTypeFactory
    public RelDataType createSqlType(
        SqlTypeName typeName)
    {
        RelDataType type = super.createSqlType(typeName);
        return addDefaultAttributes(type);
    }

    // implement RelDataTypeFactory
    public RelDataType createSqlType(
        SqlTypeName typeName,
        int precision)
    {
        RelDataType type = super.createSqlType(typeName, precision);
        return addDefaultAttributes(type);
    }

    // implement RelDataTypeFactory
    public RelDataType createSqlType(
        SqlTypeName typeName,
        int precision,
        int scale)
    {
        RelDataType type = super.createSqlType(typeName, precision, scale);
        return addDefaultAttributes(type);
    }

    private RelDataType addDefaultAttributes(RelDataType type)
    {
        if (SqlTypeUtil.inCharFamily(type)) {
            Charset charset = getDefaultCharset();
            SqlCollation collation =
                new SqlCollation(
                    SqlCollation.Coercibility.Coercible);
            type =
                createTypeWithCharsetAndCollation(
                    type,
                    charset,
                    collation);
        }
        return type;
    }

    // implement RelDataTypeFactory
    public Charset getDefaultCharset()
    {
        String charsetName = repos.getDefaultCharsetName();
        Charset charset = Charset.forName(charsetName);
        return charset;
    }

    private int [] getMappedDataType(
        int ordinal,
        int precision,
        int scale,
        String dbSpecTypeName,
        Properties typeMapping)
    {
        String leftParen = "(";
        String rightParen = ")";
        String comma = ",";

        SqlTypeName originalType = SqlTypeName.getNameForJdbcType(ordinal);
        String originalName = dbSpecTypeName.toUpperCase();
        if (originalType != null) {
            originalName = originalType.toString().toUpperCase();
        }

        // look for DATATYPE(P,S) in typeMapping
        String newName =
            typeMapping.getProperty(
                originalName + leftParen + precision + comma + scale
                + rightParen);

        // look for DATATYPE(P) in typeMapping
        if (newName == null) {
            newName =
                typeMapping.getProperty(
                    originalName + leftParen + precision + rightParen);
        }

        // look for DATATYPE in typeMapping
        if (newName == null) {
            newName = typeMapping.getProperty(originalName, originalName);
        }

        SqlTypeName newType = SqlTypeName.get(extractDataTypeName(newName));
        int newPrecision = extractPrecision(newName, precision);
        int newScale = extractScale(newName, scale);

        if (newType != null) {
            return new int[] {
                    newType.getJdbcOrdinal(), newPrecision, newScale
                };
        } else {
            return new int[] { ordinal, precision, scale };
        }
    }

    private String extractDataTypeName(String mapping)
    {
        String leftParen = "(";

        if (mapping.indexOf(leftParen) != -1) {
            mapping = mapping.substring(0, mapping.indexOf(leftParen));
        }

        for (SqlTypeName typeName : SqlTypeName.values()) {
            if (typeName.name().equalsIgnoreCase(mapping)) {
                return typeName.name();
            }
        }
        return mapping;
    }

    private int extractPrecision(String mapping, int precision)
    {
        String leftParen = "\\(";
        String comma = ",";

        String [] datamap = mapping.split(leftParen);
        if (datamap.length != 2) {
            return precision;
        }

        String precAndScale = datamap[1];
        String precisionStr =
            precAndScale.substring(0, precAndScale.length() - 1);

        int idxOfComma = precisionStr.indexOf(comma);
        if (idxOfComma != -1) {
            precisionStr = precisionStr.substring(0, idxOfComma);
        }
        return Integer.parseInt(precisionStr);
    }

    private int extractScale(String mapping, int scale)
    {
        String leftParen = "\\(";
        String comma = ",";

        String [] datamap = mapping.split(leftParen);
        if (datamap.length != 2) {
            return scale;
        }

        String precAndScale = datamap[1];
        int idxOfComma = precAndScale.indexOf(comma);

        if (idxOfComma == -1) {
            return scale;
        }
        String scaleStr =
            precAndScale.substring(idxOfComma + 1, precAndScale.length() - 1);
        return Integer.parseInt(scaleStr);
    }

    // TODO: The following JDBC types cannot currently be mapped to a
    // RelDataType
    private boolean isKnownUnsupportedJdbcType(SqlTypeName type)
    {
        if (type == null) {
            return false;
        }
        switch (type) {
        case DISTINCT:
        case STRUCTURED:
        case ROW:
        case CURSOR:
            return true;
        default:
            return false;
        }
    }
}

// End FarragoTypeFactoryImpl.java
