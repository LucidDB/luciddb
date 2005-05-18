/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2003-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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
package net.sf.farrago.type;

import java.nio.charset.Charset;
import java.sql.*;
import java.util.*;

import javax.jmi.model.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.type.runtime.*;
import net.sf.farrago.util.*;

import openjava.mop.*;
import openjava.ptree.*;

import org.eigenbase.oj.*;
import org.eigenbase.oj.util.*;
import org.eigenbase.rel.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;

import java.util.List;

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

    private int nextGeneratedClassId;

    private Map mapTypeToOJClass;

    //~ Constructors ----------------------------------------------------------

    public FarragoTypeFactoryImpl(FarragoRepos repos)
    {
        super(new OJClassMap(FarragoSyntheticObject.class));

        this.repos = repos;

        mapTypeToOJClass = new HashMap();
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

    // implement FarragoTypeFactory
    public RelDataType createCwmElementType(
        FemAbstractTypedElement abstractElement)
    {
        FemSqltypedElement element = FarragoCatalogUtil.toFemSqltypedElement(
            abstractElement);
        
        CwmClassifier classifier = element.getType();
        RelDataType type = createCwmTypeImpl(classifier, element);
        
        boolean isNullable = true;
        if (abstractElement instanceof CwmColumn) {
            isNullable = FarragoCatalogUtil.isColumnNullable(
                getRepos(), (CwmColumn) abstractElement);
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
        if (classifier instanceof CwmSqlsimpleType) {
            CwmSqlsimpleType simpleType = (CwmSqlsimpleType) classifier;

            SqlTypeName typeName = SqlTypeName.get(simpleType.getName());
            assert(typeName != null);

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
                assert(pPrecision != null);
                type = createSqlType(
                    typeName,
                    pPrecision.intValue(),
                    pScale.intValue());
            } else if (pPrecision != null) {
                type = createSqlType(
                    typeName,
                    pPrecision.intValue());
            } else {
                type = createSqlType(
                    typeName);
            }

            if (element != null) {
                String charsetName = element.getCharacterSetName();
                SqlCollation collation;
                if (!charsetName.equals("")) {
                    // TODO:  collation in CWM
                    collation = new SqlCollation(
                        SqlCollation.Coercibility.Implicit);

                    Charset charSet = Charset.forName(charsetName);
                    type = createTypeWithCharsetAndCollation(
                        type,
                        charSet,
                        collation);
                }
            }
            return type;
        } else if (classifier instanceof FemSqlcollectionType) {
            FemSqlcollectionType collectionType =
                (FemSqlcollectionType) classifier;
            assert(collectionType instanceof FemSqlmultisetType) :
                "todo array type creation not yet implemented";
            FemSqltypeAttribute femComponentType = (FemSqltypeAttribute)
                collectionType.getFeature().get(0);
            RelDataType componentType =
                createCwmElementType(femComponentType);

            if (!componentType.isStruct()) {
                // REVIEW jvs 12-Feb-2005:  what is this for?
                componentType = createStructType(
                    new RelDataType[]{componentType},
                    new String[]{"EXP$0"});
            }
            return createMultisetType(componentType, -1);
        } else if (classifier instanceof FemSqldistinguishedType) {
            FemSqldistinguishedType type = (FemSqldistinguishedType) classifier;
            RelDataType predefinedType = createCwmElementType(type);
            RelDataTypeField field = new RelDataTypeFieldImpl(
                "PREDEFINED",
                0,
                predefinedType);
            SqlIdentifier id = FarragoCatalogUtil.getQualifiedName(type);
            return canonize(
                new ObjectSqlType(
                    SqlTypeName.Distinct, id, false,
                    new RelDataTypeField [] {field},
                    getUserDefinedComparability(type)));
        } else if (classifier instanceof FemSqlobjectType) {
            FemSqlobjectType objectType =
                (FemSqlobjectType) classifier;
            // first, create an anonymous row type
            RelDataType structType = createStructTypeFromClassifier(
                objectType);
            // then, christen it
            SqlIdentifier id = FarragoCatalogUtil.getQualifiedName(objectType);
            return canonize(
                new ObjectSqlType(
                    SqlTypeName.Structured, id, false, structType.getFields(),
                    getUserDefinedComparability(objectType)));
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
        assert(type.getOrdering().size() == 1);
        FemUserDefinedOrdering udo =
            (FemUserDefinedOrdering) type.getOrdering().iterator().next();
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
        final List featureList =
            FarragoCatalogUtil.getStructuralFeatures(classifier);
        if (featureList.isEmpty()) {
            return null;
        }
        return createStructType(
            new RelDataTypeFactory.FieldInfo() {
                public int getFieldCount()
                {
                    return featureList.size();
                }

                public String getFieldName(int index)
                {
                    final FemAbstractTypedElement element =
                        (FemAbstractTypedElement) featureList.get(index);
                    return element.getName();
                }

                public RelDataType getFieldType(int index)
                {
                    final FemAbstractTypedElement element =
                        (FemAbstractTypedElement) featureList.get(index);
                    RelDataType type = createCwmElementType(element);
                    return type;
                }
            });
    }

    // implement FarragoTypeFactory
    public RelDataType createResultSetType(final ResultSetMetaData metaData)
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
                        SqlTypeName typeName = SqlTypeName.getNameForJdbcType(
                            metaData.getColumnType(iOneBased));

                        assert(typeName != null);

                        int precision = metaData.getPrecision(iOneBased);
                        if (SqlTypeFamily.getFamilyForSqlType(typeName)
                            == SqlTypeFamily.Character)
                        {
                            if ((precision == 0) || (precision > 65535)) {
                                // REVIEW jvs 4-Mar-2004: Need a good way to
                                // handle drivers like hsqldb which return 0 or
                                // large numbers to indicate unlimited
                                // precision.
                                precision = 2048;
                            }
                        }
                        int scale = metaData.getScale(iOneBased);

                        RelDataType type;
                        if (typeName.allowsScale()) {
                            type = createSqlType(
                                typeName, precision, scale);
                        } else if (typeName.allowsPrec()) {
                            type = createSqlType(
                                typeName, precision);
                        } else {
                            type = createSqlType(
                                typeName);
                        }

                        boolean isNullable =
                            (metaData.isNullable(iOneBased)
                                != ResultSetMetaData.columnNoNulls);
                        type = createTypeWithNullability(
                            type,
                            isNullable);
                        return type;
                    } catch (SQLException ex) {
                        throw newSqlTypeException(ex);
                    }
                }
            });
    }

    private EigenbaseException newSqlTypeException(SQLException ex)
    {
        return FarragoResource.instance().newJdbcDriverTypeInfoFailed(ex);
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
                typeName = SqlTypeName.Boolean;
            } else if (mofTypeName.equals("Byte")) {
                typeName = SqlTypeName.Tinyint;
            } else if (mofTypeName.equals("Double")) {
                typeName = SqlTypeName.Double;
            } else if (mofTypeName.equals("Float")) {
                typeName = SqlTypeName.Real;
            } else if (mofTypeName.equals("Integer")) {
                typeName = SqlTypeName.Integer;
            } else if (mofTypeName.equals("Long")) {
                typeName = SqlTypeName.Bigint;
            } else if (mofTypeName.equals("Short")) {
                typeName = SqlTypeName.Smallint;
            }
            isNullable = (feature.getMultiplicity().getLower() == 0);
        }

        RelDataType type;
        if (typeName == null) {
            // TODO:  cleanup
            type = createSqlType(SqlTypeName.Varchar, 128);
        } else {
            type = createSqlType(typeName);
        }
        type = createTypeWithNullability(
            type,
            isNullable);
        return type;
    }

    // override OJTypeFactoryImpl
    public OJClass toOJClass(
        OJClass declarer,
        RelDataType type)
    {
        if (type.getSqlTypeName() == SqlTypeName.Null) {
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
        switch (type.getSqlTypeName().getOrdinal()) {
        case SqlTypeName.Boolean_ordinal:
            if (type.isNullable()) {
                return OJClass.forClass(
                    NullablePrimitive.NullableBoolean.class);
            } else {
                return OJSystem.BOOLEAN;
            }
        case SqlTypeName.Tinyint_ordinal:
            if (type.isNullable()) {
                return OJClass.forClass(
                    NullablePrimitive.NullableByte.class);
            } else {
                return OJSystem.BYTE;
            }
        case SqlTypeName.Smallint_ordinal:
            if (type.isNullable()) {
                return OJClass.forClass(
                    NullablePrimitive.NullableShort.class);
            } else {
                return OJSystem.SHORT;
            }
        case SqlTypeName.Integer_ordinal:
            if (type.isNullable()) {
                return OJClass.forClass(
                    NullablePrimitive.NullableInteger.class);
            } else {
                return OJSystem.INT;
            }
        case SqlTypeName.Bigint_ordinal:
            if (type.isNullable()) {
                return OJClass.forClass(
                    NullablePrimitive.NullableLong.class);
            } else {
                return OJSystem.LONG;
            }
        case SqlTypeName.Real_ordinal:
            if (type.isNullable()) {
                return OJClass.forClass(
                    NullablePrimitive.NullableFloat.class);
            } else {
                return OJSystem.FLOAT;
            }
        case SqlTypeName.Float_ordinal:
        case SqlTypeName.Double_ordinal:
            if (type.isNullable()) {
                return OJClass.forClass(
                    NullablePrimitive.NullableDouble.class);
            } else {
                return OJSystem.DOUBLE;
            }
        case SqlTypeName.Date_ordinal:
            return newDatetimeOJClass(
                SqlDateTimeWithoutTZ.SqlDate.class, declarer, type);
        case SqlTypeName.Time_ordinal:
            return newDatetimeOJClass(
                SqlDateTimeWithoutTZ.SqlTime.class, declarer, type);
        case SqlTypeName.Timestamp_ordinal:
            return newDatetimeOJClass(
                SqlDateTimeWithoutTZ.SqlTimestamp.class, declarer, type);
        case SqlTypeName.Char_ordinal:
        case SqlTypeName.Varchar_ordinal:
        case SqlTypeName.Binary_ordinal:
        case SqlTypeName.Varbinary_ordinal:
        case SqlTypeName.Multiset_ordinal:
            return newStringOJClass(
                declarer, type);
        case SqlTypeName.Structured_ordinal:
            return createOJClassForRecordType(declarer, type);
        default:
            throw new AssertionError();
        }
    }

    private OJClass newDatetimeOJClass(
        Class superclass, OJClass declarer, RelDataType type)
    {
        return newHolderOJClass(
            superclass, new MemberDeclarationList(), declarer, type);
    }

    private OJClass newStringOJClass(
        OJClass declarer, RelDataType type)
    {
        Class superclass;
        MemberDeclarationList memberDecls = new MemberDeclarationList();
        if (type.getCharset() == null) {
            superclass = BytePointer.class;
        } else {
            String charsetName = type.getCharset().name();
            superclass = EncodedCharPointer.class;
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
            superclass, memberDecls, declarer, type);
    }

    private OJClass newHolderOJClass(
        Class superclass,
        MemberDeclarationList memberDecls,
        OJClass declarer,
        RelDataType type)
    {
        TypeName [] superDecl =
            new TypeName [] { OJUtil.typeNameForClass(superclass) };

        TypeName [] interfaceDecls = null;
        if (type.isNullable()) {
            interfaceDecls =
                new TypeName [] {
                    OJUtil.typeNameForClass(NullableValue.class)
                };
        }
        ClassDeclaration decl =
            new ClassDeclaration(new ModifierList(ModifierList.PUBLIC
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

    // implement FarragoTypeFactory
    public Expression getValueAccessExpression(
        RelDataType type,
        Expression expr)
    {
        if (SqlTypeUtil.isDatetime(type) ||
            ((getClassForPrimitive(type) != null) && type.isNullable()))
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
        switch(typeName.getOrdinal()) {
        case SqlTypeName.Boolean_ordinal:
            return boolean.class;
        case SqlTypeName.Tinyint_ordinal:
            return byte.class;
        case SqlTypeName.Smallint_ordinal:
            return short.class;
        case SqlTypeName.Integer_ordinal:
            return int.class;
        case SqlTypeName.Bigint_ordinal:
            return long.class;
        case SqlTypeName.Real_ordinal:
            return float.class;
        case SqlTypeName.Float_ordinal:
        case SqlTypeName.Double_ordinal:
            return double.class;
        case SqlTypeName.Date_ordinal:
        case SqlTypeName.Time_ordinal:
        case SqlTypeName.Timestamp_ordinal:
            return SqlDateTimeWithoutTZ.getPrimitiveClass();
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
        switch(typeName.getOrdinal()) {
        case SqlTypeName.Decimal_ordinal:
            return java.math.BigDecimal.class;
        case SqlTypeName.Char_ordinal:
        case SqlTypeName.Varchar_ordinal:
            return String.class;
        case SqlTypeName.Binary_ordinal:
        case SqlTypeName.Varbinary_ordinal:
            return byte [].class;
        case SqlTypeName.Date_ordinal:
            return java.sql.Date.class;
        case SqlTypeName.Time_ordinal:
            return java.sql.Time.class;
        case SqlTypeName.Timestamp_ordinal:
            return java.sql.Timestamp.class;
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
            String charsetName = repos.getDefaultCharsetName();
            SqlCollation collation = new SqlCollation(
                SqlCollation.Coercibility.Coercible);
            Charset charset = Charset.forName(charsetName);
            type = createTypeWithCharsetAndCollation(
                type,
                charset,
                collation);
        }
        return type;
    }
}

// End FarragoTypeFactoryImpl.java

