/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// (C) Copyright 2003-2004 John V. Sichi
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

package net.sf.saffron.sql;

import net.sf.saffron.core.SaffronType;
import net.sf.saffron.core.SaffronTypeFactory;
import net.sf.saffron.resource.SaffronResource;
import net.sf.saffron.sql.test.SqlTester;
import net.sf.saffron.sql.type.SqlTypeName;
import net.sf.saffron.sql.fun.SqlStdOperatorTable;
import net.sf.saffron.util.MultiMap;
import net.sf.saffron.util.Util;

import java.lang.reflect.Field;
import java.util.*;


/**
 * <code>SqlOperatorTable</code> is a singleton which contains an instance of
 * each operator.
 */
public class SqlOperatorTable
{

    //~ Static fields/initializers --------------------------------------------

    private static SqlStdOperatorTable instance;

    public static final SqlTypeName[] stringTypes =
      {SqlTypeName.Varchar,SqlTypeName.Bit,SqlTypeName.Binary,SqlTypeName.Varbinary};
    public static final SqlTypeName[] stringNullableTypes =
      {SqlTypeName.Null,SqlTypeName.Varchar,SqlTypeName.Bit,SqlTypeName.Binary,SqlTypeName.Varbinary};
    public static final SqlTypeName[] numericTypes =
            {SqlTypeName.Integer,SqlTypeName.Bigint,SqlTypeName.Decimal,
                                        SqlTypeName.Real,SqlTypeName.Double};
    public static final SqlTypeName[] numericNullableTypes =
            {SqlTypeName.Null,SqlTypeName.Integer,SqlTypeName.Bigint,SqlTypeName.Decimal,
                                        SqlTypeName.Real,SqlTypeName.Double};
    public static final SqlTypeName[] booleanTypes = {SqlTypeName.Boolean};
    public static final SqlTypeName[] booleanNullableTypes =
            {SqlTypeName.Null,SqlTypeName.Boolean};
    public static final SqlTypeName[] binaryNullableTypes =
            {SqlTypeName.Null,SqlTypeName.Bit,SqlTypeName.Varbinary};
    public static final SqlTypeName[] intTypes =
                   {SqlTypeName.Integer,SqlTypeName.Bigint};
    public static final SqlTypeName[] intNullableTypes =
                   {SqlTypeName.Null,SqlTypeName.Integer,SqlTypeName.Bigint};
    public static final SqlTypeName[] varcharTypes =
            {SqlTypeName.Varchar};
    public static final SqlTypeName[] varcharNullableTypes =
            {SqlTypeName.Null,SqlTypeName.Varchar};
    public static final SqlTypeName[] timeIntervalNullableTypes =
            {SqlTypeName.Null,SqlTypeName.IntervalDayTime, SqlTypeName.IntervalYearToMonth};

    /**
     * Type-inference strategy whereby the result type of a call is the type of
     * the first operand.
     */
    public static final SqlOperator.TypeInference useFirstArgType =
            new SqlOperator.TypeInference() {

                public SaffronType getType(SqlValidator validator, SqlValidator.Scope scope, SqlCall call) {
                    return validator.deriveType(scope, call.operands[0]);
                }

                public SaffronType getType(SaffronTypeFactory typeFactory, SaffronType[] argTypes) {
                    return argTypes[0];
                }
            };

    /**
     * Type-inference strategy whereby the result type of a call is the type of
     * the second operand.
     */
    public static final SqlOperator.TypeInference useSecondArgType =
            new SqlOperator.TypeInference() {

                public SaffronType getType(SqlValidator validator, SqlValidator.Scope scope, SqlCall call) {
                    return validator.deriveType(scope, call.operands[1]);
                }

                public SaffronType getType(SaffronTypeFactory typeFactory,SaffronType[] argTypes) {
                    return argTypes[1];
                }
            };

    /**
     * Type-inference strategy whereby the result type of a call is Boolean.
     */
    public static final SqlOperator.TypeInference useBoolean =
            new SqlOperator.TypeInference() {
                public SaffronType getType(SqlValidator validator,
                        SqlValidator.Scope scope, SqlCall call) {
                    return validator.typeFactory.createSqlType(SqlTypeName.Boolean);
                }
                public SaffronType getType(SaffronTypeFactory typeFactory,
                        SaffronType[] argTypes) {
                    return typeFactory.createSqlType(SqlTypeName.Boolean);
                }
            };
    /**
     * Type-inference strategy whereby the result type of a call is Boolean,
     * with nulls allowed if any of the operands allow nulls.
     */
    public static final SqlOperator.TypeInference useNullableBoolean =
    new SqlOperator.TypeInference() {
        public SaffronType getType(
            SqlValidator validator,
            SqlValidator.Scope scope,
            SqlCall call)
        {
            SaffronType type = useBoolean.getType(validator,scope,call);
            for (int i = 0; i < call.operands.length; ++i) {
                SaffronType operandType = validator.deriveType(
                    scope,call.operands[i]);
                if (operandType.isNullable()) {
                    type = validator.typeFactory.createTypeWithNullability(
                        type,true);
                    break;
                }
            }
            return type;
        }
        public SaffronType getType(
            SaffronTypeFactory typeFactory,
            SaffronType[] argTypes)
        {
            SaffronType type = useBoolean.getType(typeFactory,argTypes);
            for (int i = 0; i < argTypes.length; ++i) {
                if (argTypes[i].isNullable()) {
                    type = typeFactory.createTypeWithNullability(type,true);
                    break;
                }
            }
            return type;
        }
    };

    /**
     * Type-inference strategy whereby the result type of a call is Double.
     */
    public static final SqlOperator.TypeInference useDouble =
            new SqlOperator.TypeInference() {
                public SaffronType getType(SqlValidator validator,
                        SqlValidator.Scope scope, SqlCall call) {
                    return validator.typeFactory.createSqlType(SqlTypeName.Double);
                }
                public SaffronType getType(SaffronTypeFactory typeFactory,
                        SaffronType[] argTypes) {
                    return typeFactory.createSqlType(SqlTypeName.Double);
                }
            };

    /**
     * Type-inference strategy whereby the result type of a call is a Integer.
     */
    public static final SqlOperator.TypeInference useInteger =
            new SqlOperator.TypeInference() {
                public SaffronType getType(SqlValidator validator,
                        SqlValidator.Scope scope, SqlCall call) {
                    return validator.typeFactory.createSqlType(SqlTypeName.Integer);
                }
                public SaffronType getType(SaffronTypeFactory typeFactory,
                        SaffronType[] argTypes) {
                    return typeFactory.createSqlType(SqlTypeName.Integer);
                }
            };

    /**
     * Type-inference strategy whereby the result type of a call is
     */


    /**
     * Type-inference strategy whereby the result type of a call is using its operands biggest type,
     * using the rules described in ISO/IEC 9075-2:1999 section 9.3 "Data types of results of aggregations"
     * These rules are used in union, except, intercect, case and other places
     * E.g (500000000000 + 3.0e-3) have the operands INTEGER and DOUBLE. Its biggest type is double
     */
    public static final SqlOperator.TypeInference useBiggest =
            new SqlOperator.TypeInference() {

                public SaffronType getType(SqlValidator validator, SqlValidator.Scope scope, SqlCall call) {
                    SaffronType[] types = new SaffronType[call.operands.length];
                    for (int i = 0; i < call.operands.length; i++) {
                        types[i] = validator.deriveType(scope, call.operands[i]);
                    }
                    return getType(validator.typeFactory,types);
                }

                public SaffronType getType(SaffronTypeFactory typeFactory, SaffronType[] argTypes) {
                    // REVIEW jvs 1-Mar-2004: I changed this to
                    // leastRestrictive since that's its purpose (and at least
                    // for Farrago, works better than the old getBiggest code).
                    // But this type inference rule isn't general enough for
                    // numeric types, which use different rules depending on
                    // the operator (e.g. sum precision is based on the max of
                    // the arg precisions, while product precision is based on
                    // the sum of the arg precisions).
                    return typeFactory.leastRestrictive(argTypes);
                }
            };

    /**
     * Type-inference strategy where the expression is assumed to be registered
     * as a {@link SqlValidator.Scope}, and therefore the result type of the
     * call is the type of that scope.
     */
    public static final SqlOperator.TypeInference useScope =
            new SqlOperator.TypeInference() {
                public SaffronType getType(SqlValidator validator,
                        SqlValidator.Scope scope, SqlCall call) {
                    return validator.getScope(call).getRowType();
                }
                public SaffronType getType(SaffronTypeFactory typeFactory,
                        SaffronType[] argTypes) {
                    throw new UnsupportedOperationException();
                }
            };

    /**
     * Parameter type-inference strategy where an unknown operand
     * type is derived from the first operand with a known type.
     */
    public static final SqlOperator.ParamTypeInference useFirstKnownParam =
    new SqlOperator.ParamTypeInference()
    {
        public void inferOperandTypes(
            SqlValidator validator,
            SqlValidator.Scope scope,
            SqlCall call,
            SaffronType returnType,
            SaffronType [] operandTypes)
        {
            SqlNode [] operands = call.getOperands();
            SaffronType knownType = validator.unknownType;
            for (int i = 0; i < operands.length; ++i) {
                knownType = validator.deriveType(scope,operands[i]);
                if (!knownType.equals(validator.unknownType)) {
                    break;
                }
            }
            assert(!knownType.equals(validator.unknownType));
            for (int i = 0; i < operandTypes.length; ++i) {
                operandTypes[i] = knownType;
            }
        }
    };

    /**
     * Parameter type-inference strategy where an unknown operand
     * type is derived from the call's return type.  If the return type is
     * a record, it must have the same number of fields as the number
     * of operands.
     */
    public static final SqlOperator.ParamTypeInference useReturnForParam =
    new SqlOperator.ParamTypeInference()
    {
        public void inferOperandTypes(
            SqlValidator validator,
            SqlValidator.Scope scope,
            SqlCall call,
            SaffronType returnType,
            SaffronType [] operandTypes)
        {
            for (int i = 0; i < operandTypes.length; ++i) {
                if (returnType.isProject()) {
                    operandTypes[i] = returnType.getFields()[i].getType();
                } else {
                    operandTypes[i] = returnType;
                }
            }
        }
    };

    /**
     * Parameter type-inference strategy where an unknown operand
     * type is assumed to be boolean.
     */
    public static final SqlOperator.ParamTypeInference booleanParam =
    new SqlOperator.ParamTypeInference()
    {
        public void inferOperandTypes(
            SqlValidator validator,
            SqlValidator.Scope scope,
            SqlCall call,
            SaffronType returnType,
            SaffronType [] operandTypes)
        {
            for (int i = 0; i < operandTypes.length; ++i) {
                operandTypes[i] = validator.typeFactory.createSqlType(
                    SqlTypeName.Boolean);
            }
        }
    };

    /**
     * Parameter type-checking strategy
     * type must be nullable boolean, nullable boolean.
     */
    public static final SqlOperator.AllowedArgInference typeNullableBoolBool =
        new SqlOperator.AllowedArgInference
                (new SqlTypeName[][]{booleanNullableTypes,booleanNullableTypes} );

    /**
     * Parameter type-checking strategy
     * type must be numeric.
     */
    public static final SqlOperator.AllowedArgInference typeNumeric =
        new SqlOperator.AllowedArgInference
                (new SqlTypeName[][]{numericTypes});

    /**
     * Parameter type-checking strategy
     * type must be a numeric literal.
     */
    public static final SqlOperator.AllowedArgInference typeNumericLiteral =
        new SqlOperator.AllowedArgInference
                (new SqlTypeName[][]{{SqlTypeName.Integer,SqlTypeName.Bigint,SqlTypeName.Decimal,
                                        SqlTypeName.Real,SqlTypeName.Double}}) {
            public boolean check(SqlCall call, SqlValidator validator,
                                 SqlValidator.Scope scope,
                                 SqlNode node, int operandNbr)
            {
                boolean res = super.check(call, validator, scope, node, operandNbr);
                if (!res) return res;
                if (operandNbr == 0) {
                    if (node instanceof SqlLiteral) {
                    } else {
                        throw SaffronResource.instance().newArgumentMustBeLiteral(
                            call.operator.name);
                    }
                }
                return res;
            }
        };

    /**
     * Parameter type-checking strategy
     * type must be a positive integer literal.
     */
    public static final SqlOperator.AllowedArgInference typePositiveIntegerLiteral =
        new SqlOperator.AllowedArgInference
                (new SqlTypeName[][]{{SqlTypeName.Integer}}) {

            public boolean check(SqlCall call, SqlValidator validator,
                                 SqlValidator.Scope scope,
                                 SqlNode node, int operandNbr)
            {
                boolean res = super.check(call, validator, scope, node, operandNbr);
                if (!res) return res;
                if (operandNbr == 0) {
                    if (node instanceof SqlLiteral) {
                        final SqlLiteral arg = ((SqlLiteral) node);
                        final int value = arg.intValue();
                        if (value < 0) {
                            throw SaffronResource.instance().newArgumentMustBePositiveInteger(
                                call.operator.name);
                        }
                    } else {
                        throw SaffronResource.instance().newArgumentMustBeLiteral(
                            call.operator.name);
                    }
                }
                return res;
            }
        };

        /**
         * Parameter type-checking strategy
         * type must be numeric, numeric.
         */
        public static final SqlOperator.AllowedArgInference typeNumericNumeric =
            new SqlOperator.AllowedArgInference
                    (new SqlTypeName[][]{numericTypes,numericTypes});


    /**
     * Parameter type-checking strategy
     * type must be nullable aType, nullable sameType
     */
    public static final SqlOperator.AllowedArgInference typeNullableSameSame =
        new SqlOperator.AllowedArgInference(new SqlTypeName[][]{{SqlTypeName.Any}, {SqlTypeName.Any}}) {
            public void check(
                    SqlValidator validator,
                    SqlValidator.Scope scope,
                    SqlCall call)
            {
                if (!check(call,validator,scope)){
                    throw validator.newValidationError("Parameters must be of same type");
                }
            }


            public boolean check(SqlCall call, SqlValidator validator,
                                 SqlValidator.Scope scope) {
                assert(2==call.operands.length);
                SaffronType type1 = validator.deriveType(scope,call.operands[0]);
                SaffronType type2 = validator.deriveType(scope,call.operands[1]);
                SaffronType nullType = validator.typeFactory.createSqlType(SqlTypeName.Null);
                if (type1.equals(nullType) || type2.equals(nullType)) {
                    return true; //null is ok;
                }

                return type1.isSameTypeFamily(type2) || type2.isSameTypeFamily(type1);
            }

        };

    /**
     * Parameter type-checking strategy
     * type must be nullable numeric, nullable numeric.
     */
    public static final SqlOperator.AllowedArgInference typeNullableNumericNumeric =
        new SqlOperator.AllowedArgInference(
            new SqlTypeName[][]{numericNullableTypes, numericNullableTypes} );

    /**
     * Parameter type-checking strategy
     * type must be nullable numeric, nullable numeric, nullabl numeric.
     */
    public static final SqlOperator.AllowedArgInference typeNullableNumericNumericNumeric =
        new SqlOperator.AllowedArgInference(
            new SqlTypeName[][]{
                numericNullableTypes, numericNullableTypes, numericNullableTypes} );

    /**
     * Parameter type-checking strategy
     * type must be nullable numeric, nullable numeric.
     */
    public static final SqlOperator.AllowedArgInference typeNullableBinariesBinaries =
        new SqlOperator.AllowedArgInference(
            new SqlTypeName[][]{binaryNullableTypes, binaryNullableTypes} );

    /**
     * Parameter type-checking strategy
     * type must be null | charstring | bitstring | hexstring
     */
    public static final SqlOperator.AllowedArgInference typeNullableString =
        new SqlOperator.AllowedArgInference(
            new SqlTypeName[][]{stringNullableTypes});

    /**
     * Parameter type-checking strategy
     * types must be null | charstring | bitstring | hexstring
     */
    public static final SqlOperator.AllowedArgInference typeNullableStringString =
        new SqlOperator.AllowedArgInference(
            new SqlTypeName[][]{stringNullableTypes,stringNullableTypes});

    /**
      * Parameter type-checking strategy
      * type must be a varchar literal.
      */
     public static final SqlOperator.AllowedArgInference typeVarcharLiteral =
         new SqlOperator.AllowedArgInference
                 (new SqlTypeName[][]{SqlOperatorTable.varcharTypes}) {

             public boolean check(SqlCall call, SqlValidator validator,
                                  SqlValidator.Scope scope,
                                  SqlNode node, int operandNbr)
             {
                 boolean res = super.check(call, validator, scope, node, operandNbr);
                 if (!res) return res;
                 if (operandNbr == 0) {
                     if (node instanceof SqlLiteral) {
                     } else {
                         throw SaffronResource.instance().newArgumentMustBeLiteral(
                             call.operator.name);
                     }
                 }
                 return res;
             }
         };

    /**
      * Parameter type-checking strategy
      * type must be nullable varchar, varchar literal.
      */
     public static final SqlOperator.AllowedArgInference typeNullableVarcharVarcharLiteral =
         new SqlOperator.AllowedArgInference
                 (new SqlTypeName[][]{SqlOperatorTable.varcharNullableTypes, SqlOperatorTable.varcharTypes}) {

             public void check(SqlValidator validator, SqlValidator.Scope scope, SqlCall call) {
                 // check that the 2nd argument is a varchar literal
                 super.check(validator, scope, call);
                 if (call.getOperands()[1] instanceof SqlLiteral) {
                 } else {
                     throw SaffronResource.instance().newArgumentMustBeLiteral(
                         call.operator.name);
                 }
             }
         };

     /**
     * Parameter type-checking strategy
     * types must be null | charstring | bitstring | hexstring
     * AND types must be identical to eachother
     */
    public static final SqlOperator.AllowedArgInference typeNullableStringStringOfSameType =
        new SqlOperator.AllowedArgInference(
            new SqlTypeName[][]{stringNullableTypes,stringNullableTypes}) {

            public String getAllowedSignatures(SqlOperator op) {
                StringBuffer ret=new StringBuffer();
                for (int i = 0; i < m_types[0].length; i++) {
                    if (m_types[0][i].getOrdinal()==SqlTypeName.Null_ordinal) {
                        continue;
                    }

                    ArrayList list = new ArrayList(2);
                    list.add(m_types[0][i]); //adding same twice
                    list.add(m_types[0][i]); //adding same twice
                    ret.append(op.getSignature(list));

                    if ((i+1)<m_types[0].length){
                        ret.append(op.NL);
                    }
                }
                return ret.toString();
            }

            protected void getAllowedSignatures(int depth, ArrayList list, StringBuffer buf, SqlOperator op) {
                throw Util.needToImplement("should not be called unless implemented");
            }

            public void check(SqlValidator validator, SqlValidator.Scope scope, SqlCall call) {
                SaffronType t0 = validator.deriveType(scope,call.operands[0]);
                SaffronType t1 = validator.deriveType(scope,call.operands[1]);
                assert(null!=t0) : "should not be null";
                assert(null!=t1) : "should not be null";
                SaffronType nullType = validator.typeFactory.createSqlType(SqlTypeName.Null);
                if (!nullType.isAssignableFrom(t0, false) && !nullType.isAssignableFrom(t1, false)) {
                    if (!t0.isSameTypeFamily(t1)) {
                        throw call.newValidationSignatureError(validator,scope);
                    }
                }
                super.check(validator, scope, call);
            }
        };


    /**
     * Parameter type-checking strategy
     * types must be null | charstring | bitstring | hexstring
     */
    public static final SqlOperator.AllowedArgInference typeNullableStringStringString =
        new SqlOperator.AllowedArgInference(
            new SqlTypeName[][]{stringNullableTypes,stringNullableTypes,stringNullableTypes});

    /**
     * Parameter type-checking strategy
     * types must be
     * null | charstring | bitstring | hexstring
     * null | int
     */
    public static final SqlOperator.AllowedArgInference typeNullableStringInt=
        new SqlOperator.AllowedArgInference(
            new SqlTypeName[][]{stringNullableTypes, intNullableTypes});


    /**
     * Parameter type-checking strategy
     * types must be
     * null | charstring | bitstring | hexstring
     * null | int
     * null | int
     */
    public static final SqlOperator.AllowedArgInference typeNullableStringIntInt =
        new SqlOperator.AllowedArgInference(
            new SqlTypeName[][]{stringTypes, intNullableTypes,intNullableTypes});

    /**
     * Parameter type-checking strategy
     * 2 first types must be null | charstring | bitstring | hexstring
     * 3 type must be integer
     */
    public static final SqlOperator.AllowedArgInference typeNullableStringStringNotNullableInt =
        new SqlOperator.AllowedArgInference(
            new SqlTypeName[][]{stringTypes,stringTypes,intTypes});

    /**
     * Parameter type-checking strategy
     * 2 first types must be null | charstring | bitstring | hexstring
     * 3&4 type must be integer
     */
    public static final SqlOperator.AllowedArgInference typeNullableStringStringNotNullableIntInt  =
        new SqlOperator.AllowedArgInference(
            new SqlTypeName[][]{stringTypes,stringTypes,intTypes,intTypes});

    /**
         * Parameter type-checking strategy
         * type must be nullable numeric
         */
        public static final SqlOperator.AllowedArgInference typeNullableNumeric =
            new SqlOperator.AllowedArgInference(
                new SqlTypeName[][]{numericNullableTypes});


    /**
     * Parameter type-checking strategy
     * types must be varchar, int, int
     */
    public static final SqlOperator.AllowedArgInference typeVarcharIntInt =
        new SqlOperator.AllowedArgInference(
            new SqlTypeName[][]{varcharTypes,intTypes,intTypes} );

    /**
     * Parameter type-checking strategy
     * types must be [nullable] varchar, [nullable] int, [nullable] int
     */
    public static final SqlOperator.AllowedArgInference
                                                typeNullableVarcharIntInt =
        new SqlOperator.AllowedArgInference(
            new SqlTypeName[][]{varcharNullableTypes,
                                intNullableTypes,intNullableTypes} );

    /**
         * Parameter type-checking strategy
         * types must be varchar, int
         */
        public static final SqlOperator.AllowedArgInference typeVarcharInt =
            new SqlOperator.AllowedArgInference
                    (new SqlTypeName[][]{varcharTypes,intTypes} );

    /**
     * Parameter type-checking strategy
     * types must be [nullable] varchar, [nullable] int, [nullable] int
     */
    public static final SqlOperator.AllowedArgInference
                                            typeNullableVarcharVarcharVarchar =
        new SqlOperator.AllowedArgInference(
            new SqlTypeName[][]{varcharNullableTypes,varcharNullableTypes,
                                varcharNullableTypes} );

    /**
     * Parameter type-checking strategy
     * types must be nullable boolean
     */
    public static final SqlOperator.AllowedArgInference typeNullableBool =
        new SqlOperator.AllowedArgInference(new SqlTypeName[][]{booleanNullableTypes} );

    /**
     * Parameter type-checking strategy
     * types can be any,any
     */
    public static final SqlOperator.AllowedArgInference typeAnyAny =
        new SqlOperator.AllowedArgInference(new SqlTypeName[][]{{SqlTypeName.Any},{SqlTypeName.Any}} );

    /**
     * Parameter type-checking strategy
     * types can be any
     */
    public static final SqlOperator.AllowedArgInference typeAny =
        new SqlOperator.AllowedArgInference(new SqlTypeName[][]{{SqlTypeName.Any}} );

    /**
     * Parameter type-checking strategy
     * types must be varchar,varchar
     */
    public static final SqlOperator.AllowedArgInference typeVarcharVarchar =
        new SqlOperator.AllowedArgInference(new SqlTypeName[][]{varcharTypes,varcharTypes} );

    /**
     * Parameter type-checking strategy
     * types must be nullable varchar
     */
    public static final SqlOperator.AllowedArgInference typeNullableVarchar =
        new SqlOperator.AllowedArgInference(new SqlTypeName[][]{varcharNullableTypes} );


    /**
     * Parameter type-checking strategy
     * type must be
     * nullable aType, nullable aType
     * OR nullable numeric, nullable numeric.
     * OR nullable binary, nullable binary.
     */
    public static final SqlOperator.CompositeAllowedArgInference
            typeNullabeSameSame_or_NullableNumericNumeric_or_NullableBinariesBinaries =
        new SqlOperator.CompositeAllowedArgInference(
                new SqlOperator.AllowedArgInference[]{typeNullableSameSame,
                                                     typeNullableNumericNumeric,
                                                     typeNullableBinariesBinaries} );

    /**
     * Parameter type-checking strategy
     * type must be
     * nullable string, nullable string, nulalble string
     * OR nullable string, nullable numeric, nullable numeric.
     */
    public static final SqlOperator.CompositeAllowedArgInference
            typeNullabeStringStringString_or_NullableStringIntInt =
        new SqlOperator.CompositeAllowedArgInference(
                new SqlOperator.AllowedArgInference[]{typeNullableStringStringString,
                                                     typeNullableStringIntInt});

    /**
     * Parameter type-checking strategy
     * type must be
     * nullable String
     * OR nullable numeric
     */
    public static final SqlOperator.CompositeAllowedArgInference
            typeNullableString_or_NullableNumeric =
        new SqlOperator.CompositeAllowedArgInference(
                new SqlOperator.AllowedArgInference[]{typeNullableString,
                                                     typeNullableNumeric} );

     /**
     * Parameter type-checking strategy
     * type must a time interval
     */
    public static final SqlOperator.AllowedArgInference
            typeNullableInterval =
        new SqlOperator.AllowedArgInference(
                new SqlTypeName[][]{timeIntervalNullableTypes} );

    /**
     * Parameter type-checking strategy
     * type must a time interval
     */
    public static final SqlOperator.AllowedArgInference
            typeNullableIntervalInterval =
        new SqlOperator.AllowedArgInference(
                new SqlTypeName[][]{timeIntervalNullableTypes,timeIntervalNullableTypes} );


    //~ Instance fields -------------------------------------------------------


    //~ - ~ - ~ - ~ - ~ - ~ - ~ - ~ - ~ - ~ - ~ - ~ -
    private final MultiMap operators = new MultiMap();
    private final HashMap mapNameToOp = new HashMap();
    //~ Instance fields -------------------------------------------------------
    public final Set stringFuncNames = new LinkedHashSet();
    public final Set numericFuncNames = new LinkedHashSet();
    public final Set timeDateFuncNames = new LinkedHashSet();
    public final Set systemFuncNames = new LinkedHashSet();
    /**
     * Multi-map from function name to a list of functions with that name.
     */
    private final MultiMap mapNameToFunc = new MultiMap();


    //~ Constructors ----------------------------------------------------------

    protected SqlOperatorTable()
    {
    }

    /**
     * Call this method after constructing an operator table. It can't be
     * part of the constructor, because the sub-class' constructor needs to
     * complete first. 
     */
    public void init() {
        // Use reflection to register the expressions stored in public fields.
        Field [] fields = getClass().getFields();
        for (int i = 0; i < fields.length; i++) {
            Field field = fields[i];
            if (SqlFunction.class.isAssignableFrom(field.getType())) {
                try {
                    SqlFunction op = (SqlFunction) field.get(this);
                    if (op != null) {
                        register(op);
                    }
                } catch (IllegalArgumentException e) {
                    throw Util.newInternal(e,
                            "Error while initializing operator table");
                } catch (IllegalAccessException e) {
                    throw Util.newInternal(e,
                            "Error while initializing operator table");
                }
            } else if (SqlOperator.class.isAssignableFrom(field.getType())) {
                try {
                    SqlOperator op = (SqlOperator) field.get(this);
                    register(op);
                } catch (IllegalArgumentException e) {
                    throw Util.newInternal(
                        e,
                        "Error while initializing operator table");
                } catch (IllegalAccessException e) {
                    throw Util.newInternal(
                        e,
                        "Error while initializing operator table");
                }
            }
        }
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * Retrieves the singleton, creating it if necessary.
     *
     * @see #std
     */
    public static SqlOperatorTable instance()
    {
        if (instance == null) {
            // Use two-phase construction, because we can't intialize the
            // tables until the constructor of the sub-class has completed.
            instance = new SqlStdOperatorTable();
            instance.init();
        }
        return instance;
    }

    /**
     * Returns the singleton instance of the table of standard operators.
     */
    public static SqlStdOperatorTable std() {
        return (SqlStdOperatorTable) instance();
    }

    /**
     * Retrieves an operator by its name and syntactic type.
     */
    public SqlOperator lookup(String opName,int syntax)
    {
        final List list = operators.getMulti(opName.toUpperCase());
        for (int i = 0,n = list.size(); i < n; i++) {
            SqlOperator op = (SqlOperator) list.get(i);
            if (op.getSyntax() == syntax) {
                return op;
            }
        }
        switch (syntax) {
        case SqlOperator.Syntax.Binary:
            return (SqlBinaryOperator) mapNameToOp.get(opName + ":BINARY");
        case SqlOperator.Syntax.Prefix:
            return (SqlPrefixOperator) mapNameToOp.get(opName + ":PREFIX");
        case SqlOperator.Syntax.Postfix:
            return (SqlPostfixOperator) mapNameToOp.get(opName + ":POSTFIX");
        case SqlOperator.Syntax.Function:
            throw Util.newInternal("Use lookupFunction to lookup function");
        default:
            throw SqlOperator.Syntax.instance.badValue(syntax);
        }
    }

    public void register(SqlOperator op)
    {
        operators.putMulti(op.name,op);
        if (op instanceof SqlBinaryOperator) {
            mapNameToOp.put(op.name + ":BINARY", op);
        } else if (op instanceof SqlPrefixOperator) {
            mapNameToOp.put(op.name + ":PREFIX", op);
        } else if (op instanceof SqlPostfixOperator) {
            mapNameToOp.put(op.name + ":POSTFIX", op);
        }
    }

    /**
     * Converts a list of {expression, operator, expression, ...} into a tree,
     * taking operator precedence and associativity into account.
     *
     * @pre list.size() % 2 == 1
     */
    public SqlNode toTree(List list)
    {
        // Make several passes over the list, and each pass, coalesce the
        // expressions with the highest precedence.
        while (list.size() > 1) {
            final int count = list.size();
            int i = 1;
            while (i < count) {
                SqlOperator previous;
                SqlOperator current = (SqlOperator) list.get(i);
                SqlOperator next;
                int previousRight;
                int left = current.leftPrec;
                int right = current.rightPrec;
                int nextLeft;
                if (current instanceof SqlBinaryOperator) {
                    if (i == 1) {
                        previous = null;
                        previousRight = 0;
                    } else {
                        previous = (SqlOperator) list.get(i - 2);
                        previousRight = previous.rightPrec;
                    }
                    if (i == (count - 2)) {
                        next = null;
                        nextLeft = 0;
                    } else {
                        next = (SqlOperator) list.get(i + 2);
                        nextLeft = next.leftPrec;
                    }
                    if ((previousRight < left) && (right >= nextLeft)) {
                        // For example,
                        //    i:  0 1 2 3 4 5 6 7 8
                        // list:  a + b * c * d + e
                        // prec: 0 1 2 3 4 3 4 1 2 0
                        //
                        // At i == 3, we have the first '*' operator, and its
                        // surrounding precedences obey the relation 2 < 3 and
                        // 4 >= 3, so we can reduce (b * c) to a single node.
                        SqlNode leftExp = (SqlNode) list.get(i - 1);

                        // For example,
                        //    i:  0 1 2 3 4 5 6 7 8
                        // list:  a + b * c * d + e
                        // prec: 0 1 2 3 4 3 4 1 2 0
                        //
                        // At i == 3, we have the first '*' operator, and its
                        // surrounding precedences obey the relation 2 < 3 and
                        // 4 >= 3, so we can reduce (b * c) to a single node.
                        SqlNode rightExp = (SqlNode) list.get(i + 1);
                        final SqlCall newExp =
                            current.createCall(leftExp,rightExp);

                        // Replace elements {i - 1, i, i + 1} with the new
                        // expression.
                        list.remove(i + 1);
                        list.remove(i);
                        list.set(i - 1,newExp);
                        break;
                    }
                    i += 2;
                } else if (current instanceof SqlPostfixOperator) {
                    if (i == 1) {
                        previous = null;
                        previousRight = 0;
                    } else {
                        previous = (SqlOperator) list.get(i - 2);
                        previousRight = previous.rightPrec;
                    }
                    if (previousRight < left) {
                        // For example,
                        //    i:  0 1 2 3 4 5 6 7 8
                        // list:  a + b * c ! + d
                        // prec: 0 1 2 3 4 3 0 2
                        //
                        // At i == 3, we have the postfix '!' operator. Its
                        // high precedence determines that it binds with 'b *
                        // c'. The precedence of the following '+' operator is
                        // irrelevant.
                        SqlNode leftExp = (SqlNode) list.get(i - 1);

                        final SqlCall newExp = current.createCall(leftExp);

                        // Replace elements {i - 1, i} with the new expression.
                        list.remove(i);
                        list.set(i - 1,newExp);
                        break;
                    }
                    ++i;
                }
                else if ((current instanceof SqlSpecialOperator) &&
                        (current.kind.isA(SqlKind.Between) ||
                        current.kind.isA(SqlKind.NotBetween))) {
                    // Since this is a special operator, we dont look at the pred for now
                    SqlNode firstAnd=null;
                    SqlNode secondAnd=null;
                    // Find the first BETWEEN's AND

                    int ii=i+1;
                    for ( /* empty */; ii < list.size(); ii++) {
                        Object o = list.get(ii);
                        if ((o instanceof SqlOperator) &&
                                ((SqlOperator)o).kind.isA(SqlKind.And)) {
                            ArrayList suicideList = new ArrayList(i);
                            suicideList.addAll(list.subList(i+1,ii));
                            firstAnd = toTree(suicideList); // 1st BETWEEN AND's operand
                            break;
                        }
                    }
                    // ii contains the position where first AND was found
                    // now search for the first (if exist) operator that has lower precedence than AND

                    int jj=++ii;
                    for ( /* empty */ ; jj < list.size(); jj++) {
                        Object o = list.get(jj);
                        if ((o instanceof SqlOperator) &&
                                (current.rightPrec>=((SqlOperator) o).leftPrec))     //TODO hack for now
                        {
                            break;
                        }
                    }

                    ArrayList suicideList = new ArrayList(list.size()-jj);
                    suicideList.addAll(list.subList(ii,jj));
                    secondAnd = toTree(suicideList); // 2nd BETWEEN AND's operand

                    SqlNode leftExp = (SqlNode) list.get(i - 1);
                    SqlCall newExp = std().betweenOperator.createCall(
                            leftExp, firstAnd, secondAnd);
                    if (current.kind.isA(SqlKind.NotBetween))
                    {
                        newExp = std().notOperator.createCall(newExp);
                    }

                    list.set(i - 1,newExp);

                    for( --jj; jj>=i;--jj) {
                        list.remove(jj);
                    }
                    break;
                }
                else {
                    throw Util.newInternal("Unexpected operator type: " +
                            current);
                }
            }
            // Require the list shrinks each time around -- otherwise we will
            // never terminate.
            assert list.size() < count;
        }
        return (SqlNode) list.get(0);
    }

    /**
     * Retrieves an operator by its id.
     */
    public SqlOperator lookup(SqlKind kind)
    {
        return null;
    }

    //~ Methods ---------------------------------------------------------------
    public static SqlCall createCall(String funName, SqlNode[] operands) {
        List funs = instance().lookupFunctionsByName(funName);
        if (!funs.isEmpty()){
            return ((SqlFunction) funs.get(0)).createCall(operands);
        }

        return  new SqlFunction(funName, null, null,null){
                    public void test(SqlTester tester) {
                        /* empty implementation */
                    }
                }.createCall(operands);
     }

    /**
     * Retrieves a list of overloading function by a given name.
     * @return If no function exists, null is returned,
     *         else retrieves a list of overloading function by a given name.
     */
    public List lookupFunctionsByName(String funcName) {
        return mapNameToFunc.getMulti(funcName);
    }

    /**
     * Register function to the table.
     * @param function
     */
    public void register(SqlFunction function) {
        mapNameToFunc.putMulti(function.name, function);
        SqlFunction.SqlFuncTypeName funcType = function.getFunctionType();
        assert (funcType != null) :
                "Function type for "+function.name+" not set";
        switch (funcType.getOrdinal()) {
        case SqlFunction.SqlFuncTypeName.String_ordinal:
            stringFuncNames.add(function.name);
            break;
        case SqlFunction.SqlFuncTypeName.Numeric_ordinal:
            numericFuncNames.add(function.name);
            break;
        case SqlFunction.SqlFuncTypeName.TimeDate_ordinal:
            timeDateFuncNames.add(function.name);
            break;
        case SqlFunction.SqlFuncTypeName.System_ordinal:
            systemFuncNames.add(function.name);
            break;
        }
    }

    private SqlFunction[] lookupFunctionsByNameAndArgCount(String name,
            int numberOfParams) {
        List funcList = mapNameToFunc.getMulti(name);
        if (funcList.isEmpty()) {
            return null;
        }

        List candidateList = new LinkedList();
        for (int i = 0; i < funcList.size(); i++) {
            SqlFunction function = (SqlFunction) funcList.get(i);
            List possibleNums = function.getPossibleNumOfOperands();
            if (possibleNums.contains(new Integer(numberOfParams))) {
                candidateList.add(function);
            }
        }
        return (SqlFunction[]) candidateList.toArray(
                new SqlFunction[candidateList.size()]);
    }

    /**
     * Chose the best fit function
     * @param funcName
     * @param argTypes
     * @return
     */
    public SqlFunction lookupFunction(String funcName, SaffronType[] argTypes) {
        // The number of defined parameters need to match the invocation
        SqlFunction[] functions = lookupFunctionsByNameAndArgCount(funcName,
                argTypes.length);
        if ((null == functions) || (0==functions.length)) {
            return null;
        } else if (functions.length == 1) {
            return functions[0];
        }

        ArrayList candidates = new ArrayList();
        for (int i = 0; i < functions.length; i++) {
            SqlFunction function = functions[i];
            if (function.isMatchParamType(argTypes)) {
                candidates.add(function);
            }
        }

        if (candidates.size() == 0) {
            return null;
        } else if (candidates.size() == 1) {
            return (SqlFunction) candidates.get(1);
        }

        // Next, consider each argument of the function invocation, from left to right. For each argument,
        // eliminate all functions that are not the best match for that argument. The best match for a given
        // argument is the first data type appearing in the precedence list corresponding to the argument data
        // type in Table 3 for which there exists a function with a parameter of that data type. Lengths,
        // precisions, scales and the "FOR BIT DATA" attribute are not considered in this comparison.
        // For example, a DECIMAL(9,1) argument is considered an exact match for a DECIMAL(6,5) parameter,
        // and a VARCHAR(19) argument is an exact match for a VARCHAR(6) parameter.
        // Reference: http://www.pdc.kth.se/doc/SP/manuals/db2-5.0/html/db2s0/db2s067.htm#HDRUDFSEL
        //
        for (int i = 0; i < argTypes.length; i++) {
            SaffronType argType = argTypes[i];
            throw Util.needToImplement("Function resolution with different types is not implemented yet.");
        }
        return null;
    }

    /**
     * Returns a list of all functions and operators in this table.
     * Used for automated testing.
     */
    public ArrayList getOperatorList() {
        ArrayList list = new ArrayList();

        Iterator it = operators.entryIterMulti();
        while (it.hasNext()) {
            Map.Entry mapEntry = (Map.Entry) it.next();
            SqlOperator operator = (SqlOperator) mapEntry.getValue();
            list.add(operator);
        }

        it = mapNameToFunc.entryIterMulti();
        while (it.hasNext()) {
            Map.Entry mapEntry = (Map.Entry) it.next();
            SqlFunction function   = (SqlFunction) mapEntry.getValue();
            list.add(function);
        }

        return list;
    }

}


// End SqlOperatorTable.java
