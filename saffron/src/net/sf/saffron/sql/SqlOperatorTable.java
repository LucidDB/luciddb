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

package net.sf.saffron.sql;

import net.sf.saffron.util.MultiMap;
import net.sf.saffron.util.Util;
import net.sf.saffron.core.SaffronType;
import net.sf.saffron.core.SaffronTypeFactory;
import net.sf.saffron.sql.type.SqlTypeName;

import java.lang.reflect.Field;

import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;


/**
 * <code>SqlOperatorTable</code> is a singleton which contains an instance of
 * each operator.
 */
public class SqlOperatorTable
{

    //~ Static fields/initializers --------------------------------------------

    private static SqlOperatorTable instance;

    /**
     * Type-inference strategy whereby the result type of a call is the type of
     * the first operand.
     */
    public static final SqlOperator.TypeInference useFirstArgType =
            new SqlOperator.TypeInference() {
                public SaffronType getType(SqlValidator validator,
                        SqlValidator.Scope scope, SqlCall call) {
                    return validator.deriveType(scope, call.operands[0]);
                }

                public SaffronType getType(SaffronTypeFactory typeFactory,
                        SaffronType[] argTypes) {
                    return argTypes[0];
                }
            };
    /**
     * Type-inference strategy whereby the result type of a call is Boolean.
     */
    public static final SqlOperator.TypeInference booleanType =
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
    public static final SqlOperator.TypeInference nullableBooleanType =
    new SqlOperator.TypeInference() {
        public SaffronType getType(
            SqlValidator validator,
            SqlValidator.Scope scope,
            SqlCall call)
        {
            SaffronType type = booleanType.getType(validator,scope,call);
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
            SaffronType type = booleanType.getType(typeFactory,argTypes);
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
    public static final SqlOperator.TypeInference doubleType =
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
    public static final SqlOperator.TypeInference integerType =
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
     * Type-inference strategy whereby the result type of a call is using its operands biggest type.
     * E.g (500000000000 + 3.0e-3) have the operands INTEGER and DOUBLE. Its biggest type is double
     */
    public static final SqlOperator.TypeInference useBiggest =
            new SqlOperator.TypeInference() {
                public SaffronType getType(SqlValidator validator,
                        SqlValidator.Scope scope, SqlCall call) {
                    SaffronType[] types = new SaffronType[call.operands.length];
                    for (int i = 0; i < call.operands.length; i++) {
                        types[i] = validator.deriveType(scope, call.operands[i]);
                    }
                    return getType(validator.typeFactory,types);
                }

                public SaffronType getType(SaffronTypeFactory typeFactory,
                        SaffronType[] argTypes) {
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
    public static final SqlOperator.AllowdArgInference typeNullableBoolBool =
        new SqlOperator.AllowdArgInference
                (new SqlTypeName[][]{{SqlTypeName.Null,SqlTypeName.Boolean},{SqlTypeName.Null,SqlTypeName.Boolean}} );

    /**
     * Parameter type-checking strategy
     * type must be numeric.
     */
    public static final SqlOperator.AllowdArgInference typeNumeric =
        new SqlOperator.AllowdArgInference
                (new SqlTypeName[][]{{SqlTypeName.Integer,SqlTypeName.Bigint,SqlTypeName.Decimal,
                                        SqlTypeName.Real,SqlTypeName.Double}});

    /**
         * Parameter type-checking strategy
         * type must be numeric, numeric.
         */
        public static final SqlOperator.AllowdArgInference typeNumericNumeric =
            new SqlOperator.AllowdArgInference
                    (new SqlTypeName[][]{{SqlTypeName.Integer,SqlTypeName.Bigint,SqlTypeName.Decimal,
                                            SqlTypeName.Real,SqlTypeName.Double},
                                         {SqlTypeName.Integer,SqlTypeName.Bigint,SqlTypeName.Decimal,
                                            SqlTypeName.Real,SqlTypeName.Double}});


    /**
     * Parameter type-checking strategy
     * type must be nullable aType, nullable sameType
     */
    public static final SqlOperator.AllowdArgInference typeNullableSameSame =
        new SqlOperator.AllowdArgInference(new SqlTypeName[][]{{SqlTypeName.Any}, {SqlTypeName.Any}}) {
            public void check(
                    SqlValidator validator,
                    SqlValidator.Scope scope,
                    SqlCall call)
            {
                assert(2==call.operands.length);
                SaffronType type1 = validator.deriveType(scope,call.operands[0]);
                SaffronType type2 = validator.deriveType(scope,call.operands[1]);
                SaffronType nullType = validator.typeFactory.createSqlType(SqlTypeName.Null);
                if (type1.equals(nullType) || type2.equals(nullType)) {
                    return; //null is ok;
                }

                if (!type1.isSameTypeFamily(type2)){
                    throw validator.newValidationError("Parameters must be of same type");
                }
            }

        };

    /**
     * Parameter type-checking strategy
     * type must be nullable numeric, nullable numeric.
     */
    public static final SqlOperator.AllowdArgInference typeNullableNumericNumeric =
        new SqlOperator.AllowdArgInference
        (new SqlTypeName[][]{{SqlTypeName.Null,SqlTypeName.Integer,SqlTypeName.Bigint,SqlTypeName.Decimal,
                                               SqlTypeName.Real,SqlTypeName.Double},
                             {SqlTypeName.Null,SqlTypeName.Integer,SqlTypeName.Bigint,SqlTypeName.Decimal,
                                               SqlTypeName.Real,SqlTypeName.Double}} );
    /**
     * Parameter type-checking strategy
     * type must be nullable numeric, nullable numeric.
     */
    public static final SqlOperator.AllowdArgInference typeNullableBinariesBinaries =
        new SqlOperator.AllowdArgInference
        (new SqlTypeName[][]{{SqlTypeName.Null,SqlTypeName.Bit,SqlTypeName.Varbinary},
                             {SqlTypeName.Null,SqlTypeName.Bit,SqlTypeName.Varbinary}} );

    /**
         * Parameter type-checking strategy
         * type must be nullable numeric
         */
        public static final SqlOperator.AllowdArgInference typeNullableNumeric =
            new SqlOperator.AllowdArgInference
                    (new SqlTypeName[][]{{SqlTypeName.Null,SqlTypeName.Integer,SqlTypeName.Bigint,SqlTypeName.Decimal,
                                                           SqlTypeName.Real,SqlTypeName.Double}});


    /**
     * Parameter type-checking strategy
     * types must be varchar, int, int
     */
    public static final SqlOperator.AllowdArgInference typeVarcharIntInt =
        new SqlOperator.AllowdArgInference
                (new SqlTypeName[][]{{SqlTypeName.Varchar},{SqlTypeName.Integer},{SqlTypeName.Integer}} );

    /**
     * Parameter type-checking strategy
     * types must be varchar, int
     */
    public static final SqlOperator.AllowdArgInference typeVarcharInt =
        new SqlOperator.AllowdArgInference
                (new SqlTypeName[][]{{SqlTypeName.Varchar},{SqlTypeName.Integer}} );

    /**
     * Parameter type-checking strategy
     * types must be nullable boolean
     */
    public static final SqlOperator.AllowdArgInference typeNullableBool =
        new SqlOperator.AllowdArgInference
                (new SqlTypeName[][]{{SqlTypeName.Null,SqlTypeName.Boolean}} );

    /**
     * Parameter type-checking strategy
     * types can be any,any
     */
    public static final SqlOperator.AllowdArgInference typeAnyAny =
        new SqlOperator.AllowdArgInference
                (new SqlTypeName[][]{{SqlTypeName.Any},{SqlTypeName.Any}} );

    /**
     * Parameter type-checking strategy
     * types can be any
     */
    public static final SqlOperator.AllowdArgInference typeAny =
        new SqlOperator.AllowdArgInference
                (new SqlTypeName[][]{{SqlTypeName.Any}} );


    /**
     * Parameter type-checking strategy
     * types must be varchar,varchar
     */
    public static final SqlOperator.AllowdArgInference typeVarcharVarchar =
        new SqlOperator.AllowdArgInference
                (new SqlTypeName[][]{{SqlTypeName.Varchar},{SqlTypeName.Varchar}} );

    /**
     * Parameter type-checking strategy
     * types must be nullable varchar
     */
    public static final SqlOperator.AllowdArgInference typeNullableVarchar =
        new SqlOperator.AllowdArgInference
                (new SqlTypeName[][]{{SqlTypeName.Null,SqlTypeName.Varchar}} );


    /**
     * Parameter type-checking strategy
     * type must be
     * nullable aType, nullable aType
     * OR nullable numeric, nullable numeric.
     * OR nullable binary, nullable binary.
     */
    public static final SqlOperator.CompositeAllowdArgInference
            //todo wael: if anyone have a compact and easy to understand nomenclature, let me know
            typeNullabeSameSame_or_NullableNumericNumeric_or_NullableBinariesBinaries =
        new SqlOperator.CompositeAllowdArgInference(
                new SqlOperator.AllowdArgInference[]{typeNullableSameSame,
                                                     typeNullableNumericNumeric,
                                                     typeNullableBinariesBinaries} );

    //~ Instance fields -------------------------------------------------------

    // infix
    public final SqlBinaryOperator andOperator =
        new SqlBinaryOperator("AND",SqlKind.And,14,true,
                                  nullableBooleanType,booleanParam, typeNullableBoolBool);

    public final SqlBinaryOperator asOperator =
        new SqlBinaryOperator("AS",SqlKind.As,10,true,
                              useFirstArgType,useReturnForParam, typeAnyAny) {
            protected void checkArgTypes(
                    SqlCall call, SqlValidator validator, SqlValidator.Scope scope) {
                /* empty implementation */
            }
        };

    // FIXME jvs 23-Dec-2003:  useFirstKnownParam is incorrect here;
    // have to promote CHAR to VARCHAR
    public final SqlBinaryOperator concatOperator =
        new SqlBinaryOperator("||",SqlKind.Other,30,true,
                              null,useFirstKnownParam, typeVarcharVarchar);

    public final SqlBinaryOperator divideOperator =
        new SqlBinaryOperator("/",SqlKind.Divide,30,true,
                              useBiggest,useFirstKnownParam, typeNumericNumeric);

    public final SqlBinaryOperator dotOperator =
        new SqlBinaryOperator(".",SqlKind.Dot,40,true, null, null, typeAnyAny);

    public final SqlBinaryOperator equalsOperator =
        new SqlBinaryOperator("=",SqlKind.Equals,15,true,
                              nullableBooleanType,useFirstKnownParam,
                              typeNullabeSameSame_or_NullableNumericNumeric_or_NullableBinariesBinaries);

    public final SqlSetOperator exceptOperator =
        new SqlSetOperator("EXCEPT",SqlKind.Except,9,false);

    public final SqlSetOperator exceptAllOperator =
        new SqlSetOperator("EXCEPT ALL",SqlKind.Except,9,true);

    public final SqlBinaryOperator greaterThanOperator =
        new SqlBinaryOperator(">",SqlKind.GreaterThan,15,true,
                              nullableBooleanType,useFirstKnownParam, typeNullableNumericNumeric);

    public final SqlBinaryOperator greaterThanOrEqualOperator =
        new SqlBinaryOperator(">=",SqlKind.GreaterThanOrEqual,15,true,
                              nullableBooleanType,useFirstKnownParam, typeNullableNumericNumeric);

    public final SqlBinaryOperator inOperator =
        new SqlBinaryOperator("IN",SqlKind.In,15,true,
              nullableBooleanType,useFirstKnownParam, null);


    public final SqlBinaryOperator overlapsOperator =
            //TODO: wael, check types
        new SqlBinaryOperator("OVERLAPS",SqlKind.Overlaps,15,true,nullableBooleanType,useFirstKnownParam, null);

    public final SqlSetOperator intersectOperator =
        new SqlSetOperator("INTERSECT",SqlKind.Intersect,9,false);

    public final SqlSetOperator intersectAllOperator =
        new SqlSetOperator("INTERSECT ALL",SqlKind.Intersect,9,true);

    public final SqlBinaryOperator lessThanOperator =
        new SqlBinaryOperator("<",SqlKind.LessThan,15,true,
                              nullableBooleanType,useFirstKnownParam, typeNullableNumericNumeric);

    public final SqlBinaryOperator lessThanOrEqualOperator =
        new SqlBinaryOperator("<=",SqlKind.LessThanOrEqual,15,true,
                              nullableBooleanType,useFirstKnownParam, typeNullableNumericNumeric);

    public final SqlBinaryOperator minusOperator =
        new SqlBinaryOperator("-",SqlKind.Minus,20,true,
                              useBiggest,useFirstKnownParam, typeNullableNumericNumeric);

    public final SqlBinaryOperator multiplyOperator =
        new SqlBinaryOperator("*",SqlKind.Times,30,true,
                              useBiggest,useFirstKnownParam, typeNullableNumericNumeric);

    public final SqlBinaryOperator notEqualsOperator =
        new SqlBinaryOperator("<>",SqlKind.NotEquals,15,true,
                              nullableBooleanType,useFirstKnownParam,
                              typeNullabeSameSame_or_NullableNumericNumeric_or_NullableBinariesBinaries);

    public final SqlBinaryOperator orOperator =
        new SqlBinaryOperator("OR",SqlKind.Or,13,true,
                              nullableBooleanType,booleanParam, typeNullableBoolBool);

    // todo: useFirstArgType isn't correct in general
    public final SqlBinaryOperator plusOperator =
        new SqlBinaryOperator("+",SqlKind.Plus,20,true,
                              useBiggest,useFirstKnownParam, typeNullableNumericNumeric);

    public final SqlSetOperator unionOperator =
        new SqlSetOperator("UNION",SqlKind.Union,7,false);

    public final SqlSetOperator unionAllOperator =
        new SqlSetOperator("UNION ALL",SqlKind.Union,7,true);

    public final SqlBinaryOperator isDistinctFromOperator =
        new SqlBinaryOperator("IS DISTINCT FROM",SqlKind.Other,15,true,nullableBooleanType,useFirstKnownParam, null);

    // function
//    public final SqlFunction substringFunction =
//    new SqlFunction("substring",null,null, null);

    public final SqlFunction rowConstructor =
            new SqlFunction("ROW",SqlKind.Row,null,useReturnForParam, null) {

                protected void checkNumberOfArg(AllowdArgInference[] parameterTypes, SqlCall call) {
                    // ang number of argument is fine
                }

                protected void checkArgTypes(SqlCall call, SqlValidator validator, SqlValidator.Scope scope) {
                    // any arg types are fine
                }

                protected SaffronType inferType(SqlValidator validator,
                                                SqlValidator.Scope scope, SqlCall call) {
                    // The type of a ROW(e1,e2) expression is a record with the
                    // types {e1type,e2type}.
                    final String [] fieldNames = new String[call.operands.length];
                    final SaffronType [] types = new SaffronType[call.operands.length];
                    for (int i = 0; i < call.operands.length; i++) {
                        SqlNode operand = call.operands[i];
                        fieldNames[i] = validator.deriveAlias(operand, i);
                        types[i] = validator.deriveType(scope,operand);
                    }
                    return validator.typeFactory.createProjectType(types, fieldNames);
                }
            };

    /**
     * The SQL <code>CAST</code> operator.
     *
     * <p/>The target type is simply stored as
     * the return type, not an explicit operand. For example, the expression
     * <code>CAST(1 + 2 AS DOUBLE)</code> will become a call to
     * <code>CAST</code> with the expression <code>1 + 2</code> as its only
     * operand.
     */
    public final SqlFunction cast = new SqlFunction(
        "CAST",null,useReturnForParam, null);

    // postfix
    public final SqlPostfixOperator descendingOperator =
        new SqlPostfixOperator("DESC",SqlKind.Descending,10,
                               null,useReturnForParam,typeAny);

    public final SqlPostfixOperator isNotNullOperator =
        new SqlPostfixOperator("IS NOT NULL",SqlKind.Other,15,
                               booleanType,booleanParam,typeAny);

    public final SqlPostfixOperator isNullOperator =
        new SqlPostfixOperator("IS NULL",SqlKind.IsNull,15,
                               booleanType,booleanParam,typeAny);


	public final SqlPostfixOperator isNotTrueOperator =
	        new SqlPostfixOperator("IS NOT TRUE",SqlKind.Other,15,
	                               booleanType,booleanParam,typeNullableBool);

	public final SqlPostfixOperator isTrueOperator =
	        new SqlPostfixOperator("IS TRUE",SqlKind.IsTrue,15,
	                               booleanType,booleanParam,typeNullableBool);

	public final SqlPostfixOperator isNotFalseOperator =
	        new SqlPostfixOperator("IS NOT FALSE",SqlKind.Other,15,
	                               booleanType,booleanParam,typeNullableBool);

	public final SqlPostfixOperator isFalseOperator =
	        new SqlPostfixOperator("IS FALSE",SqlKind.IsFalse,15,
	                               booleanType,booleanParam,typeNullableBool);

    // prefix
    public final SqlPrefixOperator existsOperator =
        new SqlPrefixOperator("EXISTS",SqlKind.Exists,20,
                              booleanType,null, typeNullableBool);

    public final SqlPrefixOperator notOperator =
        new SqlPrefixOperator("NOT",SqlKind.Not,15,
                              nullableBooleanType,booleanParam, typeNullableBool);

    public final SqlPrefixOperator prefixMinusOperator =
        new SqlPrefixOperator("-",SqlKind.MinusPrefix,20,
                              useFirstArgType,useReturnForParam, typeNullableNumeric);

    public final SqlPrefixOperator prefixPlusOperator =
        new SqlPrefixOperator("+",SqlKind.PlusPrefix,20,
                              useFirstArgType,useReturnForParam, typeNullableNumeric);

    public final SqlPrefixOperator explicitTableOperator =
        new SqlPrefixOperator("TABLE",SqlKind.ExplicitTable,1,null,null,null);

    // special
    public final SqlSpecialOperator valuesOperator =
        new SqlSpecialOperator("VALUES",SqlKind.Values) {
            void unparse(
                    SqlWriter writer,
                    SqlNode[] operands,
                    int leftPrec,
                    int rightPrec) {
                writer.print("VALUES ");
                for (int i = 0; i < operands.length; i++) {
                    if (i > 0) {
                        writer.print(", ");
                    }
                    SqlNode operand = operands[i];
                    operand.unparse(writer, 0, 0);
                }
            }
        };

    public final SqlSpecialOperator betweenOperator =
        new SqlSpecialOperator("BETWEEN",SqlKind.Between,15) {
            void unparse(
                    SqlWriter writer,
                    SqlNode[] operands,
                    int leftPrec,
                    int rightPrec) {
                operands[0].unparse(writer, this.leftPrec, this.rightPrec );
                writer.print(" BETWEEN ");
                operands[1].unparse(writer, this.leftPrec, this.rightPrec );
                writer.print(" AND ");
                operands[2].unparse(writer, this.leftPrec, this.rightPrec );
            }
        };

    public final SqlSpecialOperator notBetweenOperator =
        new SqlSpecialOperator("NOT BETWEEN",SqlKind.NotBetween,15);


    public final SqlSpecialOperator likeOperator = new SqlLikeOperator("LIKE",SqlKind.Like);
    public final SqlSpecialOperator similarOperator = new SqlLikeOperator("SIMILAR",SqlKind.Similar);
    public final SqlSelectOperator selectOperator = new SqlSelectOperator();
    public final SqlJoinOperator joinOperator = new SqlJoinOperator();
    public final SqlSpecialOperator insertOperator =
    new SqlSpecialOperator("INSERT",SqlKind.Insert);
    public final SqlSpecialOperator deleteOperator =
    new SqlSpecialOperator("DELETE",SqlKind.Delete);
    public final SqlSpecialOperator updateOperator =
    new SqlSpecialOperator("UPDATE",SqlKind.Update);
    public final SqlSpecialOperator explainOperator =
    new SqlSpecialOperator("EXPLAIN",SqlKind.Explain);
    public final SqlOrderByOperator orderByOperator = new SqlOrderByOperator();

    private final MultiMap operators = new MultiMap();
    private final HashMap mapNameToOp = new HashMap();

    private final SqlFunctionTable funcTable = SqlFunctionTable.instance();

    //~ Constructors ----------------------------------------------------------

    protected SqlOperatorTable()
    {
        // Use reflection to register the expressions stored in public fields.
        Field [] fields = getClass().getFields();
        for (int i = 0; i < fields.length; i++) {
            Field field = fields[i];
            if (SqlOperator.class.isAssignableFrom(field.getType())) {
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
     */
    public static SqlOperatorTable instance()
    {
        if (instance == null) {
            instance = new SqlOperatorTable();
        }
        return instance;
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
            throw Util.newInternal("Use SqlFunctionTable to lookup function");
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

                        final SqlCall newExp;
                        if (current.equals(isNotNullOperator)) {
                            newExp = notOperator.createCall(isNullOperator.createCall(leftExp));
                        } else if (current.equals(isNotTrueOperator)) {
                            newExp = notOperator.createCall(isTrueOperator.createCall(leftExp));
                        } else if (current.equals(isNotFalseOperator)) {
                            newExp = notOperator.createCall(isFalseOperator.createCall(leftExp));
                        } else {
                            newExp = current.createCall(leftExp);
                        }

                        // Replace elements {i - 1, i} with the new expression.
                        list.remove(i);
                        list.set(i - 1,newExp);
                        break;
                    }
                    ++i;
                }
                else if ((current instanceof SqlSpecialOperator) &&
                         (current.isA(SqlKind.Between) || current.isA(SqlKind.NotBetween)))
                {
                    // Since this is a special operator, we dont look at the pred for now
                    SqlNode firstAnd=null;
                    SqlNode secondAnd=null;
                    // Find the first BETWEEN's AND

                    int ii=i+1;
                    for ( /* empty */; ii < list.size(); ii++) {
                        Object o = list.get(ii);
                        if ((o instanceof SqlOperator) && ((SqlOperator)o).isA(SqlKind.And)) {
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
                    SqlCall newExp = betweenOperator.createCall(leftExp, firstAnd, secondAnd);
                    if (current.isA(SqlKind.NotBetween))
                    {
                        newExp = notOperator.createCall(newExp);
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

}


// End SqlOperatorTable.java
