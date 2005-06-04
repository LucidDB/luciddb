/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
package org.eigenbase.sql.type;

import org.eigenbase.resource.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.reltype.*;
import org.eigenbase.util.*;

import java.util.*;

/**
 * SqlTypeStrategies defines singleton instances of strategy objects for
 * operand type checking (member prefix otc), operand type inference (member
 * prefix oti), and operator return type inference (member prefix rti).
 *
 *<p>
 *
 * NOTE: avoid anonymous inner classes here except for unique,
 * non-generalizable strategies; anything else belongs in a reusable top-level
 * class.  If you find yourself copying and pasting an existing strategy's
 * anonymous inner class, you're making a mistake.
 *
 * @author Wael Chatila
 * @version $Id$
 */
public abstract class SqlTypeStrategies
{
    // ----------------------------------------------------------------------
    // SqlOperandTypeChecker definitions
    // ----------------------------------------------------------------------
    
    /**
     * Parameter type-checking strategy for a function which takes no
     * arguments.
     */
    public static final SqlSingleOperandTypeChecker
        otcEmpty =
        new ExplicitOperandTypeChecker(new SqlTypeName[][] {});

    /**
     * Parameter type-checking strategy for an operator with
     * no restrictions on number or type of operands.
     */
    public static final SqlOperandTypeChecker
        otcVariadic =
        new SqlOperandTypeChecker()
        {
            public boolean checkCall(
                SqlValidator validator,
                SqlValidatorScope scope,
                SqlCall call,
                boolean throwOnFailure)
            {
                return true;
            }

            public SqlOperandCountRange getOperandCountRange()
            {
                return SqlOperandCountRange.Variadic;
            }

            public String getAllowedSignatures(SqlOperator op)
            {
                return "";
            }
        };
        
    /**
     * Parameter type-checking strategy
     * type must be nullable boolean, nullable boolean.
     */
    public static final SqlSingleOperandTypeChecker
        otcNullableBoolX2 =
        new ExplicitOperandTypeChecker(new SqlTypeName [][] {
            SqlTypeName.booleanNullableTypes, SqlTypeName.booleanNullableTypes
        });

    /**
     * Parameter type-checking strategy
     * type must be numeric.
     */
    public static final SqlSingleOperandTypeChecker
        otcNumeric =
        new ExplicitOperandTypeChecker(new SqlTypeName [][] {
            SqlTypeName.numericTypes });

    /**
     * Parameter type-checking strategy
     * type must be a literal. NULL literal allowed
     */
    public static final SqlSingleOperandTypeChecker
        otcNullableLit =
        new LiteralOperandTypeChecker(true);

    /**
     * Parameter type-checking strategy
     * type must be a literal, but NOT a NULL literal.
     * <code>CAST(NULL as ...)</code> is considered to be a NULL literal but not
     * <code>CAST(CAST(NULL as ...) AS ...)</code>
     */
    public static final SqlSingleOperandTypeChecker
        otcNotNullLit =
        new LiteralOperandTypeChecker(false);

    /**
     * Parameter type-checking strategy
     * type must be a positive integer literal. Null literal not allowed
     */
    public static final SqlSingleOperandTypeChecker
        otcPositiveIntLit =
        new ExplicitOperandTypeChecker(new SqlTypeName [][] {
            SqlTypeName.intTypes
        })
        {
            public boolean checkOperand(
                SqlCall call,
                SqlValidator validator,
                SqlValidatorScope scope,
                SqlNode node,
                int iFormalOperand, boolean throwOnFailure)
            {
                if (!otcNotNullLit.checkOperand(call, validator, scope, node,
                    iFormalOperand, throwOnFailure)) {
                    return false;
                }

                if (!super.checkOperand(call, validator, scope, node,
                    iFormalOperand, throwOnFailure)) {
                    return false;
                }

                final SqlLiteral arg = ((SqlLiteral) node);
                final int value = arg.intValue();
                if (value < 0) {
                    if (throwOnFailure) {
                        throw EigenbaseResource.instance()
                            .newArgumentMustBePositiveInteger(
                                call.getOperator().getName());
                    }
                    return false;
                }
                return true;
            }
        };

    /**
     * Parameter type-checking strategy
     * type must be numeric, numeric.
     */
    public static final SqlSingleOperandTypeChecker
        otcNumericX2 =
        new ExplicitOperandTypeChecker(new SqlTypeName [][] {
            SqlTypeName.numericTypes, SqlTypeName.numericTypes
        });

    /**
     * Parameter type-checking strategy
     * type must be integer.
     */
    public static final SqlSingleOperandTypeChecker
        otcInt =
        new ExplicitOperandTypeChecker(new SqlTypeName [][] {
            SqlTypeName.intTypes
        });

    /**
     * Parameter type-checking strategy
     * type must be integer, integer.
     */
    public static final SqlSingleOperandTypeChecker
        otcIntX2 =
        new ExplicitOperandTypeChecker(new SqlTypeName [][] {
            SqlTypeName.intTypes, SqlTypeName.intTypes
        });

    /**
     * Parameter type-checking strategy
     * type must be nullable integer, nullable integer. (exact types)
     */
    public static final SqlSingleOperandTypeChecker
        otcNullableIntX2 =
        new ExplicitOperandTypeChecker(new SqlTypeName [][] {
            SqlTypeName.intNullableTypes, SqlTypeName.intNullableTypes
        });

    /**
     * Parameter type-checking strategy
     * type must be nullable aType, nullable sameFamily
     */
    public static final SqlOperandTypeChecker
        otcNullableSameX2 =
        new SameOperandTypeChecker(
            new SqlTypeName [][] {
                { SqlTypeName.Any },
                { SqlTypeName.Any }
            });

    /**
     * Parameter type-checking strategy
     * type must be nullable aType, nullable sameFamily, nullable sameFamily
     */
    public static final SqlOperandTypeChecker
        otcNullableSameX3 =
        new SameOperandTypeChecker(
            new SqlTypeName [][] {
                { SqlTypeName.Any },
                { SqlTypeName.Any },
                { SqlTypeName.Any }
            });

    /**
     * Parameter type-checking strategy where types must allow
     * ordered comparisons.
     */
    public static final SqlOperandTypeChecker
        otcComparableOrdered =
        new ComparableOperandTypeChecker(
            RelDataTypeComparability.All);

    /**
     * Parameter type-checking strategy where types must allow
     * unordered comparisons.
     */
    public static final SqlOperandTypeChecker
        otcComparableUnordered =
        new ComparableOperandTypeChecker(
            RelDataTypeComparability.Unordered);

    /**
     * Parameter type-checking strategy
     * type must be nullable numeric, nullable numeric.
     */
    public static final SqlSingleOperandTypeChecker
        otcNullableNumericX2 =
        new ExplicitOperandTypeChecker(new SqlTypeName [][] {
            SqlTypeName.numericNullableTypes, SqlTypeName.numericNullableTypes
        });

    /**
     * Parameter type-checking strategy
     * type must be nullable numeric, nullable numeric, nullabl numeric.
     */
    public static final SqlSingleOperandTypeChecker
        otcNullableNumericX3 =
        new ExplicitOperandTypeChecker(new SqlTypeName [][] {
            SqlTypeName.numericNullableTypes, SqlTypeName.numericNullableTypes,
            SqlTypeName.numericNullableTypes
        });

    /**
     * Parameter type-checking strategy
     * type must be nullable binary, nullable binary.
     */
    public static final SqlSingleOperandTypeChecker
        otcNullableBinaryX2 =
        new ExplicitOperandTypeChecker(new SqlTypeName [][] {
            SqlTypeName.binaryNullableTypes, SqlTypeName.binaryNullableTypes
        });

    /**
     * Parameter type-checking strategy
     * type must be nullable binary, nullable binary, nullable binary.
     */
    public static final SqlSingleOperandTypeChecker
        otcNullableBinaryX3 =
        new ExplicitOperandTypeChecker(new SqlTypeName [][] {
            SqlTypeName.binaryNullableTypes,
            SqlTypeName.binaryNullableTypes,
            SqlTypeName.binaryNullableTypes
        });

    /**
     * Parameter type-checking strategy
     * type must be null | charstring | bitstring | hexstring
     */
    public static final SqlSingleOperandTypeChecker
        otcNullableString =
        new ExplicitOperandTypeChecker(new SqlTypeName [][] {
            SqlTypeName.stringNullableTypes
        });

    /**
     * Parameter type-checking strategy
     * types must be null | charstring | bitstring | hexstring
     */
    public static final SqlSingleOperandTypeChecker
        otcNullableStringX2 =
        new ExplicitOperandTypeChecker(new SqlTypeName [][] {
            SqlTypeName.stringNullableTypes, SqlTypeName.stringNullableTypes
        });

    /**
     * Parameter type-checking strategy
     * type must be a varchar literal.
     */
    public static final SqlSingleOperandTypeChecker
        otcVarcharLit =
        new CompositeOperandTypeChecker(
            CompositeOperandTypeChecker.AND, 
            new SqlOperandTypeChecker[] {
                new ExplicitOperandTypeChecker(
                    new SqlTypeName[][]{SqlTypeName.charTypes}),
                otcNotNullLit
            });

    /**
     * Parameter type-checking strategy
     * type must be a nullable varchar literal.
     */
    public static final SqlSingleOperandTypeChecker
        otcNullableVarcharLit =
        new CompositeOperandTypeChecker(
            CompositeOperandTypeChecker.AND, 
            new SqlOperandTypeChecker[] {
                new ExplicitOperandTypeChecker(
                    new SqlTypeName[][]{SqlTypeName.charNullableTypes}),
                otcNullableLit
            });

    /**
     * Parameter type-checking strategy type must be nullable varchar, NOT
     * nullable varchar literal.  the expression <code>CAST(NULL AS
     * TYPE)</code> is considered a NULL literal.
     */
    public static final SqlSingleOperandTypeChecker
        otcNullableVarcharNotNullVarcharLit =
        new ExplicitOperandTypeChecker(new SqlTypeName [][] {
            SqlTypeName.charNullableTypes, SqlTypeName.charTypes
        }) {
            public boolean checkCall(
                SqlValidator validator,
                SqlValidatorScope scope,
                SqlCall call, boolean throwOnFailure)
            {
                //checking if char types
                if (!super.checkCall(validator, scope, call, throwOnFailure)) {
                    return false;
                }

                // check that the 2nd argument is a literal
                return otcNotNullLit.checkOperand(call,validator, scope,
                    call.operands[1],0, throwOnFailure);
            }
        };

    /**
     * Parameter type-checking strategy
     * types must be null | charstring | bitstring | hexstring
     * AND types must be identical to eachother
     */
    public static final SqlSingleOperandTypeChecker
        otcNullableStringSameX2 =
        new CompositeOperandTypeChecker(
            CompositeOperandTypeChecker.AND, 
            new SqlOperandTypeChecker[] {
                new ExplicitOperandTypeChecker(new SqlTypeName [][] {
                    SqlTypeName.stringNullableTypes,
                    SqlTypeName.stringNullableTypes
                }),
                otcNullableSameX2
            });

    /**
     * Parameter type-checking strategy
     * types must be null | charstring | bitstring | hexstring
     * AND types must be identical to eachother
     */
    public static final SqlSingleOperandTypeChecker
        otcNullableStringSameX3 =
        new CompositeOperandTypeChecker(
            CompositeOperandTypeChecker.AND, 
            new SqlOperandTypeChecker[] {
                new ExplicitOperandTypeChecker(new SqlTypeName [][] {
                    SqlTypeName.stringNullableTypes,
                    SqlTypeName.stringNullableTypes,
                    SqlTypeName.stringNullableTypes
                }),
                otcNullableSameX3
            });

    /**
     * Parameter type-checking strategy
     * types must be null | charstring | bitstring | hexstring
     */
    public static final SqlSingleOperandTypeChecker
        otcNullableStringX3 =
        new ExplicitOperandTypeChecker(new SqlTypeName [][] {
            SqlTypeName.stringNullableTypes, SqlTypeName.stringNullableTypes,
            SqlTypeName.stringNullableTypes
        });

    /**
     * Parameter type-checking strategy
     * types must be
     * null | charstring | bitstring | hexstring
     * null | int
     */
    public static final SqlSingleOperandTypeChecker
        otcNullableStringInt =
        new ExplicitOperandTypeChecker(new SqlTypeName [][] {
            SqlTypeName.stringNullableTypes, SqlTypeName.intNullableTypes
        });

    /**
     * Parameter type-checking strategy
     * types must be
     * null | charstring | bitstring | hexstring
     * null | int
     * null | int
     */
    public static final SqlSingleOperandTypeChecker
        otcNullableStringIntX2 =
        new ExplicitOperandTypeChecker(new SqlTypeName [][] {
            SqlTypeName.stringTypes, SqlTypeName.intNullableTypes,
            SqlTypeName.intNullableTypes
        });

    /**
     * Parameter type-checking strategy
     * 2 first types must be null | charstring | bitstring | hexstring
     * 3 type must be integer
     */
    public static final SqlSingleOperandTypeChecker
        otcNullableStringX2NotNullInt =
        new ExplicitOperandTypeChecker(new SqlTypeName [][] {
            SqlTypeName.stringTypes, SqlTypeName.stringTypes,
            SqlTypeName.intTypes
        });

    /**
     * Parameter type-checking strategy
     * 2 first types must be null | charstring | bitstring | hexstring
     * 3&4 type must be integer
     */
    public static final SqlSingleOperandTypeChecker
        otcNullableStringX2NotNullIntX2 =
        new ExplicitOperandTypeChecker(new SqlTypeName [][] {
            SqlTypeName.stringTypes, SqlTypeName.stringTypes,
            SqlTypeName.intTypes,
            SqlTypeName.intTypes
        });

    /**
     * Parameter type-checking strategy
     * type must be nullable numeric
     */
    public static final SqlSingleOperandTypeChecker
        otcNullableNumeric =
        new ExplicitOperandTypeChecker(new SqlTypeName [][] {
            SqlTypeName.numericNullableTypes
        });

    /**
     * Parameter type-checking strategy
     * types must be varchar, int, int
     */
    public static final SqlSingleOperandTypeChecker
        otcVarcharIntX2 =
        new ExplicitOperandTypeChecker(new SqlTypeName [][] {
            SqlTypeName.charTypes, SqlTypeName.intTypes, SqlTypeName.intTypes
        });

    /**
     * Parameter type-checking strategy
     * types must be [nullable] varchar, [nullable] int, [nullable] int
     */
    public static final SqlSingleOperandTypeChecker
        otcNullableVarcharIntX2 =
        new ExplicitOperandTypeChecker(new SqlTypeName [][] {
            SqlTypeName.charNullableTypes, SqlTypeName.intNullableTypes,
            SqlTypeName.intNullableTypes
        });

    /**
     * Parameter type-checking strategy
     * types must be not nullable varchar, not nullable int
     */
    public static final SqlSingleOperandTypeChecker
        otcVarcharInt =
        new ExplicitOperandTypeChecker(new SqlTypeName [][] {
            SqlTypeName.charTypes, SqlTypeName.intTypes
        });

    /**
     * Parameter type-checking strategy
     * types must be [nullable] varchar, [nullable] varchar,
     */
    public static final SqlSingleOperandTypeChecker
        otcNullableVarcharX2 =
        new ExplicitOperandTypeChecker(new SqlTypeName [][] {
            SqlTypeName.charNullableTypes, SqlTypeName.charNullableTypes
        });

    /**
     * Parameter type-checking strategy
     * types must be [nullable] varchar, [nullable] varchar, [nullable] varchar
     */
    public static final SqlSingleOperandTypeChecker
        otcNullableVarcharX3 =
        new ExplicitOperandTypeChecker(new SqlTypeName [][] {
            SqlTypeName.charNullableTypes, SqlTypeName.charNullableTypes,
            SqlTypeName.charNullableTypes
        });

    /**
     * Parameter type-checking strategy
     * types must be nullable boolean
     */
    public static final SqlSingleOperandTypeChecker
        otcNullableBool =
        new ExplicitOperandTypeChecker(new SqlTypeName [][] {
            SqlTypeName.booleanNullableTypes
        });

    /**
     * Parameter type-checking strategy
     * types can be any,any
     */
    public static final SqlSingleOperandTypeChecker
        otcAnyX2 =
        new ExplicitOperandTypeChecker(new SqlTypeName [][] {
            { SqlTypeName.Any },
            { SqlTypeName.Any }
        });

    /**
     * Parameter type-checking strategy
     * types can be any
     */
    public static final SqlSingleOperandTypeChecker
        otcAny =
        new ExplicitOperandTypeChecker(new SqlTypeName [][] {
            { SqlTypeName.Any }
        });

    /**
     * Parameter type-checking strategy
     * types must be varchar,varchar
     */
    public static final SqlSingleOperandTypeChecker
        otcVarcharX2 =
        new ExplicitOperandTypeChecker(new SqlTypeName [][] {
            SqlTypeName.charTypes, SqlTypeName.charTypes
        });

    /**
     * Parameter type-checking strategy
     * types must be nullable varchar
     */
    public static final SqlSingleOperandTypeChecker
        otcNullableVarchar =
        new ExplicitOperandTypeChecker(new SqlTypeName [][] {
            SqlTypeName.charNullableTypes
        });

    /**
     * Parameter type-checking strategy
     * type must be
     * positive integer literal
     * OR varchar literal
     */
    public static final SqlSingleOperandTypeChecker
        otcPositiveIntOrVarcharLit =
        new CompositeOperandTypeChecker(
            CompositeOperandTypeChecker.OR,
            new SqlOperandTypeChecker [] {
                otcPositiveIntLit, otcVarcharLit
        });


    /**
     * Parameter type-checking strategy
     * type must a nullable datetime type
     * A datetime type is either a TIME, DATE or TIMESTAMP,
     * TIME WITH TIME ZONE, TIMESTAMP WITH TIMEZONE
     * NOTE: timezone types not yet implemented
     */
    public static final SqlSingleOperandTypeChecker
        otcNullableDatetime =
        new ExplicitOperandTypeChecker(new SqlTypeName [][] {
            SqlTypeName.datetimeNullableTypes
        });
    
    /**
     *  Parameter type checking for the datetime type but
     * not nullable.
     */
    public static final SqlSingleOperandTypeChecker
        otcDatetime =
        new ExplicitOperandTypeChecker(new SqlTypeName[][]{
            SqlTypeName.datetimeTypes
        });

    /**
     * Parameter type-checking strategy
     * type must a nullable time interval, nullable time interval
     */
    public static final SqlSingleOperandTypeChecker
        otcNullableDatetimeX2 =
        new CompositeOperandTypeChecker(
            CompositeOperandTypeChecker.AND, 
            new SqlOperandTypeChecker[] {
                new ExplicitOperandTypeChecker(
                    new SqlTypeName[][]{
                        SqlTypeName.datetimeNullableTypes,
                        SqlTypeName.datetimeNullableTypes}),
                otcNullableSameX2
            });

    /**
     * Parameter type-checking strategy
     * type must a nullable time interval
     */
    public static final SqlSingleOperandTypeChecker
        otcNullableInterval =
        new ExplicitOperandTypeChecker(new SqlTypeName [][] {
            SqlTypeName.timeIntervalNullableTypes
        });

    /**
     * Parameter type-checking strategy
     * type must a nullable time interval, nullable time interval
     */
    public static final SqlSingleOperandTypeChecker
        otcNullableIntervalX2 =
        new CompositeOperandTypeChecker(
            CompositeOperandTypeChecker.AND, 
            new SqlOperandTypeChecker[] {
                new ExplicitOperandTypeChecker(
                    new SqlTypeName[][]{
                        SqlTypeName.timeIntervalNullableTypes,
                        SqlTypeName.timeIntervalNullableTypes}),
                otcNullableSameX2
            });

    public static final SqlSingleOperandTypeChecker
        otcNullableNumericInterval =
        new ExplicitOperandTypeChecker(new SqlTypeName [][] {
            SqlTypeName.numericNullableTypes,
            SqlTypeName.timeIntervalNullableTypes
        });

    public static final SqlSingleOperandTypeChecker
        otcNullableIntervalNumeric =
        new ExplicitOperandTypeChecker(new SqlTypeName [][] {
            SqlTypeName.timeIntervalNullableTypes,
            SqlTypeName.numericNullableTypes
        });

    public static final SqlSingleOperandTypeChecker
        otcNullableDatetimeInterval =
        new CompositeOperandTypeChecker(
            CompositeOperandTypeChecker.AND, 
            new SqlOperandTypeChecker[] {
                 otcNullableDatetime,
                 otcNullableInterval
            });

    /**
     * Type checking strategy for the "+" operator
     */
    public static final SqlSingleOperandTypeChecker
        otcPlusOperator =
        new CompositeOperandTypeChecker(
            CompositeOperandTypeChecker.OR,
            new SqlOperandTypeChecker[] {
                  otcNullableNumericX2,
                  otcNullableIntervalX2
                  //todo datetime+interval checking missing
                  //todo interval+datetime checking missing
            });

    /**
     * Type checking strategy for the "*" operator
     */
    public static final SqlSingleOperandTypeChecker
        otcMultiplyOperator =
        new CompositeOperandTypeChecker(
            CompositeOperandTypeChecker.OR,
            new SqlOperandTypeChecker[] {
                  otcNullableNumericX2,
                  otcNullableIntervalNumeric,
                  otcNullableNumericInterval
            });

    /**
     * Type checking strategy for the "/" operator
     */
    public static final SqlSingleOperandTypeChecker
        otcDivisionOperator =
        new CompositeOperandTypeChecker(
            CompositeOperandTypeChecker.OR,
            new SqlOperandTypeChecker[] {
                  otcNullableNumericX2,
                  otcNullableIntervalNumeric
            });

    public static final SqlSingleOperandTypeChecker
        otcMinusOperator =
        new CompositeOperandTypeChecker(
            CompositeOperandTypeChecker.OR,
            new SqlOperandTypeChecker[] {
                  otcNullableNumericX2,
                  otcNullableIntervalX2
//todo                , otcNullableDatetimeInterval
            });

    public static final SqlSingleOperandTypeChecker
        otcMinusDateOperator =
        new ExplicitOperandTypeChecker(new SqlTypeName [][] {
            SqlTypeName.datetimeNullableTypes,
            SqlTypeName.datetimeNullableTypes,
            SqlTypeName.timeIntervalNullableTypes
        }) {
            public boolean checkCall(
                SqlValidator validator,
                SqlValidatorScope scope,
                SqlCall call,
                boolean throwOnFailure) {
                if (!super.checkCall(validator, scope, call, throwOnFailure)) {
                    return false;
                }
                if (!otcNullableSameX2.checkCall(
                        validator, scope, call, throwOnFailure))
                {
                    return false;
                }
                return true;
            }
        };

    public static final SqlSingleOperandTypeChecker
        otcNullableNumericOrInterval =
        new CompositeOperandTypeChecker(
            CompositeOperandTypeChecker.OR,
            new SqlOperandTypeChecker[] {
                  otcNullableNumeric,
                  otcNullableInterval
            });

    /**
     * Parameter type-checking strategy where types can be nullable and must be
     * comparable to each other with unordered comparisons allowed.
     */
    public static final SqlSingleOperandTypeChecker
        otcNullableComparableUnordered =
        new CompositeOperandTypeChecker(
            CompositeOperandTypeChecker.AND, 
            new SqlOperandTypeChecker [] {
                otcNullableSameX2, otcComparableUnordered
        });
    
    /**
     * Parameter type-checking strategy where types can be nullable and must be
     * comparable to each other with ordered comparisons allowed.
     */
    public static final SqlSingleOperandTypeChecker
        otcNullableComparableOrdered =
        new CompositeOperandTypeChecker(
            CompositeOperandTypeChecker.AND, 
            new SqlOperandTypeChecker [] {
                otcNullableSameX2, otcComparableOrdered
        });
    
    /**
     * Parameter type-checking strategy
     * type must a nullable multiset
     */
    public static final SqlSingleOperandTypeChecker
        otcNullableMultiset =
        new ExplicitOperandTypeChecker(new SqlTypeName [][] {
            SqlTypeName.multisetNullableTypes
        });

    public static final SqlSingleOperandTypeChecker
        otcNullableRecordMultiset =
        new SqlSingleOperandTypeChecker() {
            public boolean checkOperand(
                SqlCall call,
                SqlValidator validator,
                SqlValidatorScope scope,
                SqlNode node,
                int iFormalOperand,
                boolean throwOnFailure)
            {
                assert(0 == iFormalOperand);
                RelDataType type = validator.deriveType(scope, node);
                boolean validationError = false;
                if (!type.isStruct()) {
                    validationError = true;
                } else if (type.getFieldList().size() != 1) {
                    validationError = true;
                } else if (!SqlTypeName.Multiset.equals(
                    type.getFields()[0].getType().getSqlTypeName()))
                {
                    validationError = true;
                }

                if (validationError && throwOnFailure) {
                    throw call.newValidationSignatureError(validator, scope);
                }
                return !validationError;
            }

            public boolean checkCall(
                SqlValidator validator,
                SqlValidatorScope scope,
                SqlCall call,
                boolean throwOnFailure)
            {
                return checkOperand(call,
                             validator,
                             scope,
                             call.operands[0],
                             0,
                             throwOnFailure);
            }

            public SqlOperandCountRange getOperandCountRange()
            {
                return SqlOperandCountRange.One;
            }

            public String getAllowedSignatures(SqlOperator op)
            {
                return "UNNEST(<MULTISET>)";
            }
        };

    public static final SqlSingleOperandTypeChecker
        otcNullableMultisetOrRecordTypeMultiset =
        new CompositeOperandTypeChecker(
            CompositeOperandTypeChecker.OR,
            new SqlOperandTypeChecker[]{
                otcNullableMultiset,otcNullableRecordMultiset});

    /**
     * Parameter type-checking strategy
     * types must be
     * [nullable] Multiset, [nullable] Multiset
     * and the two types must have the same element type
     * @see {@link MultisetSqlType#getComponentType}
     */
    public static final SqlOperandTypeChecker
        otcNullableMultisetX2 =
        new MultisetOperandTypeChecker();

    /**
     * Parameter type-checking strategy for a set operator (UNION, INTERSECT,
     * EXCEPT).
     */
    public static final SqlOperandTypeChecker
        otcSetop =
        new SetopOperandTypeChecker();
    
    // ----------------------------------------------------------------------
    // SqlReturnTypeInference definitions
    // ----------------------------------------------------------------------
    
    /**
     * Type-inference strategy whereby the result type of a call is the type of
     * the first operand.
     */
    public static final SqlReturnTypeInference
        rtiFirstArgType =
        new OrdinalReturnTypeInference(0);

    /**
     * Type-inference strategy whereby the result type of a call is the type of
     * the first operand. If any of the other operands are nullable the returned
     * type will also be nullable.
     */
    public static final SqlReturnTypeInference
        rtiNullableFirstArgType =
        new SqlTypeTransformCascade(
            rtiFirstArgType, SqlTypeTransforms.toNullable);

    public static final SqlReturnTypeInference
        rtiFirstInterval =
        new MatchReturnTypeInference(0, SqlTypeName.timeIntervalTypes);

    public static final SqlReturnTypeInference
        rtiNullableFirstInterval =
        new SqlTypeTransformCascade(
            rtiFirstInterval, SqlTypeTransforms.toNullable);

    /**
     * Type-inference strategy whereby the result type of a call is VARYING
     * the type of the first argument.
     * The length returned is the same as length of the
     * first argument.
     * If any of the other operands are nullable the returned
     * type will also be nullable.
     * First Arg must be of string type.
     */
    public static final SqlReturnTypeInference
        rtiNullableVaryingFirstArgType =
        new SqlTypeTransformCascade(
            rtiFirstArgType,
            SqlTypeTransforms.toNullable,
            SqlTypeTransforms.toVarying);

    /**
     * Type-inference strategy whereby the result type of a call is the type of
     * the second operand.
     */
    public static final SqlReturnTypeInference
        rtiSecondArgType =
        new OrdinalReturnTypeInference(1);

    /**
     * Type-inference strategy whereby the result type of a call is the type of
     * the third operand.
     */
    public static final SqlReturnTypeInference
        rtiThirdArgType =
        new OrdinalReturnTypeInference(2);

    /**
     * Type-inference strategy whereby the result type of a call is Boolean.
     */
    public static final SqlReturnTypeInference
        rtiBoolean =
        new ExplicitReturnTypeInference(SqlTypeName.Boolean);

    /**
     * Type-inference strategy whereby the result type of a call is Boolean,
     * with nulls allowed if any of the operands allow nulls.
     */
    public static final SqlReturnTypeInference
        rtiNullableBoolean =
        new SqlTypeTransformCascade(
            rtiBoolean, SqlTypeTransforms.toNullable);

    /**
     * Type-inference strategy whereby the result type of a call is Date.
     */
    public static final SqlReturnTypeInference
        rtiDate =
        new ExplicitReturnTypeInference(SqlTypeName.Date);

    /**
     * Type-inference strategy whereby the result type of a call is Time(0).
     */
    public static final SqlReturnTypeInference
        rtiTime =
        new ExplicitReturnTypeInference(SqlTypeName.Time, 0);

    /**
     * Type-inference strategy whereby the result type of a call is nullable
     * Time(0).
     */
     public static final SqlReturnTypeInference
         rtiNullableTime =
        new SqlTypeTransformCascade(
            rtiTime, SqlTypeTransforms.toNullable);

    /**
     * Type-inference strategy whereby the result type of a call is Double.
     */
    public static final SqlReturnTypeInference
        rtiDouble =
        new ExplicitReturnTypeInference(SqlTypeName.Double);

    /**
     * Type-inference strategy whereby the result type of a call is Double
     * with nulls allowed if any of the operands allow nulls.
     */
    public static final SqlReturnTypeInference
        rtiNullableDouble =
        new SqlTypeTransformCascade(
            rtiDouble, SqlTypeTransforms.toNullable);

    /**
     * Type-inference strategy whereby the result type of a call is an Integer.
     */
    public static final SqlReturnTypeInference
        rtiInteger =
        new ExplicitReturnTypeInference(SqlTypeName.Integer);

    /**
     * Type-inference strategy whereby the result type of a call is an Integer
     * with nulls allowed if any of the operands allow nulls.
     */
    public static final SqlReturnTypeInference
        rtiNullableInteger =
        new SqlTypeTransformCascade(
            rtiInteger, SqlTypeTransforms.toNullable);

    /**
     * Type-inference strategy which always returns "VARCHAR(2000)".
     */
    public static final SqlReturnTypeInference
        rtiVarchar2000 =
        new ExplicitReturnTypeInference(SqlTypeName.Varchar, 2000);

    /**
     * Type-inference strategy whereby the result type of a call is using its
     * operands biggest type, using the SQL:1999 rules described in
     * "Data types of results of aggregations".
     * These rules are used in union, except, intercept, case and other places.
     *
     * @sql.99 Part 2 Section 9.3
     *
     * <p>For example, the expression <code>(500000000000 + 3.0e-3)</code> has
     * the operands INTEGER and DOUBLE. Its biggest type is double.
     */
    public static final SqlReturnTypeInference
        rtiLeastRestrictive =
        new SqlReturnTypeInference() {
            public RelDataType getType(
                SqlValidator validator,
                SqlValidatorScope scope,
                RelDataTypeFactory typeFactory,
                CallOperands callOperands)
            {
                // REVIEW jvs 1-Mar-2004: I changed this to
                // leastRestrictive since that's its purpose (and at least
                // for Farrago, works better than the old getBiggest code).
                // But this type inference rule isn't general enough for
                // numeric types, which use different rules depending on
                // the operator (e.g. sum length is based on the max of
                // the arg precisions, while product length is based on
                // the sum of the arg precisions).
                return typeFactory.leastRestrictive(
                            callOperands.collectTypes());
            }
        };

    /**
     * Same as {@link #rtiLeastRestrictive} but with INTERVAL as well.
     */
    public static final SqlReturnTypeInference
        rtiBiggest =
        new SqlTypeTransformCascade(
            rtiLeastRestrictive, SqlTypeTransforms.toLeastRestrictiveInterval);

    /**
     * Type-inference strategy similar to {@link #rtiBiggest}, except that the
     * result is nullable if any of the arguments is nullable.
     */
    public static final SqlReturnTypeInference
        rtiNullableBiggest =
        new SqlTypeTransformCascade(
            rtiLeastRestrictive,
            SqlTypeTransforms.toLeastRestrictiveInterval,
            SqlTypeTransforms.toNullable);

    public static final SqlReturnTypeInference
        rtiNullableProduct =
        new SqlReturnTypeInferenceChain(
            rtiNullableFirstInterval, rtiNullableBiggest);

    /**
     * Type-inference strategy where by the
     * Result type of a call is
     * <ul>
     * <li>the same type as the input types but with the
     * combined length of the two first types</li>
     * <li>If types are of char type the type with the highest coercibility
     * will be used</li>
     *
     * @pre input types must be of the same string type
     * @pre types must be comparable without casting
     */
    public static final SqlReturnTypeInference
        rtiDyadicStringSumPrecision =
        new SqlReturnTypeInference()
        {
            /**
             * @pre SqlTypeUtil.sameNamedType(argTypes[0], (argTypes[1]))
             */
            public RelDataType getType(
                SqlValidator validator,
                SqlValidatorScope scope,
                RelDataTypeFactory typeFactory,
                CallOperands callOperands)
            {
                if (!(SqlTypeUtil.inCharFamily(callOperands.getType(0))
                        && SqlTypeUtil.inCharFamily(callOperands.getType(1))))
                {
                    Util.pre(
                        SqlTypeUtil.sameNamedType(
                            callOperands.getType(0), callOperands.getType(1)),
                        "SqlTypeUtil.sameNamedType(argTypes[0], argTypes[1])");
                }
                SqlCollation pickedCollation = null;
                if (SqlTypeUtil.inCharFamily(callOperands.getType(0))) {
                    if (!SqlTypeUtil.isCharTypeComparable(
                            callOperands.collectTypes(), 0, 1)) {
                        throw EigenbaseResource.instance()
                            .newTypeNotComparable(
                                callOperands.getType(0).toString(),
                                callOperands.getType(1).toString());
                    }

                    pickedCollation =
                        SqlCollation.getCoercibilityDyadicOperator(
                            callOperands.getType(0).getCollation(),
                            callOperands.getType(1).getCollation());
                    assert (null != pickedCollation);
                }

                RelDataType ret;
                ret = typeFactory.createSqlType(
                        callOperands.getType(0).getSqlTypeName(),
                        callOperands.getType(0).getPrecision()
                        + callOperands.getType(1).getPrecision());
                if (null != pickedCollation) {
                    RelDataType pickedType;
                    if (callOperands.getType(0).getCollation().equals(
                            pickedCollation))
                    {
                        pickedType = callOperands.getType(0);
                    } else if (callOperands.getType(1).getCollation().equals(
                                   pickedCollation))
                    {
                        pickedType = callOperands.getType(1);
                    } else {
                        throw Util.newInternal("should never come here");
                    }
                    ret = typeFactory.createTypeWithCharsetAndCollation(
                            ret,
                            pickedType.getCharset(),
                            pickedType.getCollation());
                }
                return ret;
            }
        };

    /**
     * Same as {@link #rtiDyadicStringSumPrecision} and using
     * {@link #toNullable}
     */
    public static final SqlReturnTypeInference
        rtiNullableDyadicStringSumPrecision =
        new SqlTypeTransformCascade(
            rtiDyadicStringSumPrecision,
            new SqlTypeTransform [] { SqlTypeTransforms.toNullable });

    /**
     * Same as {@link #rtiDyadicStringSumPrecision} and using
     * {@link #toNullable}, {@link #toVarying}
     */
    public static final SqlReturnTypeInference
        rtiNullableVaryingDyadicStringSumPrecision =
        new SqlTypeTransformCascade(
            rtiDyadicStringSumPrecision,
            new SqlTypeTransform [] {
                SqlTypeTransforms.toNullable, SqlTypeTransforms.toVarying
            });

    /**
     * Type-inference strategy where the expression is assumed to be registered
     * as a {@link org.eigenbase.sql.validate.SqlValidatorNamespace}, and
     * therefore the result type of the call is the type of that namespace.
     */
    public static final SqlReturnTypeInference
        rtiScope =
        new SqlReturnTypeInference()
        {
            public RelDataType getType(
                SqlValidator validator,
                SqlValidatorScope scope,
                RelDataTypeFactory typeFactory,
                CallOperands callOperands)
            {
                return validator.getNamespace(
                    (SqlNode) callOperands.getUnderlyingObject()).getRowType();
            }

            public RelDataType getType(
                RelDataTypeFactory typeFactory,
                RelDataType [] argTypes)
            {
                throw new UnsupportedOperationException();
            }
        };

    /**
     * Returns the same type as the multiset carries. The multiset type returned
     * is the least restrictive of the call's multiset operands
     */
    public static final SqlReturnTypeInference
        rtiMultiset =
        new SqlReturnTypeInference()
        {
            public RelDataType getType(
                SqlValidator validator,
                SqlValidatorScope scope,
                RelDataTypeFactory typeFactory,
                CallOperands callOperands)
            {
                RelDataType[] argElementTypes =
                    new RelDataType[callOperands.size()];
                for (int i = 0; i < callOperands.size(); i++) {
                    argElementTypes[i] =
                        callOperands.getType(i).getComponentType();
                }

                CallOperands.RelDataTypesCallOperands types = new
                    CallOperands.RelDataTypesCallOperands(argElementTypes);
                RelDataType biggestElementType = rtiBiggest.
                    getType(validator, scope,typeFactory, types);
                return  typeFactory.createMultisetType(biggestElementType, -1);
            }
        };

    /**
     * Same as {@link #rtiMultiset} but returns with nullablity
     * if any of the operands is nullable
     */
    public static final SqlReturnTypeInference
        rtiNullableMultiset =
        new SqlTypeTransformCascade(
            rtiMultiset, SqlTypeTransforms.toNullable);

    /**
     * Returns the element type of a multiset
     */
    public static final SqlReturnTypeInference
        rtiNullableMultisetElementType =
        new SqlTypeTransformCascade(
            rtiMultiset, SqlTypeTransforms.toMultisetElementType);
    
    // ----------------------------------------------------------------------
    // SqlOperandTypeInference definitions
    // ----------------------------------------------------------------------
    
    /**
     * Operand type-inference strategy where an unknown operand
     * type is derived from the first operand with a known type.
     */
    public static final SqlOperandTypeInference
        otiFirstKnown =
        new SqlOperandTypeInference()
        {
            public void inferOperandTypes(
                SqlValidator validator,
                SqlValidatorScope scope,
                SqlCall call,
                RelDataType returnType,
                RelDataType [] operandTypes)
            {
                SqlNode [] operands = call.getOperands();
                final RelDataType unknownType = validator.getUnknownType();
                RelDataType knownType = unknownType;
                for (int i = 0; i < operands.length; ++i) {
                    knownType = validator.deriveType(scope, operands[i]);
                    if (!knownType.equals(unknownType)) {
                        break;
                    }
                }
                assert !knownType.equals(unknownType);
                for (int i = 0; i < operandTypes.length; ++i) {
                    operandTypes[i] = knownType;
                }
            }
        };
    
    /**
     * Operand type-inference strategy where an unknown operand
     * type is derived from the call's return type.  If the return type is
     * a record, it must have the same number of fields as the number
     * of operands.
     */
    public static final SqlOperandTypeInference
        otiReturnType =
        new SqlOperandTypeInference()
        {
            public void inferOperandTypes(
                SqlValidator validator,
                SqlValidatorScope scope,
                SqlCall call,
                RelDataType returnType,
                RelDataType [] operandTypes)
            {
                for (int i = 0; i < operandTypes.length; ++i) {
                    if (returnType.isStruct()) {
                        operandTypes[i] = returnType.getFields()[i].getType();
                    } else {
                        operandTypes[i] = returnType;
                    }
                }
            }
        };
    
    /**
     * Operand type-inference strategy where an unknown operand
     * type is assumed to be boolean.
     */
    public static final SqlOperandTypeInference
        otiBoolean =
        new SqlOperandTypeInference()
        {
            public void inferOperandTypes(
                SqlValidator validator,
                SqlValidatorScope scope,
                SqlCall call,
                RelDataType returnType,
                RelDataType [] operandTypes)
            {
                RelDataTypeFactory typeFactory = validator.getTypeFactory();
                for (int i = 0; i < operandTypes.length; ++i) {
                    operandTypes[i] =
                        typeFactory.createSqlType(SqlTypeName.Boolean);
                }
            }
        };
}

// End SqlTypeStrategies.java
