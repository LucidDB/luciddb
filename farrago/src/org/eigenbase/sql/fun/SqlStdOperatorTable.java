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

package org.eigenbase.sql.fun;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.sql.type.SqlTypeStrategies;
import org.eigenbase.sql.util.ReflectiveSqlOperatorTable;

/**
 * Implementation of {@link org.eigenbase.sql.SqlOperatorTable} containing the
 * standard operators and functions.
 *
 * @author jhyde
 * @since May 28, 2004
 * @version $Id$
 **/
public class SqlStdOperatorTable extends ReflectiveSqlOperatorTable
{
    //~ Instance fields -------------------------------------------
    /**
     * The standard operator table.
     */
    private static SqlStdOperatorTable instance;


    //~ INNER CLASSES ------------------------------------------
    /**
     * Returns the standard operator table, creating it if necessary.
     */
    public static synchronized SqlStdOperatorTable instance()
    {
        if (instance == null) {
            // Creates and initializes the standard operator table.
            // Uses two-phase construction, because we can't intialize the
            // table until the constructor of the sub-class has completed.
            instance = new SqlStdOperatorTable();
            instance.init();
        }
        return instance;
    }

    //~ OPERATORS AND FUNCTIONS -----------------------------------

    //-------------------------------------------------------------
    //                   SET OPERATORS
    //-------------------------------------------------------------
    // The set operators can be compared to the arthimetic operators
    // UNION -> +
    // EXCEPT -> -
    // INTERSECT -> *
    // which explains the different precedence values
    public static final SqlSetOperator unionOperator =
        new SqlSetOperator("UNION", SqlKind.Union, 7, false);

    public static final SqlSetOperator unionAllOperator =
        new SqlSetOperator("UNION ALL", SqlKind.Union, 7, true);

    public static final SqlSetOperator exceptOperator =
        new SqlSetOperator("EXCEPT", SqlKind.Except, 7, false);

    public static final SqlSetOperator exceptAllOperator =
        new SqlSetOperator("EXCEPT ALL", SqlKind.Except, 7, true);

    public static final SqlSetOperator intersectOperator =
        new SqlSetOperator("INTERSECT", SqlKind.Intersect, 9, false);

    public static final SqlSetOperator intersectAllOperator =
        new SqlSetOperator("INTERSECT ALL", SqlKind.Intersect, 9, true);


    /** The "MULTISET UNION" operator. */
    public static final SqlMultisetSetOperator multisetUnionOperator =
        new SqlMultisetSetOperator("MULTISET UNION", 7, false);

    /** The "MULTISET UNION ALL" operator. */
    public static final SqlMultisetSetOperator multisetUnionAllOperator =
        new SqlMultisetSetOperator("MULTISET UNION ALL", 7, true);

    /** The "MULTISET EXCEPT" operator. */
    public static final SqlMultisetSetOperator multisetExceptOperator =
        new SqlMultisetSetOperator("MULTISET EXCEPT", 7, false);

    /** The "MULTISET EXCEPT ALL" operator. */
    public static final SqlMultisetSetOperator multisetExceptAllOperator =
        new SqlMultisetSetOperator("MULTISET EXCEPT ALL", 7, true);

    /** The "MULTISET INTERSECT" operator. */
    public static final SqlMultisetSetOperator multisetIntersectOperator =
        new SqlMultisetSetOperator("MULTISET INTERSECT", 9, false);

    /** The "MULTISET INTERSECT ALL" operator. */
    public static final SqlMultisetSetOperator multisetIntersectAllOperator =
        new SqlMultisetSetOperator("MULTISET INTERSECT ALL", 9, true);

    //-------------------------------------------------------------
    //                   BINARY OPERATORS
    //-------------------------------------------------------------

    /**
     * Logical <code>AND</code> operator.
     */
    public static final SqlBinaryOperator andOperator =
        new SqlBinaryOperator("AND", SqlKind.And, 14, true,
            SqlTypeStrategies.rtiNullableBoolean,
            SqlTypeStrategies.otiBoolean,
            SqlTypeStrategies.otcBoolX2);

    /**
     * <code>AS</code> operator associates an expression in the SELECT
     * clause with an alias.
     */
    public static final SqlBinaryOperator asOperator =
        new SqlAsOperator();

    /**
     * String concatenation operator, '<code>||</code>'.
     */
    public static final SqlBinaryOperator concatOperator =
        new SqlBinaryOperator("||", SqlKind.Other, 30, true,
            SqlTypeStrategies.rtiNullableVaryingDyadicStringSumPrecision,
            null,
            SqlTypeStrategies.otcStringSameX2);

    /**
     * Arithmetic division operator, '<code>/</code>'.
     */
    public static final SqlBinaryOperator divideOperator =
        new SqlBinaryOperator("/", SqlKind.Divide, 30, true,
            SqlTypeStrategies.rtiNullableProduct,
            SqlTypeStrategies.otiFirstKnown,
            SqlTypeStrategies.otcDivisionOperator);

    /**
     * Dot operator, '<code>.</code>', used for referencing fields of records.
     */
    public static final SqlBinaryOperator dotOperator =
        new SqlBinaryOperator(".", SqlKind.Dot, 40, true, null, null,
            SqlTypeStrategies.otcAnyX2);

    /**
     * Logical equals operator, '<code>=</code>'.
     */
    public static final SqlBinaryOperator equalsOperator =
        new SqlBinaryOperator("=", SqlKind.Equals, 15, true,
            SqlTypeStrategies.rtiNullableBoolean,
            SqlTypeStrategies.otiFirstKnown,
            SqlTypeStrategies.otcComparableUnorderedX2);

    /**
     * Logical greater-than operator, '<code>&gt;</code>'.
     */
    public static final SqlBinaryOperator greaterThanOperator =
        new SqlBinaryOperator(">", SqlKind.GreaterThan, 15, true,
            SqlTypeStrategies.rtiNullableBoolean,
            SqlTypeStrategies.otiFirstKnown,
            SqlTypeStrategies.otcComparableOrderedX2);

    /**
     * <code>IS DISTINCT FROM</code> operator.
     */
    public static final SqlBinaryOperator isDistinctFromOperator =
        new SqlBinaryOperator("IS DISTINCT FROM", SqlKind.Other, 15, true,
            SqlTypeStrategies.rtiNullableBoolean,
            SqlTypeStrategies.otiFirstKnown,
            SqlTypeStrategies.otcComparableUnorderedX2);

    /**
     * <code>IS NOT DISTINCT FROM</code> operator. Is equivalent to
     * <code>NOT(x IS DISTINCT FROM y)</code>
     */
    public static final SqlBinaryOperator isNotDistinctFromOperator =
        new SqlBinaryOperator("IS NOT DISTINCT FROM", SqlKind.Other, 15, true,
            SqlTypeStrategies.rtiNullableBoolean,
            SqlTypeStrategies.otiFirstKnown,
            SqlTypeStrategies.otcComparableUnorderedX2);

    /**
     * Logical greater-than-or-equal operator, '<code>&gt;=</code>'.
     */
    public static final SqlBinaryOperator greaterThanOrEqualOperator =
        new SqlBinaryOperator(">=", SqlKind.GreaterThanOrEqual, 15, true,
            SqlTypeStrategies.rtiNullableBoolean,
            SqlTypeStrategies.otiFirstKnown,
            SqlTypeStrategies.otcComparableOrderedX2);

    /**
     * <code>IN</code> operator tests for a value's membership in a subquery
     * or a list of values.
     */
    public static final SqlBinaryOperator inOperator =
        new SqlBinaryOperator("IN", SqlKind.In, 15, true,
            SqlTypeStrategies.rtiNullableBoolean,
            SqlTypeStrategies.otiFirstKnown, null);

    /**
     * Logical less-than operator, '<code>&lt;</code>'.
     */
    public static final SqlBinaryOperator lessThanOperator =
        new SqlBinaryOperator("<", SqlKind.LessThan, 15, true,
            SqlTypeStrategies.rtiNullableBoolean,
            SqlTypeStrategies.otiFirstKnown,
            SqlTypeStrategies.otcComparableOrderedX2);

    /**
     * Logical less-than-or-equal operator, '<code>&lt;=</code>'.
     */
    public static final SqlBinaryOperator lessThanOrEqualOperator =
        new SqlBinaryOperator("<=", SqlKind.LessThanOrEqual, 15, true,
            SqlTypeStrategies.rtiNullableBoolean,
            SqlTypeStrategies.otiFirstKnown,
            SqlTypeStrategies.otcComparableOrderedX2);

    /**
     * Arithmetic minus operator, '<code>-</code>'.
     */
    public static final SqlBinaryOperator minusOperator =
        new SqlMonotonicBinaryOperator("-", SqlKind.Minus, 20, true,
            // FIXME jvs 4-June-2005:  this is incorrect; minus
            // has to take precision into account
            SqlTypeStrategies.rtiLeastRestrictive,
            SqlTypeStrategies.otiFirstKnown,
            SqlTypeStrategies.otcMinusOperator);

    /**
     * Arithmetic multiplication operator, '<code>*</code>'.
     */
    public static final SqlBinaryOperator multiplyOperator =
        new SqlMonotonicBinaryOperator("*", SqlKind.Times, 30, true,
            SqlTypeStrategies.rtiNullableProduct,
            SqlTypeStrategies.otiFirstKnown,
            SqlTypeStrategies.otcMultiplyOperator);

    /**
     * Logical not-equals operator, '<code>&lt;&gt;</code>'.
     */
    public static final SqlBinaryOperator notEqualsOperator =
        new SqlBinaryOperator("<>", SqlKind.NotEquals, 15, true,
            SqlTypeStrategies.rtiNullableBoolean,
            SqlTypeStrategies.otiFirstKnown,
            SqlTypeStrategies.otcComparableUnorderedX2);

    /**
     * Logical <code>OR</code> operator.
     */
    public static final SqlBinaryOperator orOperator =
        new SqlBinaryOperator("OR", SqlKind.Or, 13, true,
            SqlTypeStrategies.rtiNullableBoolean,
            SqlTypeStrategies.otiBoolean,
            SqlTypeStrategies.otcBoolX2);

    public static final SqlBinaryOperator plusOperator =
        new SqlMonotonicBinaryOperator("+", SqlKind.Plus, 20, true,
            // FIXME jvs 4-June-2005:  this is incorrect; plus
            // has to take precision into account
            SqlTypeStrategies.rtiLeastRestrictive,
            SqlTypeStrategies.otiFirstKnown,
            SqlTypeStrategies.otcPlusOperator);

    /**
     * Multiset MEMBER OF. Checks to see if a element belongs to a multiset.<br>
     * Example:<br>
     * <code>'green' MEMBER OF MULTISET['red','almost green','blue']</code>
     * returns <code>false</code>.
     */
    public static final SqlBinaryOperator memberOfOperator =
        new SqlMultisetMemberOfOperator();

    /**
     * Submultiset. Checks to see if an multiset is a sub-set of another
     * multiset.<br>
     * Example:<br>
     * <code>
     *  MULTISET['green'] SUBMULTISET OF MULTISET['red','almost green','blue']
     * </code>
     * returns <code>false</code>.<p>
     * But
     * <code>
     *  MULTISET['blue', 'red'] SUBMULTISET OF MULTISET['red','almost green','blue']
     * </code>
     * returns <code>true</code> (<b>NB</b> multisets is order independant)
     */
    public static final SqlBinaryOperator submultisetOfOperator =
        //TODO check if precedence is correct
        new SqlBinaryOperator("SUBMULTISET OF", SqlKind.Other, 15, true,
            SqlTypeStrategies.rtiNullableBoolean,
            null,
            SqlTypeStrategies.otcMultisetX2);

    //-------------------------------------------------------------
    //                   POSTFIX OPERATORS
    //-------------------------------------------------------------
    public static final SqlPostfixOperator descendingOperator =
        new SqlPostfixOperator("DESC", SqlKind.Descending, 10, null,
            SqlTypeStrategies.otiReturnType, SqlTypeStrategies.otcAny);

    public static final SqlPostfixOperator isNotNullOperator =
        new SqlPostfixOperator("IS NOT NULL", SqlKind.Other, 15,
            SqlTypeStrategies.rtiBoolean,
            SqlTypeStrategies.otiBoolean, SqlTypeStrategies.otcAny);

    public static final SqlPostfixOperator isNullOperator =
        new SqlPostfixOperator("IS NULL", SqlKind.IsNull, 15,
            SqlTypeStrategies.rtiBoolean,
            SqlTypeStrategies.otiBoolean, SqlTypeStrategies.otcAny);

    public static final SqlPostfixOperator isNotTrueOperator =
        new SqlPostfixOperator("IS NOT TRUE", SqlKind.Other, 15,
            SqlTypeStrategies.rtiBoolean,
            SqlTypeStrategies.otiBoolean,
            SqlTypeStrategies.otcBool);

    public static final SqlPostfixOperator isTrueOperator =
        new SqlPostfixOperator("IS TRUE", SqlKind.IsTrue, 15,
            SqlTypeStrategies.rtiBoolean,
            SqlTypeStrategies.otiBoolean,
            SqlTypeStrategies.otcBool);

    public static final SqlPostfixOperator isNotFalseOperator =
        new SqlPostfixOperator("IS NOT FALSE", SqlKind.Other, 15,
            SqlTypeStrategies.rtiBoolean,
            SqlTypeStrategies.otiBoolean,
            SqlTypeStrategies.otcBool);

    public static final SqlPostfixOperator isFalseOperator =
        new SqlPostfixOperator("IS FALSE", SqlKind.IsFalse, 15,
            SqlTypeStrategies.rtiBoolean,
            SqlTypeStrategies.otiBoolean,
            SqlTypeStrategies.otcBool);

    public static final SqlPostfixOperator isNotUnknownOperator =
        new SqlPostfixOperator("IS NOT UNKNOWN", SqlKind.Other, 15,
            SqlTypeStrategies.rtiBoolean,
            SqlTypeStrategies.otiBoolean,
            SqlTypeStrategies.otcBool);

    public static final SqlPostfixOperator isUnknownOperator =
        new SqlPostfixOperator("IS UNKNOWN", SqlKind.IsNull, 15,
            SqlTypeStrategies.rtiBoolean,
            SqlTypeStrategies.otiBoolean,
            SqlTypeStrategies.otcBool);

    public static final SqlPostfixOperator isASetOperator =
        new SqlPostfixOperator("IS A SET", SqlKind.Other, 15,
            SqlTypeStrategies.rtiBoolean,
            null,
            SqlTypeStrategies.otcMultiset);

    //-------------------------------------------------------------
    //                   PREFIX OPERATORS
    //-------------------------------------------------------------
    public static final SqlPrefixOperator existsOperator =
        new SqlPrefixOperator("EXISTS", SqlKind.Exists, 20,
            SqlTypeStrategies.rtiBoolean, null,
            SqlTypeStrategies.otcAny);

    public static final SqlPrefixOperator notOperator =
        new SqlPrefixOperator("NOT", SqlKind.Not, 15,
            SqlTypeStrategies.rtiNullableBoolean,
            SqlTypeStrategies.otiBoolean,
            SqlTypeStrategies.otcBool);

    public static final SqlPrefixOperator prefixMinusOperator =
        new SqlPrefixOperator("-", SqlKind.MinusPrefix, 20,
            SqlTypeStrategies.rtiFirstArgType,
            SqlTypeStrategies.otiReturnType,
            SqlTypeStrategies.otcNumericOrInterval);

    public static final SqlPrefixOperator prefixPlusOperator =
        new SqlPrefixOperator("+", SqlKind.PlusPrefix, 20,
            SqlTypeStrategies.rtiFirstArgType,
            SqlTypeStrategies.otiReturnType,
            SqlTypeStrategies.otcNumericOrInterval);

    public static final SqlPrefixOperator explicitTableOperator =
        new SqlPrefixOperator("TABLE", SqlKind.ExplicitTable, 1, null, null,
            null);

    //-------------------------------------------------------------
    // AGGREGATE OPERATORS
    //-------------------------------------------------------------
    /**
     * <code>SUM</code> aggregate function.
     */
    public static final SqlAggFunction sumOperator =
        new SqlSumAggFunction(null);
    /**
     * <code>COUNT</code> aggregate function.
     */
    public static final SqlAggFunction countOperator =
        new SqlCountAggFunction();
    /**
     * <code>MIN</code> aggregate function.
     */
    public static final SqlAggFunction minOperator =
        new SqlMinMaxAggFunction(new RelDataType[0], true,
            SqlMinMaxAggFunction.MINMAX_COMPARABLE);
    /**
     * <code>MAX</code> aggregate function.
     */
    public static final SqlAggFunction maxOperator =
        new SqlMinMaxAggFunction(new RelDataType[0], false,
            SqlMinMaxAggFunction.MINMAX_COMPARABLE);

    /**
     * <code>LAST</code> aggregate function.
     */
    public static final SqlAggFunction lastValueOperator =
        new SqlLastValueAggFunction();

    //-------------------------------------------------------------
    // WINDOW Rank Functions
    //-------------------------------------------------------------
    /**
     * <code>CUME_DIST</code> Window function.
     */
    public static final SqlRankFunction cumeDistFunc =
        new SqlRankFunction("CUME_DIST");

    /**
     * <code>DENSE_RANK</code> Window function.
     */
    public static final SqlRankFunction denseRankFunc =
        new SqlRankFunction("DENSE_RANK");

    /**
     * <code>PERCENT_RANK</code> Window function.
     */
    public static final SqlRankFunction percentRankFunc =
        new SqlRankFunction("PERCENT_RANK");

    /**
     * <code>RANK</code> Window function.
     */
    public static final SqlRankFunction rankFunc =
        new SqlRankFunction("RANK");

    /**
     * <code>ROW_NUMBER</code> Window function.
     */
    public static final SqlRankFunction rowNumberFunc =
        new SqlRankFunction("ROW_NUMBER");

    //-------------------------------------------------------------
    //                   SPECIAL OPERATORS
    //-------------------------------------------------------------
    public static final SqlRowOperator rowConstructor =
        new SqlRowOperator();

    /**
     * A special operator for the subtraction of two DATETIMEs.  The format of
     * DATETIME substraction is: <br> <code>"(" &lt;datetime&gt; "-"
     * &lt;datetime&gt; ")" <interval qualifier></code>.  This operator
     * is special since it needs to hold the additional interval qualifier
     * specification.
     */
    public static final SqlOperator minusDateOperator =
        new SqlDatetimeSubtractionOperator();

    /**
     * The MULTISET Value Constructor.
     * e.g. "<code>MULTISET[1,2,3]</code>".
     */
    public static final SqlMultisetOperator multisetValueConstructor =
        new SqlMultisetOperator(SqlKind.MultisetValueConstructor);

    /**
     * The MULTISET Query Constructor.
     * e.g. "<code>SELECT dname, MULTISET(SELECT * FROM
     * emp WHERE deptno = dept.deptno) FROM dept</code>".
     */
    public static final SqlMultisetOperator multisetQueryConstructor =
        new SqlMultisetOperator(SqlKind.MultisetQueryConstructor);

    /**
     * The <code>UNNEST<code> operator.
     */
    public static final SqlSpecialOperator unnestOperator =
        new SqlUnnestOperator();

    /**
     * The <code>LATERAL<code> operator.
     */
    public static final SqlSpecialOperator lateralOperator =
        new SqlFunctionalOperator ("LATERAL", SqlKind.Lateral,
            100, true,
            SqlTypeStrategies.rtiFirstArgType,
            null,
            SqlTypeStrategies.otcAny);

    public static final SqlOverlapsOperator overlapsOperator =
        new SqlOverlapsOperator();

    public static final SqlSpecialOperator valuesOperator =
        new SqlValuesOperator();

    public static final SqlInternalOperator literalChainOperator =
        new SqlLiteralChainOperator();

    public static final SqlInternalOperator throwOperator =
        new SqlThrowOperator();

    public static final SqlBetweenOperator betweenOperator =
        new SqlBetweenOperator(
            SqlBetweenOperator.Flag.Asymmetric,
            false);

    public static final SqlBetweenOperator symmetricBetweenOperator =
        new SqlBetweenOperator(
            SqlBetweenOperator.Flag.Symmetric,
            false);

    public static final SqlBetweenOperator notBetweenOperator =
        new SqlBetweenOperator(
            SqlBetweenOperator.Flag.Asymmetric,
            true);

    public static final SqlBetweenOperator symmetricNotBetweenOperator =
        new SqlBetweenOperator(
            SqlBetweenOperator.Flag.Symmetric,
            true);

    public static final SqlSpecialOperator notLikeOperator =
        new SqlLikeOperator("NOT LIKE", SqlKind.Like, true);

    public static final SqlSpecialOperator likeOperator =
        new SqlLikeOperator("LIKE", SqlKind.Like, false);

    public static final SqlSpecialOperator notSimilarOperator =
        new SqlLikeOperator("NOT SIMILAR TO", SqlKind.Similar, true);

    public static final SqlSpecialOperator similarOperator =
        new SqlLikeOperator("SIMILAR TO", SqlKind.Similar, false);

    /**
     * Internal operator used to represent the ESCAPE clause of a LIKE or
     * SIMILAR TO expression.
     */
    public static final SqlSpecialOperator escapeOperator =
        new SqlSpecialOperator("Escape", SqlKind.Escape, 15);

    /**
     * The standard SELECT operator.
     */
    public static final SqlSelectOperator selectOperator =
        new SqlSelectOperator();

    public static final SqlCaseOperator caseOperator =
        new SqlCaseOperator();

    public static final SqlJoinOperator joinOperator =
        new SqlJoinOperator();

    public static final SqlSpecialOperator insertOperator =
        new SqlSpecialOperator("INSERT", SqlKind.Insert);

    public static final SqlSpecialOperator deleteOperator =
        new SqlSpecialOperator("DELETE", SqlKind.Delete);

    public static final SqlSpecialOperator updateOperator =
        new SqlSpecialOperator("UPDATE", SqlKind.Update);

    public static final SqlSpecialOperator explainOperator =
        new SqlSpecialOperator("EXPLAIN", SqlKind.Explain);

    public static final SqlOrderByOperator orderByOperator =
        new SqlOrderByOperator();

    public static final SqlOperator procedureCallOperator =
        new SqlProcedureCallOperator();

    public static final SqlOperator newOperator =
        new SqlNewOperator();

    /**
     * The WINDOW clause of a SELECT statment.
     *
     * @see #overOperator
     */
    public static final SqlWindowOperator windowOperator =
        new SqlWindowOperator();

    /**
     * The <code>OVER</code> operator, which applies an aggregate functions to
     * a {@link SqlWindow window}.
     *
     * <p>Operands are as follows:<ol>
     * <li>name of window function ({@link org.eigenbase.sql.SqlCall})</li>
     * <li>window name ({@link org.eigenbase.sql.SqlLiteral})
     *     or window in-line specification (@link SqlWindowOperator})</li>
     * </ul>
     */
    public static final SqlBinaryOperator overOperator =
        new SqlOverOperator();


    //-------------------------------------------------------------
    //                   FUNCTIONS
    //-------------------------------------------------------------
    /**
     * The character substring function:
     * <code>SUBSTRING(string FROM start [FOR length])</code>.
     *
     * <p>If the length parameter is a constant, the length
     * of the result is the minimum of the length of the input
     * and that length. Otherwise it is the length of the input.<p>
     */
    public static final SqlFunction substringFunc =
        new SqlSubstringFunction();

    public static final SqlFunction convertFunc =
        new SqlConvertFunction("CONVERT");

    public static final SqlFunction translateFunc =
        new SqlConvertFunction("TRANSLATE");

    public static final SqlFunction overlayFunc =
        new SqlOverlayFunction();

    /**
     * The "TRIM" function.
     * */
    public static final SqlFunction trimFunc =
        new SqlTrimFunction();

    public static final SqlFunction positionFunc =
        new SqlPositionFunction();

    public static final SqlFunction charLengthFunc =
        new SqlFunction("CHAR_LENGTH", SqlKind.Function,
            SqlTypeStrategies.rtiNullableInteger, null,
            SqlTypeStrategies.otcCharString,
            SqlFunctionCategory.Numeric);

    public static final SqlFunction characterLengthFunc =
        new SqlFunction("CHARACTER_LENGTH", SqlKind.Function,
            SqlTypeStrategies.rtiNullableInteger, null,
            SqlTypeStrategies.otcCharString,
            SqlFunctionCategory.Numeric);

    public static final SqlFunction upperFunc =
        new SqlFunction("UPPER", SqlKind.Function,
            SqlTypeStrategies.rtiNullableFirstArgType, null,
            SqlTypeStrategies.otcCharString,
            SqlFunctionCategory.String);

    public static final SqlFunction lowerFunc =
        new SqlFunction("LOWER", SqlKind.Function,
            SqlTypeStrategies.rtiNullableFirstArgType, null,
            SqlTypeStrategies.otcCharString,
            SqlFunctionCategory.String);

    public static final SqlFunction initcapFunc =
        new SqlFunction("INITCAP", SqlKind.Function,
            SqlTypeStrategies.rtiNullableFirstArgType, null,
            SqlTypeStrategies.otcCharString,
            SqlFunctionCategory.String);

    /**
     * Uses SqlOperatorTable.useDouble for its return type since we don't know
     * what the result type will be by just looking at the operand types.
     * For example POW(int, int) can return a non integer if the second operand
     * is negative.
     */
    public static final SqlFunction powFunc =
        new SqlFunction("POW", SqlKind.Function,
            SqlTypeStrategies.rtiNullableDouble, null,
            SqlTypeStrategies.otcNumericX2,
            SqlFunctionCategory.Numeric);

    public static final SqlFunction modFunc =
        // FIXME jvs 4-June-2005:  this is incorrect; mod
        // has to take precision into account
        new SqlFunction("MOD", SqlKind.Function,
            SqlTypeStrategies.rtiLeastRestrictive, null,
            SqlTypeStrategies.otcNumericX2,
            SqlFunctionCategory.Numeric);

    public static final SqlFunction lnFunc =
        new SqlFunction("LN", SqlKind.Function,
            SqlTypeStrategies.rtiNullableDouble, null,
            SqlTypeStrategies.otcNumeric, SqlFunctionCategory.Numeric);

    public static final SqlFunction logFunc =
        new SqlFunction("LOG", SqlKind.Function,
            SqlTypeStrategies.rtiNullableDouble, null,
            SqlTypeStrategies.otcNumeric, SqlFunctionCategory.Numeric);

    public static final SqlFunction absFunc =
        new SqlFunction("ABS", SqlKind.Function,
            SqlTypeStrategies.rtiFirstArgType, null,
            SqlTypeStrategies.otcNumericOrInterval,
            SqlFunctionCategory.Numeric);

    public static final SqlFunction nullIfFunc =
        new SqlNullifFunction();

    /**
     * The COALESCE builtin function.
     */
    public static final SqlFunction coalesceFunc =
        new SqlCoalesceFunction();

    /** The <code>FLOOR</code> function. */
    public static final SqlFunction floorFunc =
        new SqlMonotonicUnaryFunction("FLOOR", SqlKind.Function,
            SqlTypeStrategies.rtiFirstArgType, null,
            SqlTypeStrategies.otcNumericOrInterval,
            SqlFunctionCategory.Numeric);

    /** The <code>CEIL</code> function. */
    public static final SqlFunction ceilFunc =
        new SqlMonotonicUnaryFunction("CEIL", SqlKind.Function,
            SqlTypeStrategies.rtiFirstArgType, null,
            SqlTypeStrategies.otcNumericOrInterval,
            SqlFunctionCategory.Numeric);

    /** The <code>USER</code> function. */
    public static final SqlFunction userFunc =
        new SqlStringContextVariable("USER");

    /** The <code>CURRENT_USER</code> function. */
    public static final SqlFunction currentUserFunc =
        new SqlStringContextVariable("CURRENT_USER");

    /** The <code>SESSION_USER</code> function. */
    public static final SqlFunction sessionUserFunc =
        new SqlStringContextVariable("SESSION_USER");

    /** The <code>SYSTEM_USER</code> function. */
    public static final SqlFunction systemUserFunc =
        new SqlStringContextVariable("SYSTEM_USER");

    /** The <code>CURRENT_PATH</code> function. */
    public static final SqlFunction currentPathFunc =
        new SqlStringContextVariable("CURRENT_PATH");

    /** The <code>CURRENT_ROLE</code> function. */
    public static final SqlFunction currentRoleFunc =
        new SqlStringContextVariable("CURRENT_ROLE");

    /** The <code>LOCALTIME [(<i>precision</i>)]</code> function. */
    public static final SqlFunction localTimeFunc =
        new SqlAbstractTimeFunction("LOCALTIME", SqlTypeName.Time);

    /** The <code>LOCALTIMESTAMP [(<i>precision</i>)]</code> function. */
    public static final SqlFunction localTimestampFunc =
        new SqlAbstractTimeFunction("LOCALTIMESTAMP", SqlTypeName.Timestamp);

    /** The <code>CURRENT_TIME [(<i>precision</i>)]</code> function. */
    public static final SqlFunction currentTimeFunc =
        new SqlAbstractTimeFunction("CURRENT_TIME", SqlTypeName.Time);

    /** The <code>CURRENT_TIMESTAMP [(<i>precision</i>)]</code> function. */
    public static final SqlFunction currentTimestampFunc =
        new SqlAbstractTimeFunction("CURRENT_TIMESTAMP", SqlTypeName.Timestamp);

    /** The <code>CURRENT_DATE</code> function. */
    public static final SqlFunction currentDateFunc =
        new SqlCurrentDateFunction();

    /**
     * The SQL <code>CAST</code> operator.
     *
     * <p/>The target type is simply stored as
     * the return type, not an explicit operand. For example, the expression
     * <code>CAST(1 + 2 AS DOUBLE)</code> will become a call to
     * <code>CAST</code> with the expression <code>1 + 2</code> as its only
     * operand.
     */
    public static final SqlFunction castFunc =
        new SqlCastFunction();

    /**
     * The SQL <code>EXTRACT</code> operator.  Extracts a specified field value
     * from a DATETIME or an INTERVAL.  E.g.<br> <code>EXTRACT(HOUR FROM
     * INTERVAL '364 23:59:59')</code> returns <code>23</code>
     */
    public static final SqlFunction extractFunc =
        new SqlExtractFunction();

    /**
     * The ELEMENT operator, used to convert a multiset with only one item
     * to a "regular" type. Example
     * ... log(ELEMENT(MULTISET[1])) ...
     */
     public static final SqlFunction elementFunc =
        new SqlFunction("ELEMENT", SqlKind.Function,
            SqlTypeStrategies.rtiNullableMultisetElementType, null,
            SqlTypeStrategies.otcMultiset,
            SqlFunctionCategory.System);

    /**
     * The CARDINALITY operator, used to retrieve the number of elements in
     * a MULTISET
     */
     public static final SqlFunction cardinalityFunc =
        new SqlFunction("CARDINALITY", SqlKind.Function,
            SqlTypeStrategies.rtiNullableInteger, null,
            SqlTypeStrategies.otcMultiset,
            SqlFunctionCategory.System);


    /**
     * The COLLECT operator. Multiset aggregator function.
     */
     public static final SqlFunction collectFunc =
        new SqlFunction("COLLECT", SqlKind.Function,
            SqlTypeStrategies.rtiFirstArgType, null,
            SqlTypeStrategies.otcAny, SqlFunctionCategory.System);

    /**
     * The FUSION operator. Multiset aggregator function.
     */
     public static final SqlFunction fusionFunc =
        new SqlFunction("FUSION", SqlKind.Function,
            SqlTypeStrategies.rtiFirstArgType, null,
            SqlTypeStrategies.otcMultiset,
            SqlFunctionCategory.System);
}

// End SqlStdOperatorTable.java
