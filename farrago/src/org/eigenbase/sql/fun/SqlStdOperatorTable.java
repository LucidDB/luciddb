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

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.util.*;


/**
 * Implementation of {@link org.eigenbase.sql.SqlOperatorTable} containing the
 * standard operators and functions.
 *
 * @author jhyde
 * @version $Id$
 * @since May 28, 2004
 */
public class SqlStdOperatorTable
    extends ReflectiveSqlOperatorTable
{

    //~ Static fields/initializers ---------------------------------------------

    /**
     * The standard operator table.
     */
    private static SqlStdOperatorTable instance;

    //-------------------------------------------------------------
    //                   SET OPERATORS
    //-------------------------------------------------------------
    // The set operators can be compared to the arthimetic operators
    // UNION -> +
    // EXCEPT -> -
    // INTERSECT -> *
    // which explains the different precedence values
    public static final SqlSetOperator unionOperator =
        new SqlSetOperator("UNION", SqlKind.Union, 14, false);

    public static final SqlSetOperator unionAllOperator =
        new SqlSetOperator("UNION ALL", SqlKind.Union, 14, true);

    public static final SqlSetOperator exceptOperator =
        new SqlSetOperator("EXCEPT", SqlKind.Except, 14, false);

    public static final SqlSetOperator exceptAllOperator =
        new SqlSetOperator("EXCEPT ALL", SqlKind.Except, 14, true);

    public static final SqlSetOperator intersectOperator =
        new SqlSetOperator("INTERSECT", SqlKind.Intersect, 18, false);

    public static final SqlSetOperator intersectAllOperator =
        new SqlSetOperator("INTERSECT ALL", SqlKind.Intersect, 18, true);

    /**
     * The "MULTISET UNION" operator.
     */
    public static final SqlMultisetSetOperator multisetUnionOperator =
        new SqlMultisetSetOperator("MULTISET UNION", 14, false);

    /**
     * The "MULTISET UNION ALL" operator.
     */
    public static final SqlMultisetSetOperator multisetUnionAllOperator =
        new SqlMultisetSetOperator("MULTISET UNION ALL", 14, true);

    /**
     * The "MULTISET EXCEPT" operator.
     */
    public static final SqlMultisetSetOperator multisetExceptOperator =
        new SqlMultisetSetOperator("MULTISET EXCEPT", 14, false);

    /**
     * The "MULTISET EXCEPT ALL" operator.
     */
    public static final SqlMultisetSetOperator multisetExceptAllOperator =
        new SqlMultisetSetOperator("MULTISET EXCEPT ALL", 14, true);

    /**
     * The "MULTISET INTERSECT" operator.
     */
    public static final SqlMultisetSetOperator multisetIntersectOperator =
        new SqlMultisetSetOperator("MULTISET INTERSECT", 18, false);

    /**
     * The "MULTISET INTERSECT ALL" operator.
     */
    public static final SqlMultisetSetOperator multisetIntersectAllOperator =
        new SqlMultisetSetOperator("MULTISET INTERSECT ALL", 18, true);

    //-------------------------------------------------------------
    //                   BINARY OPERATORS
    //-------------------------------------------------------------

    /**
     * Logical <code>AND</code> operator.
     */
    public static final SqlBinaryOperator andOperator =
        new SqlBinaryOperator("AND",
            SqlKind.And,
            28,
            true,
            SqlTypeStrategies.rtiNullableBoolean,
            SqlTypeStrategies.otiBoolean,
            SqlTypeStrategies.otcBoolX2);

    /**
     * <code>AS</code> operator associates an expression in the SELECT clause
     * with an alias.
     */
    public static final SqlBinaryOperator asOperator = new SqlAsOperator();

    /**
     * String concatenation operator, '<code>||</code>'.
     */
    public static final SqlBinaryOperator concatOperator =
        new SqlBinaryOperator("||",
            SqlKind.Other,
            60,
            true,
            SqlTypeStrategies.rtiNullableDyadicStringSumPrecision,
            null,
            SqlTypeStrategies.otcStringSameX2);

    /**
     * Arithmetic division operator, '<code>/</code>'.
     */
    public static final SqlBinaryOperator divideOperator =
        new SqlBinaryOperator("/",
            SqlKind.Divide,
            60,
            true,
            SqlTypeStrategies.rtiNullableQuotient,
            SqlTypeStrategies.otiFirstKnown,
            SqlTypeStrategies.otcDivisionOperator);

    /**
     * Dot operator, '<code>.</code>', used for referencing fields of records.
     */
    public static final SqlBinaryOperator dotOperator =
        new SqlBinaryOperator(".",
            SqlKind.Dot,
            80,
            true,
            null,
            null,
            SqlTypeStrategies.otcAnyX2);

    /**
     * Logical equals operator, '<code>=</code>'.
     */
    public static final SqlBinaryOperator equalsOperator =
        new SqlBinaryOperator("=",
            SqlKind.Equals,
            30,
            true,
            SqlTypeStrategies.rtiNullableBoolean,
            SqlTypeStrategies.otiFirstKnown,
            SqlTypeStrategies.otcComparableUnorderedX2);

    /**
     * Logical greater-than operator, '<code>&gt;</code>'.
     */
    public static final SqlBinaryOperator greaterThanOperator =
        new SqlBinaryOperator(">",
            SqlKind.GreaterThan,
            30,
            true,
            SqlTypeStrategies.rtiNullableBoolean,
            SqlTypeStrategies.otiFirstKnown,
            SqlTypeStrategies.otcComparableOrderedX2);

    /**
     * <code>IS DISTINCT FROM</code> operator.
     */
    public static final SqlBinaryOperator isDistinctFromOperator =
        new SqlBinaryOperator("IS DISTINCT FROM",
            SqlKind.Other,
            30,
            true,
            SqlTypeStrategies.rtiBoolean,
            SqlTypeStrategies.otiFirstKnown,
            SqlTypeStrategies.otcComparableUnorderedX2);

    /**
     * <code>IS NOT DISTINCT FROM</code> operator. Is equivalent to <code>NOT(x
     * IS DISTINCT FROM y)</code>
     */
    public static final SqlBinaryOperator isNotDistinctFromOperator =
        new SqlBinaryOperator("IS NOT DISTINCT FROM",
            SqlKind.Other,
            30,
            true,
            SqlTypeStrategies.rtiBoolean,
            SqlTypeStrategies.otiFirstKnown,
            SqlTypeStrategies.otcComparableUnorderedX2);

    /**
     * The internal <code>$IS_DIFFERENT_FROM</code> operator is the same as the
     * user-level {@link #isDistinctFromOperator} in all respects except that
     * the test for equality on character datatypes treats trailing spaces
     * as significant.
     */
    public static final SqlBinaryOperator isDifferentFromOperator =
        new SqlBinaryOperator("$IS_DIFFERENT_FROM",
            SqlKind.Other,
            30,
            true,
            SqlTypeStrategies.rtiBoolean,
            SqlTypeStrategies.otiFirstKnown,
            SqlTypeStrategies.otcComparableUnorderedX2);

    /**
     * Logical greater-than-or-equal operator, '<code>&gt;=</code>'.
     */
    public static final SqlBinaryOperator greaterThanOrEqualOperator =
        new SqlBinaryOperator(">=",
            SqlKind.GreaterThanOrEqual,
            30,
            true,
            SqlTypeStrategies.rtiNullableBoolean,
            SqlTypeStrategies.otiFirstKnown,
            SqlTypeStrategies.otcComparableOrderedX2);

    /**
     * <code>IN</code> operator tests for a value's membership in a subquery or
     * a list of values.
     */
    public static final SqlBinaryOperator inOperator = new SqlInOperator(false);

    /**
     * <code>NOT IN</code> operator tests for a value's membership in a subquery or
     * a list of values.
     */
    public static final SqlBinaryOperator notInOperator = new SqlInOperator(true);
    
    /**
     * Logical less-than operator, '<code>&lt;</code>'.
     */
    public static final SqlBinaryOperator lessThanOperator =
        new SqlBinaryOperator("<",
            SqlKind.LessThan,
            30,
            true,
            SqlTypeStrategies.rtiNullableBoolean,
            SqlTypeStrategies.otiFirstKnown,
            SqlTypeStrategies.otcComparableOrderedX2);

    /**
     * Logical less-than-or-equal operator, '<code>&lt;=</code>'.
     */
    public static final SqlBinaryOperator lessThanOrEqualOperator =
        new SqlBinaryOperator("<=",
            SqlKind.LessThanOrEqual,
            30,
            true,
            SqlTypeStrategies.rtiNullableBoolean,
            SqlTypeStrategies.otiFirstKnown,
            SqlTypeStrategies.otcComparableOrderedX2);

    /**
     * Infix arithmetic minus operator, '<code>-</code>'.
     *
     * <p>Its precedence is less than the prefix {@link #prefixPlusOperator +}
     * and {@link #prefixMinusOperator -} operators.
     */
    public static final SqlBinaryOperator minusOperator =
        new SqlMonotonicBinaryOperator("-",
            SqlKind.Minus,
            40,
            true,

            // Same type inference strategy as sum
            SqlTypeStrategies.rtiNullableSum,
            SqlTypeStrategies.otiFirstKnown,
            SqlTypeStrategies.otcMinusOperator);

    /**
     * Arithmetic multiplication operator, '<code>*</code>'.
     */
    public static final SqlBinaryOperator multiplyOperator =
        new SqlMonotonicBinaryOperator("*",
            SqlKind.Times,
            60,
            true,
            SqlTypeStrategies.rtiNullableProduct,
            SqlTypeStrategies.otiFirstKnown,
            SqlTypeStrategies.otcMultiplyOperator);

    /**
     * Logical not-equals operator, '<code>&lt;&gt;</code>'.
     */
    public static final SqlBinaryOperator notEqualsOperator =
        new SqlBinaryOperator("<>",
            SqlKind.NotEquals,
            30,
            true,
            SqlTypeStrategies.rtiNullableBoolean,
            SqlTypeStrategies.otiFirstKnown,
            SqlTypeStrategies.otcComparableUnorderedX2);

    /**
     * Logical <code>OR</code> operator.
     */
    public static final SqlBinaryOperator orOperator =
        new SqlBinaryOperator("OR",
            SqlKind.Or,
            26,
            true,
            SqlTypeStrategies.rtiNullableBoolean,
            SqlTypeStrategies.otiBoolean,
            SqlTypeStrategies.otcBoolX2);

    /**
     * Infix arithmetic plus operator, '<code>+</code>'.
     */
    public static final SqlBinaryOperator plusOperator =
        new SqlMonotonicBinaryOperator("+",
            SqlKind.Plus,
            40,
            true,
            SqlTypeStrategies.rtiNullableSum,
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
     * <code>MULTISET['green'] SUBMULTISET OF MULTISET['red','almost
     * green','blue']</code> returns <code>false</code>.
     *
     * <p>But <code>MULTISET['blue', 'red'] SUBMULTISET OF
     * MULTISET['red','almost green','blue']</code> returns <code>true</code>
     * (<b>NB</b> multisets is order independant)
     */
    public static final SqlBinaryOperator submultisetOfOperator =

        //TODO check if precedence is correct
        new SqlBinaryOperator("SUBMULTISET OF",
            SqlKind.Other,
            30,
            true,
            SqlTypeStrategies.rtiNullableBoolean,
            null,
            SqlTypeStrategies.otcMultisetX2);

    //-------------------------------------------------------------
    //                   POSTFIX OPERATORS
    //-------------------------------------------------------------
    public static final SqlPostfixOperator descendingOperator =
        new SqlPostfixOperator("DESC",
            SqlKind.Descending,
            20,
            null,
            SqlTypeStrategies.otiReturnType,
            SqlTypeStrategies.otcAny);

    public static final SqlPostfixOperator isNotNullOperator =
        new SqlPostfixOperator("IS NOT NULL",
            SqlKind.Other,
            30,
            SqlTypeStrategies.rtiBoolean,
            SqlTypeStrategies.otiBoolean,
            SqlTypeStrategies.otcAny);

    public static final SqlPostfixOperator isNullOperator =
        new SqlPostfixOperator("IS NULL",
            SqlKind.IsNull,
            30,
            SqlTypeStrategies.rtiBoolean,
            SqlTypeStrategies.otiBoolean,
            SqlTypeStrategies.otcAny);

    public static final SqlPostfixOperator isNotTrueOperator =
        new SqlPostfixOperator("IS NOT TRUE",
            SqlKind.Other,
            30,
            SqlTypeStrategies.rtiBoolean,
            SqlTypeStrategies.otiBoolean,
            SqlTypeStrategies.otcBool);

    public static final SqlPostfixOperator isTrueOperator =
        new SqlPostfixOperator("IS TRUE",
            SqlKind.IsTrue,
            30,
            SqlTypeStrategies.rtiBoolean,
            SqlTypeStrategies.otiBoolean,
            SqlTypeStrategies.otcBool);

    public static final SqlPostfixOperator isNotFalseOperator =
        new SqlPostfixOperator("IS NOT FALSE",
            SqlKind.Other,
            30,
            SqlTypeStrategies.rtiBoolean,
            SqlTypeStrategies.otiBoolean,
            SqlTypeStrategies.otcBool);

    public static final SqlPostfixOperator isFalseOperator =
        new SqlPostfixOperator("IS FALSE",
            SqlKind.IsFalse,
            30,
            SqlTypeStrategies.rtiBoolean,
            SqlTypeStrategies.otiBoolean,
            SqlTypeStrategies.otcBool);

    public static final SqlPostfixOperator isNotUnknownOperator =
        new SqlPostfixOperator("IS NOT UNKNOWN",
            SqlKind.Other,
            30,
            SqlTypeStrategies.rtiBoolean,
            SqlTypeStrategies.otiBoolean,
            SqlTypeStrategies.otcBool);

    public static final SqlPostfixOperator isUnknownOperator =
        new SqlPostfixOperator("IS UNKNOWN",
            SqlKind.IsNull,
            30,
            SqlTypeStrategies.rtiBoolean,
            SqlTypeStrategies.otiBoolean,
            SqlTypeStrategies.otcBool);

    public static final SqlPostfixOperator isASetOperator =
        new SqlPostfixOperator("IS A SET",
            SqlKind.Other,
            30,
            SqlTypeStrategies.rtiBoolean,
            null,
            SqlTypeStrategies.otcMultiset);

    //-------------------------------------------------------------
    //                   PREFIX OPERATORS
    //-------------------------------------------------------------
    public static final SqlPrefixOperator existsOperator =
        new SqlPrefixOperator("EXISTS",
            SqlKind.Exists,
            40,
            SqlTypeStrategies.rtiBoolean,
            null,
            SqlTypeStrategies.otcAny)
        {
            public boolean argumentMustBeScalar(int ordinal)
            {
                return false;
            }
        };

    public static final SqlPrefixOperator notOperator =
        new SqlPrefixOperator("NOT",
            SqlKind.Not,
            30,
            SqlTypeStrategies.rtiNullableBoolean,
            SqlTypeStrategies.otiBoolean,
            SqlTypeStrategies.otcBool);

    /**
     * Prefix arithmetic minus operator, '<code>-</code>'.
     *
     * <p>Its precedence is greater than the infix '{@link #plusOperator +}' and
     * '{@link #minusOperator -}' operators.
     */
    public static final SqlPrefixOperator prefixMinusOperator =
        new SqlPrefixOperator("-",
            SqlKind.MinusPrefix,
            80,
            SqlTypeStrategies.rtiFirstArgType,
            SqlTypeStrategies.otiReturnType,
            SqlTypeStrategies.otcNumericOrInterval);

    /**
     * Prefix arithmetic plus operator, '<code>+</code>'.
     *
     * <p>Its precedence is greater than the infix '{@link #plusOperator +}' and
     * '{@link #minusOperator -}' operators.
     */
    public static final SqlPrefixOperator prefixPlusOperator =
        new SqlPrefixOperator("+",
            SqlKind.PlusPrefix,
            80,
            SqlTypeStrategies.rtiFirstArgType,
            SqlTypeStrategies.otiReturnType,
            SqlTypeStrategies.otcNumericOrInterval);

    /**
     * Keyword which allows an identifier to be explicitly flagged as a table.
     * For example, <code>select * from (TABLE t)</code> or <code>TABLE
     * t</code>. See also {@link #collectionTableOperator}.
     */
    public static final SqlPrefixOperator explicitTableOperator =
        new SqlPrefixOperator(
            "TABLE",
            SqlKind.ExplicitTable,
            2,
            null,
            null,
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
        new SqlMinMaxAggFunction(
            new RelDataType[0],
            true,
            SqlMinMaxAggFunction.MINMAX_COMPARABLE);

    /**
     * <code>MAX</code> aggregate function.
     */
    public static final SqlAggFunction maxOperator =
        new SqlMinMaxAggFunction(
            new RelDataType[0],
            false,
            SqlMinMaxAggFunction.MINMAX_COMPARABLE);

    /**
     * <code>LAST_VALUE</code> aggregate function.
     */
    public static final SqlAggFunction lastValueOperator =
        new SqlFirstLastValueAggFunction(false);

    /**
     * <code>FIRST_VALUE</code> aggregate function.
     */
    public static final SqlAggFunction firstValueOperator =
        new SqlFirstLastValueAggFunction(true);

    /**
     * <code>SINGLE_VALUE</code> aggregate function.
     */
    public static final SqlAggFunction singleValueOperator =
        new SqlSingleValueAggFunction(null);

    /**
     * <code>AVG</code> aggregate function.
     */
    public static final SqlAggFunction avgOperator =
        new SqlAvgAggFunction(null);

    //-------------------------------------------------------------
    // WINDOW Aggregate Functions
    //-------------------------------------------------------------
    /**
     * <code>HISTORAM</code> aggregate function support. Used by window
     * aggregate versions of MIN/MAX
     */
    public static final SqlAggFunction histogramAggFunction =
        new SqlHistogramAggFunction(null);

    /**
     * <code>HISTOGRAM_MIN</code> window aggregate function.
     */
    public static final SqlFunction histogramMinFunction =
        new SqlFunction("$HISTOGRAM_MIN",
            SqlKind.Function,
            SqlTypeStrategies.rtiNullableFirstArgType,
            null,
            SqlTypeStrategies.otcNumericOrString,
            SqlFunctionCategory.Numeric);

    /**
     * <code>HISTOGRAM_MAX</code> window aggregate function.
     */
    public static final SqlFunction histogramMaxFunction =
        new SqlFunction("$HISTOGRAM_MAX",
            SqlKind.Function,
            SqlTypeStrategies.rtiNullableFirstArgType,
            null,
            SqlTypeStrategies.otcNumericOrString,
            SqlFunctionCategory.Numeric);

    /**
     * <code>HISTOGRAM_FIRST_VALUE</code> window aggregate function.
     */
    public static final SqlFunction histogramFirstValueFunction =
        new SqlFunction("$HISTOGRAM_FIRST_VALUE",
            SqlKind.Function,
            SqlTypeStrategies.rtiNullableFirstArgType,
            null,
            SqlTypeStrategies.otcNumericOrString,
            SqlFunctionCategory.Numeric);

    /**
     * <code>HISTOGRAM_LAST_VALUE</code> window aggregate function.
     */
    public static final SqlFunction histogramLastValueFunction =
        new SqlFunction("$HISTOGRAM_LAST_VALUE",
            SqlKind.Function,
            SqlTypeStrategies.rtiNullableFirstArgType,
            null,
            SqlTypeStrategies.otcNumericOrString,
            SqlFunctionCategory.Numeric);

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
    public static final SqlRankFunction rankFunc = new SqlRankFunction("RANK");

    /**
     * <code>ROW_NUMBER</code> Window function.
     */
    public static final SqlRankFunction rowNumberFunc =
        new SqlRankFunction("ROW_NUMBER");

    //-------------------------------------------------------------
    //                   SPECIAL OPERATORS
    //-------------------------------------------------------------
    public static final SqlRowOperator rowConstructor = new SqlRowOperator();

    /**
     * A special operator for the subtraction of two DATETIMEs. The format of
     * DATETIME substraction is:<br>
     * <code>"(" &lt;datetime&gt; "-" &lt;datetime&gt; ")" <interval
     * qualifier></code>. This operator is special since it needs to hold the
     * additional interval qualifier specification.
     */
    public static final SqlOperator minusDateOperator =
        new SqlDatetimeSubtractionOperator();

    /**
     * The MULTISET Value Constructor. e.g. "<code>MULTISET[1,2,3]</code>".
     */
    public static final SqlMultisetValueConstructor multisetValueConstructor =
        new SqlMultisetValueConstructor();

    /**
     * The MULTISET Query Constructor. e.g. "<code>SELECT dname, MULTISET(SELECT
     * FROM emp WHERE deptno = dept.deptno) FROM dept</code>".
     */
    public static final SqlMultisetQueryConstructor multisetQueryConstructor =
        new SqlMultisetQueryConstructor();

    /**
     * The CURSOR constructor. e.g. "<code>SELECT * FROM
     * TABLE(DEDUP(CURSOR(SELECT * FROM EMPS), 'name'))</code>".
     */
    public static final SqlCursorConstructor cursorConstructor =
        new SqlCursorConstructor();

    /**
     * The <code>UNNEST<code>operator.
     */
    public static final SqlSpecialOperator unnestOperator =
        new SqlUnnestOperator();

    /**
     * The <code>LATERAL<code>operator.
     */
    public static final SqlSpecialOperator lateralOperator =
        new SqlFunctionalOperator("LATERAL",
            SqlKind.Lateral,
            200,
            true,
            SqlTypeStrategies.rtiFirstArgType,
            null,
            SqlTypeStrategies.otcAny);

    /**
     * The "table function derived table" operator, which a table-valued
     * function into a relation, e.g. "<code>SELECT * FROM
     * TABLE(ramp(5))</code>".
     *
     * <p>This operator has function syntax (with one argument), whereas {@link
     * #explicitTableOperator} is a prefix operator.
     */
    public static final SqlSpecialOperator collectionTableOperator =
        new SqlCollectionTableOperator(
            "TABLE",
            SqlCollectionTableOperator.MODALITY_RELATIONAL);

    public static final SqlOverlapsOperator overlapsOperator =
        new SqlOverlapsOperator();

    public static final SqlSpecialOperator valuesOperator =
        new SqlValuesOperator();

    public static final SqlLiteralChainOperator literalChainOperator =
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
        new SqlSpecialOperator("Escape", SqlKind.Escape, 30);

    /**
     * The standard SELECT operator.
     */
    public static final SqlSelectOperator selectOperator =
        new SqlSelectOperator();

    public static final SqlCaseOperator caseOperator = new SqlCaseOperator();

    public static final SqlJoinOperator joinOperator = new SqlJoinOperator();

    public static final SqlSpecialOperator insertOperator =
        new SqlSpecialOperator("INSERT", SqlKind.Insert);

    public static final SqlSpecialOperator deleteOperator =
        new SqlSpecialOperator("DELETE", SqlKind.Delete);

    public static final SqlSpecialOperator updateOperator =
        new SqlSpecialOperator("UPDATE", SqlKind.Update);

    public static final SqlSpecialOperator mergeOperator =
        new SqlSpecialOperator("MERGE", SqlKind.Merge);

    public static final SqlSpecialOperator explainOperator =
        new SqlSpecialOperator("EXPLAIN", SqlKind.Explain);

    public static final SqlOrderByOperator orderByOperator =
        new SqlOrderByOperator();

    public static final SqlOperator procedureCallOperator =
        new SqlProcedureCallOperator();

    public static final SqlOperator newOperator = new SqlNewOperator();

    /**
     * The WINDOW clause of a SELECT statment.
     *
     * @see #overOperator
     */
    public static final SqlWindowOperator windowOperator =
        new SqlWindowOperator();

    /**
     * The <code>OVER</code> operator, which applies an aggregate functions to a
     * {@link SqlWindow window}.
     *
     * <p>Operands are as follows:
     *
     * <ol>
     * <li>name of window function ({@link org.eigenbase.sql.SqlCall})</li>
     * <li>window name ({@link org.eigenbase.sql.SqlLiteral}) or window in-line
     * specification (@link SqlWindowOperator})</li>
     * </ul>
     */
    public static final SqlBinaryOperator overOperator = new SqlOverOperator();

    /**
     * An <code>REINTERPRET<code>operator is internal to the planner. When the
     * physical storage of two types is the same, this operator may be used to
     * reinterpret values of one type as the other. This operator is similar to
     * a cast, except that it does not alter the data value. Like a regular cast
     * it accepts one operand and stores the target type as the return type. It
     * performs an overflow check if it has <i>any</i> second operand, whether
     * true or not.
     */
    public static final SqlSpecialOperator reinterpretOperator =
        new SqlSpecialOperator("Reinterpret", SqlKind.Reinterpret) {
            public SqlOperandCountRange getOperandCountRange()
            {
                return SqlOperandCountRange.OneOrTwo;
            }
        };

    //-------------------------------------------------------------
    //                   FUNCTIONS
    //-------------------------------------------------------------
    /**
     * The character substring function: <code>SUBSTRING(string FROM start [FOR
     * length])</code>.
     *
     * <p>If the length parameter is a constant, the length of the result is the
     * minimum of the length of the input and that length. Otherwise it is the
     * length of the input.
     *
     * <p>
     */
    public static final SqlFunction substringFunc = new SqlSubstringFunction();

    public static final SqlFunction convertFunc =
        new SqlConvertFunction("CONVERT");

    public static final SqlFunction translateFunc =
        new SqlConvertFunction("TRANSLATE");

    public static final SqlFunction overlayFunc = new SqlOverlayFunction();

    /**
     * The "TRIM" function.
     */
    public static final SqlFunction trimFunc = new SqlTrimFunction();

    public static final SqlFunction positionFunc = new SqlPositionFunction();

    public static final SqlFunction charLengthFunc =
        new SqlFunction("CHAR_LENGTH",
            SqlKind.Function,
            SqlTypeStrategies.rtiNullableInteger,
            null,
            SqlTypeStrategies.otcCharString,
            SqlFunctionCategory.Numeric);

    public static final SqlFunction characterLengthFunc =
        new SqlFunction("CHARACTER_LENGTH",
            SqlKind.Function,
            SqlTypeStrategies.rtiNullableInteger,
            null,
            SqlTypeStrategies.otcCharString,
            SqlFunctionCategory.Numeric);

    public static final SqlFunction upperFunc =
        new SqlFunction("UPPER",
            SqlKind.Function,
            SqlTypeStrategies.rtiNullableFirstArgType,
            null,
            SqlTypeStrategies.otcCharString,
            SqlFunctionCategory.String);

    public static final SqlFunction lowerFunc =
        new SqlFunction("LOWER",
            SqlKind.Function,
            SqlTypeStrategies.rtiNullableFirstArgType,
            null,
            SqlTypeStrategies.otcCharString,
            SqlFunctionCategory.String);

    public static final SqlFunction initcapFunc =
        new SqlFunction("INITCAP",
            SqlKind.Function,
            SqlTypeStrategies.rtiNullableFirstArgType,
            null,
            SqlTypeStrategies.otcCharString,
            SqlFunctionCategory.String);

    /**
     * Uses SqlOperatorTable.useDouble for its return type since we don't know
     * what the result type will be by just looking at the operand types. For
     * example POW(int, int) can return a non integer if the second operand is
     * negative.
     */
    public static final SqlFunction powFunc =
        new SqlFunction("POW",
            SqlKind.Function,
            SqlTypeStrategies.rtiNullableDouble,
            null,
            SqlTypeStrategies.otcNumericX2,
            SqlFunctionCategory.Numeric);

    public static final SqlFunction modFunc =

        // Return type is same as divisor (2nd operand)
        // SQL2003 Part2 Section 6.27, Syntax Rules 9
        new SqlFunction("MOD",
            SqlKind.Function,
            SqlTypeStrategies.rtiNullableSecondArgType,
            null,
            SqlTypeStrategies.otcExactNumericX2,
            SqlFunctionCategory.Numeric);

    public static final SqlFunction lnFunc =
        new SqlFunction("LN",
            SqlKind.Function,
            SqlTypeStrategies.rtiNullableDouble,
            null,
            SqlTypeStrategies.otcNumeric,
            SqlFunctionCategory.Numeric);

    public static final SqlFunction log10Func =
        new SqlFunction("LOG10",
            SqlKind.Function,
            SqlTypeStrategies.rtiNullableDouble,
            null,
            SqlTypeStrategies.otcNumeric,
            SqlFunctionCategory.Numeric);

    public static final SqlFunction absFunc =
        new SqlFunction("ABS",
            SqlKind.Function,
            SqlTypeStrategies.rtiFirstArgType,
            null,
            SqlTypeStrategies.otcNumericOrInterval,
            SqlFunctionCategory.Numeric);

    public static final SqlFunction expFunc =
        new SqlFunction("EXP",
            SqlKind.Function,
            SqlTypeStrategies.rtiNullableDouble,
            null,
            SqlTypeStrategies.otcNumeric,
            SqlFunctionCategory.Numeric);

    public static final SqlFunction nullIfFunc = new SqlNullifFunction();

    /**
     * The COALESCE builtin function.
     */
    public static final SqlFunction coalesceFunc = new SqlCoalesceFunction();

    /**
     * The <code>FLOOR</code> function.
     */
    public static final SqlFunction floorFunc =
        new SqlMonotonicUnaryFunction("FLOOR",
            SqlKind.Function,
            SqlTypeStrategies.rtiFirstArgTypeOrExactNoScale,
            null,
            SqlTypeStrategies.otcNumericOrInterval,
            SqlFunctionCategory.Numeric);

    /**
     * The <code>CEIL</code> function.
     */
    public static final SqlFunction ceilFunc =
        new SqlMonotonicUnaryFunction("CEIL",
            SqlKind.Function,
            SqlTypeStrategies.rtiFirstArgTypeOrExactNoScale,
            null,
            SqlTypeStrategies.otcNumericOrInterval,
            SqlFunctionCategory.Numeric);

    /**
     * The <code>USER</code> function.
     */
    public static final SqlFunction userFunc =
        new SqlStringContextVariable("USER");

    /**
     * The <code>CURRENT_USER</code> function.
     */
    public static final SqlFunction currentUserFunc =
        new SqlStringContextVariable("CURRENT_USER");

    /**
     * The <code>SESSION_USER</code> function.
     */
    public static final SqlFunction sessionUserFunc =
        new SqlStringContextVariable("SESSION_USER");

    /**
     * The <code>SYSTEM_USER</code> function.
     */
    public static final SqlFunction systemUserFunc =
        new SqlStringContextVariable("SYSTEM_USER");

    /**
     * The <code>CURRENT_PATH</code> function.
     */
    public static final SqlFunction currentPathFunc =
        new SqlStringContextVariable("CURRENT_PATH");

    /**
     * The <code>CURRENT_ROLE</code> function.
     */
    public static final SqlFunction currentRoleFunc =
        new SqlStringContextVariable("CURRENT_ROLE");

    /**
     * The <code>LOCALTIME [(<i>precision</i>)]</code> function.
     */
    public static final SqlFunction localTimeFunc =
        new SqlAbstractTimeFunction("LOCALTIME", SqlTypeName.Time);

    /**
     * The <code>LOCALTIMESTAMP [(<i>precision</i>)]</code> function.
     */
    public static final SqlFunction localTimestampFunc =
        new SqlAbstractTimeFunction("LOCALTIMESTAMP", SqlTypeName.Timestamp);

    /**
     * The <code>CURRENT_TIME [(<i>precision</i>)]</code> function.
     */
    public static final SqlFunction currentTimeFunc =
        new SqlAbstractTimeFunction("CURRENT_TIME", SqlTypeName.Time);

    /**
     * The <code>CURRENT_TIMESTAMP [(<i>precision</i>)]</code> function.
     */
    public static final SqlFunction currentTimestampFunc =
        new SqlAbstractTimeFunction("CURRENT_TIMESTAMP", SqlTypeName.Timestamp);

    /**
     * The <code>CURRENT_DATE</code> function.
     */
    public static final SqlFunction currentDateFunc =
        new SqlCurrentDateFunction();

    /**
     * The SQL <code>CAST</code> operator.
     *
     * <p/>The target type is simply stored as the return type, not an explicit
     * operand. For example, the expression <code>CAST(1 + 2 AS DOUBLE)</code>
     * will become a call to <code>CAST</code> with the expression <code>1 +
     * 2</code> as its only operand.
     */
    public static final SqlFunction castFunc = new SqlCastFunction();

    /**
     * The SQL <code>EXTRACT</code> operator. Extracts a specified field value
     * from a DATETIME or an INTERVAL. E.g.<br>
     * <code>EXTRACT(HOUR FROM INTERVAL '364 23:59:59')</code> returns <code>
     * 23</code>
     */
    public static final SqlFunction extractFunc = new SqlExtractFunction();

    /**
     * The ELEMENT operator, used to convert a multiset with only one item to a
     * "regular" type. Example ... log(ELEMENT(MULTISET[1])) ...
     */
    public static final SqlFunction elementFunc =
        new SqlFunction("ELEMENT",
            SqlKind.Function,
            SqlTypeStrategies.rtiNullableMultisetElementType,
            null,
            SqlTypeStrategies.otcMultiset,
            SqlFunctionCategory.System);

    /**
     * The internal "$SLICE" operator takes a multiset of records and returns a
     * multiset of the first column of those records.
     *
     * <p>It is introduced when multisets of scalar types are created, in order
     * to keep types consistent. For example, <code>MULTISET [5]</code> has type
     * <code>INTEGER MULTISET</code> but is translated to an expression of type
     * <code>RECORD(INTEGER EXPR$0) MULTISET</code> because in our internal
     * representation of multisets, every element must be a record. Applying the
     * "$SLICE" operator to this result converts the type back to an <code>
     * INTEGER MULTISET</code> multiset value.
     *
     * <p><code>$SLICE</code> is often translated away when the multiset type is
     * converted back to scalar values.
     */
    public static final SqlInternalOperator sliceOp =
        new SqlInternalOperator("$SLICE",
            SqlKind.Other,
            0,
            false,
            SqlTypeStrategies.rtiMultisetFirstColumnMultiset,
            null,
            SqlTypeStrategies.otcRecordMultiset) {
        };

    /**
     * The internal "$ELEMENT_SLICE" operator returns the first field of the
     * only element of a multiset.
     *
     * <p/>It is introduced when multisets of scalar types are created, in order
     * to keep types consistent. For example, <code>ELEMENT(MULTISET [5])</code>
     * is translated to <code>$ELEMENT_SLICE(MULTISET (VALUES ROW (5
     * EXPR$0))</code> It is translated away when the multiset type is converted
     * back to scalar values.
     *
     * <p/>NOTE: jhyde, 2006/1/9: Usages of this operator are commented out, but
     * I'm not deleting the operator, because some multiset tests are disabled,
     * and we may need this operator to get them working!
     */
    public static final SqlInternalOperator elementSlicefunc =
        new SqlInternalOperator("$ELEMENT_SLICE",
            SqlKind.Other,
            0,
            false,
            SqlTypeStrategies.rtiMultisetRecordMultiset,
            null,
            SqlTypeStrategies.otcMultiset) {
            public void unparse(
                SqlWriter writer,
                SqlNode [] operands,
                int leftPrec,
                int rightPrec)
            {
                SqlUtil.unparseFunctionSyntax(this,
                    writer,
                    operands,
                    true,
                    null);
            }
        };

    /**
     * The internal "$SCALAR_QUERY" operator returns a scalar value from a
     * record type. It asusmes the record type only has one field, and
     * returns that field as the output.
     */
    public static final SqlInternalOperator scalarQueryOperator =
        new SqlInternalOperator("$SCALAR_QUERY",
            SqlKind.ScalarQuery,
            0,
            false,
            SqlTypeStrategies.rtiRecordToScalarType,
            null,
            SqlTypeStrategies.otcRecordToScalarType)
        {
            public void unparse(
                SqlWriter writer,
                SqlNode [] operands,
                int leftPrec,
                int rightPrec)
            {
                final SqlWriter.Frame frame = writer.startList("(", ")");
                operands[0].unparse(writer, 0, 0);
                writer.endList(frame);
            }

            public boolean argumentMustBeScalar(int ordinal)
            {
                // Obvious, really.
                return false;
            }
        };
        
    /**
     * The CARDINALITY operator, used to retrieve the number of elements in a
     * MULTISET
     */
    public static final SqlFunction cardinalityFunc =
        new SqlFunction("CARDINALITY",
            SqlKind.Function,
            SqlTypeStrategies.rtiNullableInteger,
            null,
            SqlTypeStrategies.otcMultiset,
            SqlFunctionCategory.System);

    /**
     * The COLLECT operator. Multiset aggregator function.
     */
    public static final SqlFunction collectFunc =
        new SqlFunction("COLLECT",
            SqlKind.Function,
            SqlTypeStrategies.rtiFirstArgType,
            null,
            SqlTypeStrategies.otcAny,
            SqlFunctionCategory.System);

    /**
     * The FUSION operator. Multiset aggregator function.
     */
    public static final SqlFunction fusionFunc =
        new SqlFunction("FUSION",
            SqlKind.Function,
            SqlTypeStrategies.rtiFirstArgType,
            null,
            SqlTypeStrategies.otcMultiset,
            SqlFunctionCategory.System);

    /**
     * The sequence next value function: <code>NEXT VALUE FOR sequence</code>
     */
    public static final SqlFunction nextValueFunc =
        new SqlFunction("NEXT_VALUE",
            SqlKind.Function,
            SqlTypeStrategies.rtiBigint,
            null,
            SqlTypeStrategies.otcCharString,
            SqlFunctionCategory.System) {
            public boolean isDeterministic()
            {
                return false;
            }
        };

    /**
     * The <code>TABLESAMPLE</code> operator.
     *
     * <p>Examples:
     *
     * <ul>
     * <li><code>&lt;query&gt; TABLESAMPLE SUBSTITUTE('sampleName')</code>
     * (non-standard)
     * <li><code>&lt;query&gt; TABLESAMPLE BERNOULLI(&lt;percent&gt;)</code>
     * (standard, but not implemented yet)
     * </ul>
     *
     * <p>Operand #0 is a query or table; Operand #1 is a {@link SqlSampleSpec}
     * wrapped in a {@link SqlLiteral}.
     */
    public static final SqlSpecialOperator sampleFunction =
        new SqlSpecialOperator(
            "TABLESAMPLE",
            SqlKind.TableSample,
            20,
            true,
            SqlTypeStrategies.rtiFirstArgType,
            null,
            SqlTypeStrategies.otcVariadic) {
            public void unparse(
                SqlWriter writer,
                SqlNode [] operands,
                int leftPrec,
                int rightPrec)
            {
                operands[0].unparse(writer, leftPrec, 0);
                writer.keyword("TABLESAMPLE");
                operands[1].unparse(writer, 0, rightPrec);
            }
        };


    //~ Methods ----------------------------------------------------------------

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
}

// End SqlStdOperatorTable.java
