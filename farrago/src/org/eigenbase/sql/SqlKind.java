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

package org.eigenbase.sql;

import org.eigenbase.util.EnumeratedValues;

/**
 * Enumerates the possible types of {@link SqlNode}.
 *
 * <p>Only commonly-used nodes have their own type; other nodes are of type
 * {@link #Other}. Some of the values, such as {@link #SetQuery},
 * represent aggregates.</p>
 *
 * @author jhyde
 * @version $Id$
 * @since Dec 12, 2003
 */
public class SqlKind extends EnumeratedValues.BasicValue
{
    //~ Static fields/initializers --------------------------------------------

    // the basics 0 - 99

    /** Other */
    public static final int OtherORDINAL = 0;
    public static final SqlKind Other = new SqlKind("Other", OtherORDINAL);

    /** SELECT statement or sub-query */
    public static final int SelectORDINAL = 1;
    public static final SqlKind Select = new SqlKind("Select", SelectORDINAL);

    /** JOIN operator or compound FROM clause.
     *
     * <p>A FROM clause with more than one table is represented as if it were a
     * join. For example, "FROM x, y, z" is represented as
     * "JOIN(x, JOIN(x, y))".</p> */
    public static final int JoinORDINAL = 2;
    public static final SqlKind Join = new SqlKind("Join", JoinORDINAL);

    /** Identifier */
    public static final int IdentifierORDINAL = 3;
    public static final SqlKind Identifier =
        new SqlKind("Identifier", IdentifierORDINAL);

    /** Literal */
    public static final int LiteralORDINAL = 4;
    public static final SqlKind Literal =
        new SqlKind("Literal", LiteralORDINAL);

    /** Function */
    public static final int FunctionORDINAL = 5;
    public static final SqlKind Function =
        new SqlKind("Function", FunctionORDINAL);

    /** EXPLAIN statement */
    public static final int ExplainORDINAL = 6;
    public static final SqlKind Explain =
        new SqlKind("Explain", ExplainORDINAL);

    /** INSERT statement */
    public static final int InsertORDINAL = 7;
    public static final SqlKind Insert = new SqlKind("Insert", InsertORDINAL);

    /** DELETE statement */
    public static final int DeleteORDINAL = 8;
    public static final SqlKind Delete = new SqlKind("Delete", DeleteORDINAL);

    /** UPDATE statement */
    public static final int UpdateORDINAL = 9;
    public static final SqlKind Update = new SqlKind("Update", UpdateORDINAL);

    /** Dynamic Param */
    public static final int DynamicParamORDINAL = 10;
    public static final SqlKind DynamicParam =
        new SqlKind("DynamicParam", DynamicParamORDINAL);

    public static final int OrderByORDINAL = 11;
    public static final SqlKind OrderBy =
        new SqlKind("OrderBy", OrderByORDINAL);

    /** Union */
    public static final int UnionORDINAL = 12;
    public static final SqlKind Union = new SqlKind("Union", UnionORDINAL);

    /** Except */
    public static final int ExceptORDINAL = 13;
    public static final SqlKind Except = new SqlKind("Except", ExceptORDINAL);

    /** Intersect */
    public static final int IntersectORDINAL = 14;
    public static final SqlKind Intersect =
        new SqlKind("Intersect", IntersectORDINAL);

    /** As */
    public static final int AsORDINAL = 15;
    public static final SqlKind As = new SqlKind("As", AsORDINAL);

    public static final int OverORDINAL = 16;
    /** Over */
    public static final SqlKind Over = new SqlKind("Over", OverORDINAL);

    public static final int WindowORDINAL = 17;
    /** Window specification */
    public static final SqlKind Window = new SqlKind("Window", WindowORDINAL);

    // binary operators
    // arithmetic 100 - 109

    /** Times */
    public static final int TimesORDINAL = 100;
    public static final SqlKind Times = new SqlKind("Times", TimesORDINAL);

    /** Divide */
    public static final int DivideORDINAL = 101;
    public static final SqlKind Divide = new SqlKind("Divide", DivideORDINAL);

    /** Plus */
    public static final int PlusORDINAL = 102;
    public static final SqlKind Plus = new SqlKind("Plus", PlusORDINAL);

    /** Minus */
    public static final int MinusORDINAL = 103;
    public static final SqlKind Minus = new SqlKind("Minus", MinusORDINAL);

    // comparison operators 110-119

    /** In */
    public static final int InORDINAL = 110;
    public static final SqlKind In = new SqlKind("In", InORDINAL);

    /** LessThan */
    public static final int LessThanORDINAL = 111;
    public static final SqlKind LessThan =
        new SqlKind("LessThan", LessThanORDINAL);

    /** GreaterThan */
    public static final int GreaterThanORDINAL = 112;
    public static final SqlKind GreaterThan =
        new SqlKind("GreaterThan", GreaterThanORDINAL);

    /** LessThanOrEqual */
    public static final int LessThanOrEqualORDINAL = 113;
    public static final SqlKind LessThanOrEqual =
        new SqlKind("LessThanOrEqual", LessThanOrEqualORDINAL);

    /** GreaterThanOrEqual */
    public static final int GreaterThanOrEqualORDINAL = 114;
    public static final SqlKind GreaterThanOrEqual =
        new SqlKind("GreaterThanOrEqual", GreaterThanOrEqualORDINAL);

    /** Equals */
    public static final int EqualsORDINAL = 115;
    public static final SqlKind Equals = new SqlKind("Equals", EqualsORDINAL);

    /** NotEquals */
    public static final int NotEqualsORDINAL = 116;
    public static final SqlKind NotEquals =
        new SqlKind("NotEquals", NotEqualsORDINAL);

    /** Comparison */
    public static final int ComparisonORDINAL = 119;
    public static final SqlKind Comparison =
        new SqlKind("Comparison", ComparisonORDINAL);

    /** Or */

    // boolean infix 120-129
    public static final int OrORDINAL = 120;
    public static final SqlKind Or = new SqlKind("Or", OrORDINAL);

    /** And */
    public static final int AndORDINAL = 121;
    public static final SqlKind And = new SqlKind("And", AndORDINAL);

    // other infix 130-139

    /** Dot */
    public static final int DotORDINAL = 130;
    public static final SqlKind Dot = new SqlKind("Dot", DotORDINAL);

    /** Overlaps */
    public static final int OverlapsORDINAL = 131;
    public static final SqlKind Overlaps =
        new SqlKind("Overlaps", OverlapsORDINAL);

    /** Like */
    public static final int LikeORDINAL = 132;
    public static final SqlKind Like = new SqlKind("Like", LikeORDINAL);

    /** Similar */
    public static final int SimilarORDINAL = 133;
    public static final SqlKind Similar =
        new SqlKind("Similar", SimilarORDINAL);

    /** Between */
    public static final int BetweenORDINAL = 134;
    public static final SqlKind Between =
        new SqlKind("Between", BetweenORDINAL);

    /** CASE  */
    public static final int CaseORDINAL = 135;
    public static final SqlKind Case = new SqlKind("CASE", CaseORDINAL);

    // prefix operators

    /** Not */
    public static final int NotORDINAL = 140;
    public static final SqlKind Not = new SqlKind("Not", NotORDINAL);

    /** PlusPrefix */
    public static final int PlusPrefixORDINAL = 141;
    public static final SqlKind PlusPrefix =
        new SqlKind("PlusPrefix", PlusPrefixORDINAL);

    /** MinusPrefix */
    public static final int MinusPrefixORDINAL = 142;
    public static final SqlKind MinusPrefix =
        new SqlKind("MinusPrefix", MinusPrefixORDINAL);

    /** Exists */
    public static final int ExistsORDINAL = 143;
    public static final SqlKind Exists = new SqlKind("Exists", ExistsORDINAL);

    /** Values */
    public static final int ValuesORDINAL = 144;
    public static final SqlKind Values = new SqlKind("Values", ValuesORDINAL);

    /** ExplicitTable */
    public static final int ExplicitTableORDINAL = 145;
    public static final SqlKind ExplicitTable =
        new SqlKind("ExplicitTable", ExplicitTableORDINAL);

    /** ProcedureCall */
    public static final int ProcedureCallORDINAL = 146;
    public static final SqlKind ProcedureCall =
        new SqlKind("ProcedureCall", ProcedureCallORDINAL);

    /** NewSpecification */
    public static final int NewSpecificationORDINAL = 147;
    public static final SqlKind NewSpecification =
        new SqlKind("NewSpecification", NewSpecificationORDINAL);

    // postfix operators

    /** Descending */
    public static final int DescendingORDINAL = 150;
    public static final SqlKind Descending =
        new SqlKind("Descending", DescendingORDINAL);

    /** IS TRUE */
    public static final int IsTrueORDINAL = 151;
    public static final SqlKind IsTrue = new SqlKind("IsTrue", IsTrueORDINAL);

    /** IS FALSE */
    public static final int IsFalseORDINAL = 152;
    public static final SqlKind IsFalse =
        new SqlKind("IsFalse", IsFalseORDINAL);

    /** IS UNKNOWN */
    public static final int IsUnknownORDINAL = 153;
    public static final SqlKind IsUnknown =
        new SqlKind("IsUnknown", IsUnknownORDINAL);

    /** IS NULL */
    public static final int IsNullORDINAL = 154;
    public static final SqlKind IsNull = new SqlKind("IsNull", IsNullORDINAL);

    // functions 160-169

    /** ROW function */
    public static final int RowORDINAL = 160;
    public static final SqlKind Row = new SqlKind("Row", RowORDINAL);

    /** CAST  */
    public static final int CastORDINAL = 161;
    public static final SqlKind Cast = new SqlKind("CAST", CastORDINAL);

    /** TRIM */
    public static final int TrimORDINAL = 162;
    public static final SqlKind Trim = new SqlKind("TRIM", TrimORDINAL);

    /** Call to a function using JDBC function syntax. */
    public static final int JdbcFnORDINAL = 163;
    public static final SqlKind JdbcFn = new SqlKind("JdbcFn", JdbcFnORDINAL);

    /** MultisetValueConstructor Value Constructor*/
    public static final int MultisetValueConstructorORDINAL = 164;
    public static final SqlKind MultisetValueConstructor =
        new SqlKind("MultisetValueConstructor", MultisetValueConstructorORDINAL);

    /** MultisetValueConstructor Query Constructor*/
    public static final int MultisetQueryConstructorORDINAL = 165;
    public static final SqlKind MultisetQueryConstructor =
        new SqlKind("MultisetQueryConstructor", MultisetQueryConstructorORDINAL);

    /** Unnest */
    public static final int UnnestORDINAL = 166;
    public static final SqlKind Unnest = new SqlKind("UNNEST", UnnestORDINAL);

    /** Lateral */
    public static final int LateralORDINAL = 167;
    public static final SqlKind Lateral = new SqlKind("LATERAL", LateralORDINAL);


    // internal operators (evaluated in validator) 200-299

    /** LiteralChain operator (for composite string literals) */
    public static final int LiteralChainORDINAL = 200;
    public static final SqlKind LiteralChain =
        new SqlKind("LiteralChain", LiteralChainORDINAL);

    /** Escape operator (always part of LIKE or SIMILAR TO expression) */
    public static final int EscapeORDINAL = 201;
    public static final SqlKind Escape =
        new SqlKind("EscapeChain", EscapeORDINAL);

    // aggregates of other kinds, 300-399

    /**
     * <code>SetQuery</code> is an aggregate of set-query node types.
     * <code>node.isA(Kind.SetQuery)</code> evaluates to
     * <code>true</code> if it <code>node</code> is an {@link #Except},
     * {@link #Intersect} or {@link #Union}.
     */
    public static final int SetQueryORDINAL = 300;
    public static final SqlKind SetQuery =
        new SqlKind("SetQuery", SetQueryORDINAL);

    /**
     * <code>Expression</code> is an aggregate of all expression
     * operators.
     */
    public static final int ExpressionORDINAL = 301;
    public static final SqlKind Expression =
        new SqlKind("Expression", ExpressionORDINAL);

    /**
     * <code>Dml</code> is an aggregate of all DML operators.
     * <code>node.isA(Kind.Dml)</code> evaluates to
     * <code>true</code> if it <code>node</code> is an {@link #Insert}
     * or {@link #Delete}.
     */
    public static final int DmlORDINAL = 302;
    public static final SqlKind Dml = new SqlKind("Dml", DmlORDINAL);

    /**
     * <code>Query</code> is an aggregate of query node types.
     * <code>node.isA(Kind.SetQuery)</code> evaluates to
     * <code>true</code> if it <code>node</code> is a {@link #Except},
     * {@link #Intersect}, {@link #Select} or {@link #Union}.
     */
    public static final int QueryORDINAL = 303;
    public static final SqlKind Query = new SqlKind("Query", QueryORDINAL);

    /**
     * Aggregate of SQL statement types {@link #Query}, {@link #Dml}.
     */
    public static final int TopLevelORDINAL = 304;
    public static final SqlKind TopLevel =
        new SqlKind("TopLevel", TopLevelORDINAL);

    public static final EnumeratedValues enumeration =
        new EnumeratedValues(new SqlKind [] {

            // the basics
            Other, Select, Join, Identifier, Literal, Function, Explain,
            Insert, Update, Delete, Union, Except, Intersect, As, Over, Window,
            // arithmetic
            Times, Divide, Plus, Minus,
            // comparisons
            In, LessThan, GreaterThan, LessThanOrEqual, GreaterThanOrEqual,
            Equals, NotEquals,
            // boolean
            Or, And,
            // other infix
            Dot, Overlaps, Like, Similar, Between, Case,
            // prefix
            Not, PlusPrefix, MinusPrefix, Exists, Values, ExplicitTable,
            // postfix
            Descending, IsTrue, IsFalse, IsNull,
            // row
            Row, Cast, Trim,
            // special
            MultisetValueConstructor, MultisetQueryConstructor, LiteralChain,
            Unnest, Lateral
            });

    //~ Constructors ----------------------------------------------------------

    private SqlKind(
        String name,
        int ordinal)
    {
        super(name, ordinal, null);
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * Returns whether this kind is the same as a given kind, or is a member
     * if the given kind is an aggregate
     */
    public boolean isA(SqlKind kind)
    {
        // todo: Members of aggregates, see how RexKind does it
        switch (kind.getOrdinal()) {
        case TopLevelORDINAL:
            return this.isA(Query) || this.isA(Dml);
        case QueryORDINAL:
            return (this == Select) || (this == Union) || (this == Intersect)
                || (this == Except) || (this == Values) || (this == OrderBy)
                || (this == ExplicitTable);
        case SetQueryORDINAL:
            return (this == Union) || (this == Intersect) || (this == Except);
        case DmlORDINAL:
            return (this == Insert) || (this == Delete) || (this == Update);
        case ExpressionORDINAL:
            return !((this == As) || (this == Descending) || (this == Select)
                || (this == Join) || (this == Function) || (this == Cast)
                || (this == Trim) || (this == LiteralChain) || (this == JdbcFn));
        case FunctionORDINAL:
            return (this == Function) || (this == Row) || (this == Trim)
                || (this == Cast) || (this == JdbcFn);
        case ComparisonORDINAL:
            return (this == In) || (this == LessThan) || (this == GreaterThan)
                || (this == LessThanOrEqual) || (this == GreaterThanOrEqual)
                || (this == Equals) || (this == NotEquals);
        default:
            return this == kind;
        }
    }

    /**
     * Looks up a kind from its ordinal.
     */
    public static SqlKind get(int ordinal)
    {
        return (SqlKind) enumeration.getValue(ordinal);
    }

    /**
     * Looks up a kind from its name.
     */
    public static SqlKind get(String name)
    {
        return (SqlKind) enumeration.getValue(name);
    }
}


// End SqlKind.java
