/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2003-2003 Disruptive Technologies, Inc.
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

import net.sf.saffron.util.EnumeratedValues;

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
    private SqlKind(String name, int ordinal) {
        super(name, ordinal, null);
    }

    // REVIEW jvs 8-Feb-2004:  time for a RENUM?

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
    public static final SqlKind Identifier = new SqlKind("Identifier", IdentifierORDINAL);
    /** Literal */
    public static final int LiteralORDINAL = 4;
    public static final SqlKind Literal = new SqlKind("Literal", LiteralORDINAL);
    /** Function */
    public static final int FunctionORDINAL = 5;
    public static final SqlKind Function = new SqlKind("Function", FunctionORDINAL);
    /** EXPLAIN statement */
    public static final int ExplainORDINAL = 6;
    public static final SqlKind Explain = new SqlKind("Explain", ExplainORDINAL);
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
    public static final SqlKind DynamicParam = new SqlKind("DynamicParam",DynamicParamORDINAL);

    public static final int OrderByORDINAL = 11;
    public static final SqlKind OrderBy = new SqlKind("OrderBy", OrderByORDINAL);

    // binary operators
    /** Times */
    public static final int TimesORDINAL = 12;
    public static final SqlKind Times = new SqlKind("Times", TimesORDINAL);
    /** Divide */
    public static final int DivideORDINAL = 13;
    public static final SqlKind Divide = new SqlKind("Divide", DivideORDINAL);
    /** Plus */
    public static final int PlusORDINAL = 14;
    public static final SqlKind Plus = new SqlKind("Plus", PlusORDINAL);
    /** Minus */
    public static final int MinusORDINAL = 15;
    public static final SqlKind Minus = new SqlKind("Minus", MinusORDINAL);
    /** In */
    public static final int InORDINAL = 29;
    public static final SqlKind In = new SqlKind("In", InORDINAL);
    /** LessThan */
    public static final int LessThanORDINAL = 16;
    public static final SqlKind LessThan = new SqlKind("LessThan", LessThanORDINAL);
    /** GreaterThan */
    public static final int GreaterThanORDINAL = 17;
    public static final SqlKind GreaterThan = new SqlKind("GreaterThan", GreaterThanORDINAL);
    /** LessThanOrEqual */
    public static final int LessThanOrEqualORDINAL = 18;
    public static final SqlKind LessThanOrEqual = new SqlKind("LessThanOrEqual", LessThanOrEqualORDINAL);
    /** GreaterThanOrEqual */
    public static final int GreaterThanOrEqualORDINAL = 19;
    public static final SqlKind GreaterThanOrEqual = new SqlKind("GreaterThanOrEqual", GreaterThanOrEqualORDINAL);
    /** Equals */
    public static final int EqualsORDINAL = 20;
    public static final SqlKind Equals = new SqlKind("Equals", EqualsORDINAL);
    /** NotEquals */
    public static final int NotEqualsORDINAL = 21;
    public static final SqlKind NotEquals = new SqlKind("NotEquals", NotEqualsORDINAL);
    /** Or */
    public static final int OrORDINAL = 22;
    public static final SqlKind Or = new SqlKind("Or", OrORDINAL);
    /** And */
    public static final int AndORDINAL = 23;
    public static final SqlKind And = new SqlKind("And", AndORDINAL);
    /** Dot */
    public static final int DotORDINAL = 24;
    public static final SqlKind Dot = new SqlKind("Dot", DotORDINAL);
    /** Union */
    public static final int UnionORDINAL = 25;
    public static final SqlKind Union = new SqlKind("Union", UnionORDINAL);
    /** Except */
    public static final int ExceptORDINAL = 26;
    public static final SqlKind Except = new SqlKind("Except", ExceptORDINAL);
    /** Intersect */
    public static final int IntersectORDINAL = 27;
    public static final SqlKind Intersect = new SqlKind("Intersect", IntersectORDINAL);
    /** As */
    public static final int AsORDINAL = 28;
    public static final SqlKind As = new SqlKind("As", AsORDINAL);
    /** Overlaps */
    public static final int OverlapsORDINAL = 37;
    public static final SqlKind Overlaps = new SqlKind("Overlaps", OverlapsORDINAL);
    /** Like */
    public static final int LikeORDINAL = 38;
    public static final SqlKind Like = new SqlKind("Like", LikeORDINAL);
    /** Similar */
    public static final int SimilarORDINAL = 39;
    public static final SqlKind Similar = new SqlKind("Similar", SimilarORDINAL);
    /** Between */
    public static final int BetweenORDINAL = 41;
    public static final SqlKind Between = new SqlKind("Between", BetweenORDINAL);
    /** Not Between */
    public static final int NotBetweenORDINAL = 42;
    public static final SqlKind NotBetween = new SqlKind("Not Between", NotBetweenORDINAL);
    /** CASE  */
    public static final int CaseORDINAL = 47;
    public static final SqlKind Case = new SqlKind("CASE", CaseORDINAL);

    // prefix operators
    /** Not */
    public static final int NotORDINAL = 30;
    public static final SqlKind Not = new SqlKind("Not", NotORDINAL);
    /** PlusPrefix */
    public static final int PlusPrefixORDINAL = 31;
    public static final SqlKind PlusPrefix = new SqlKind("PlusPrefix", PlusPrefixORDINAL);
    /** MinusPrefix */
    public static final int MinusPrefixORDINAL = 32;
    public static final SqlKind MinusPrefix = new SqlKind("MinusPrefix", MinusPrefixORDINAL);
    /** Exists */
    public static final int ExistsORDINAL = 33;
    public static final SqlKind Exists = new SqlKind("Exists", ExistsORDINAL);
    /** Values */
    public static final int ValuesORDINAL = 36;
    public static final SqlKind Values = new SqlKind("Values", ValuesORDINAL);
    /** ExplicitTable */
    public static final int ExplicitTableORDINAL = 45;
    public static final SqlKind ExplicitTable = new SqlKind("ExplicitTable", ExplicitTableORDINAL);

    // postfix operators
    /** Descending */
    public static final int DescendingORDINAL = 40;
    public static final SqlKind Descending = new SqlKind("Descending", DescendingORDINAL);

    /** IS TRUE */
    public static final int IsTrueORDINAL = 43;
    public static final SqlKind IsTrue= new SqlKind("IsTrue", IsTrueORDINAL);

    /** IS FALSE */
    public static final int IsFalseORDINAL = 44;
    public static final SqlKind IsFalse= new SqlKind("IsFalse", IsFalseORDINAL);

    /** IS NULL */
    public static final int IsNullORDINAL = 46;
    public static final SqlKind IsNull= new SqlKind("IsNull", IsNullORDINAL);

    // functions
    /** ROW function */
    public static final int RowORDINAL = 50;
    public static final SqlKind Row = new SqlKind("Row", RowORDINAL);

    // aggregates of other kinds

    /**
     * <code>SetQuery</code> is an aggregate of set-query node types.
     * <code>node.isA(Kind.SetQuery)</code> evaluates to
     * <code>true</code> if it <code>node</code> is an {@link #Except},
     * {@link #Intersect} or {@link #Union}.
     */
    public static final int SetQueryORDINAL = 100;
    public static final SqlKind SetQuery = new SqlKind("SetQuery", SetQueryORDINAL);

    /**
     * <code>Expression</code> is an aggregate of all expression
     * operators.
     */
    public static final int ExpressionORDINAL = 101;
    public static final SqlKind Expression = new SqlKind("Expression", ExpressionORDINAL);

    /**
     * <code>Dml</code> is an aggregate of all DML operators.
     * <code>node.isA(Kind.Dml)</code> evaluates to
     * <code>true</code> if it <code>node</code> is an {@link #Insert}
     * or {@link #Delete}.
     */
    public static final int DmlORDINAL = 102;
    public static final SqlKind Dml = new SqlKind("Dml", DmlORDINAL);

    public static final int QueryORDINAL = 103;
    /**
     * <code>Query</code> is an aggregate of query node types.
     * <code>node.isA(Kind.SetQuery)</code> evaluates to
     * <code>true</code> if it <code>node</code> is a {@link #Except},
     * {@link #Intersect}, {@link #Select} or {@link #Union}.
     */
    public static final SqlKind Query = new SqlKind("Query", QueryORDINAL);

    public static final int TopLevelORDINAL = 104;
    /**
     * Aggregate of SQL statement types {@link #Query}, {@link #Dml}.
     */
    public static final SqlKind TopLevel = new SqlKind("TopLevel", TopLevelORDINAL);

    public static final EnumeratedValues enumeration = new EnumeratedValues(
            new SqlKind[] {
                Other,Select,Join,Identifier,Literal,Times,Divide,Plus,
                Minus,In,LessThan,GreaterThan,LessThanOrEqual,
                GreaterThanOrEqual,Equals,NotEquals,Or,And,Dot,Union,
                Except,Intersect,As,Not,PlusPrefix,MinusPrefix,Exists,
                Values,ExplicitTable,Descending,IsTrue,IsFalse,IsNull,Row,
                Explain,Insert,Update,Delete,
            });

    /**
     * Returns whether this kind is the same as a given kind, or is a member
     * if the given kind is an aggregate
     */
    public boolean isA(SqlKind kind) {
        // todo: Members of aggregates, see how RexKind does it
        switch (kind.getOrdinal()) {
        case TopLevelORDINAL:
            return this.isA(Query) || this.isA(Dml);
        case QueryORDINAL:
            return this == Select || this == Union || this == Intersect
                || this == Except || this == Values || this == OrderBy
                || this == ExplicitTable;
        case SetQueryORDINAL:
            return this == Union || this == Intersect || this == Except;
        case DmlORDINAL:
            return this == Insert || this == Delete || this == Update;
        case ExpressionORDINAL:
            return !(this == As || this == Descending || this == Select
                    || this == Join || this == Function);
        case FunctionORDINAL:
            return this == Function || this == Row;
        default:
            return this == kind;
        }
    }

    /**
     * Looks up a kind from its ordinal.
     */
    public static SqlKind get(int ordinal) {
        return (SqlKind) enumeration.getValue(ordinal);
    }
    /**
     * Looks up a kind from its name.
     */
    public static SqlKind get(String name) {
        return (SqlKind) enumeration.getValue(name);
    }
}

// End SqlKind.java
