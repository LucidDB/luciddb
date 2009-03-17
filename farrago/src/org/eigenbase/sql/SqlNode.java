/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2002-2007 Disruptive Tech
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
package org.eigenbase.sql;

import java.util.*;

import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.pretty.*;
import org.eigenbase.sql.util.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.util.*;


/**
 * A <code>SqlNode</code> is a SQL parse tree. It may be an {@link SqlOperator
 * operator}, {@link SqlLiteral literal}, {@link SqlIdentifier identifier}, and
 * so forth.
 *
 * @author jhyde
 * @version $Id$
 * @since Dec 12, 2003
 */
public abstract class SqlNode
    implements Cloneable
{
    //~ Static fields/initializers ---------------------------------------------

    public static final SqlNode [] emptyArray = new SqlNode[0];

    //~ Instance fields --------------------------------------------------------

    private final SqlParserPos pos;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a node.
     *
     * @param pos Parser position, must not be null.
     */
    SqlNode(SqlParserPos pos)
    {
        Util.pre(pos != null, "pos != null");
        this.pos = pos;
    }

    //~ Methods ----------------------------------------------------------------

    public Object clone()
    {
        return clone(getParserPosition());
    }

    /**
     * Clones a SqlNode with a different position.
     */
    public SqlNode clone(SqlParserPos pos)
    {
        // REVIEW jvs 26-July-2006:  shouldn't pos be used here?  Or are
        // subclasses always supposed to override, in which case this
        // method should probably be abstract?
        try {
            return (SqlNode) super.clone();
        } catch (CloneNotSupportedException e) {
            throw Util.newInternal(e, "error while cloning " + this);
        }
    }

    /**
     * Returns whether this node is a particular kind.
     *
     * @param kind a {@link org.eigenbase.sql.SqlKind} value
     */
    public boolean isA(SqlKind kind)
    {
        // REVIEW jvs 6-Feb-2005:  either this should be
        // getKind().isA(kind), or this method should be renamed to
        // avoid confusion
        return getKind() == kind;
    }

    /**
     * Returns the type of node this is, or {@link
     * org.eigenbase.sql.SqlKind#Other} if it's nothing special.
     *
     * @return a {@link SqlKind} value, never null
     */
    public SqlKind getKind()
    {
        return SqlKind.Other;
    }

    public static SqlNode [] cloneArray(SqlNode [] nodes)
    {
        SqlNode [] clones = (SqlNode []) nodes.clone();
        for (int i = 0; i < clones.length; i++) {
            SqlNode node = clones[i];
            if (node != null) {
                clones[i] = (SqlNode) node.clone();
            }
        }
        return clones;
    }

    public String toString()
    {
        return toSqlString(null);
    }

    /**
     * Returns the SQL text of the tree of which this <code>SqlNode</code> is
     * the root.
     *
     * @param dialect
     * @param forceParens wraps all expressions in parentheses; good for parse
     * test, but false by default.
     *
     * <p>Typical return values are:
     *
     * <ul>
     * <li>'It''s a bird!'</li>
     * <li>NULL</li>
     * <li>12.3</li>
     * <li>DATE '1969-04-29'</li>
     * </ul>
     */
    public String toSqlString(SqlDialect dialect, boolean forceParens)
    {
        if (dialect == null) {
            dialect = SqlUtil.dummyDialect;
        }
        SqlPrettyWriter writer = new SqlPrettyWriter(dialect);
        writer.setAlwaysUseParentheses(forceParens);
        writer.setSelectListItemsOnSeparateLines(false);
        writer.setIndentation(0);
        unparse(writer, 0, 0);
        return writer.toString();
    }

    public String toSqlString(SqlDialect dialect)
    {
        return toSqlString(dialect, false);
    }

    /**
     * Writes a SQL representation of this node to a writer.
     *
     * <p>The <code>leftPrec</code> and <code>rightPrec</code> parameters give
     * us enough context to decide whether we need to enclose the expression in
     * parentheses. For example, we need parentheses around "2 + 3" if preceded
     * by "5 *". This is because the precedence of the "*" operator is greater
     * than the precedence of the "+" operator.
     *
     * <p>The algorithm handles left- and right-associative operators by giving
     * them slightly different left- and right-precedence.
     *
     * <p>If {@link SqlWriter#isAlwaysUseParentheses()} is true, we use
     * parentheses even when they are not required by the precedence rules.
     *
     * <p>For the details of this algorithm, see {@link SqlCall#unparse}.
     *
     * @param writer Target writer
     * @param leftPrec The precedence of the {@link SqlNode} immediately
     * preceding this node in a depth-first scan of the parse tree
     * @param rightPrec The precedence of the {@link SqlNode} immediately
     */
    public abstract void unparse(
        SqlWriter writer,
        int leftPrec,
        int rightPrec);

    public SqlParserPos getParserPosition()
    {
        return pos;
    }

    /**
     * Validates this node.
     *
     * <p>The typical implementation of this method will make a callback to the
     * validator appropriate to the node type and context. The validator has
     * methods such as {@link SqlValidator#validateLiteral} for these purposes.
     *
     * @param scope Validator
     */
    public abstract void validate(
        SqlValidator validator,
        SqlValidatorScope scope);

    /**
     * Lists all the valid alternatives for this node if the parse position of
     * the node matches that of pos. Only implemented now for SqlCall and
     * SqlOperator.
     *
     * @param validator Validator
     * @param scope Validation scope
     * @param pos SqlParserPos indicating the cursor position at which competion
     * hints are requested for
     * @param hintList list of valid options
     */
    public void findValidOptions(
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlParserPos pos,
        List<SqlMoniker> hintList)
    {
        // no valid options
    }

    /**
     * Validates this node in an expression context.
     *
     * <p>Usually, this method does much the same as {@link #validate}, but a
     * {@link SqlIdentifier} can occur in expression and non-expression
     * contexts.
     */
    public void validateExpr(
        SqlValidator validator,
        SqlValidatorScope scope)
    {
        validate(validator, scope);
        Util.discard(validator.deriveType(scope, this));
    }

    /**
     * Accepts a generic visitor.
     *
     * <p>Implementations of this method in subtypes simply call the appropriate
     * <code>visit</code> method on the {@link org.eigenbase.sql.util.SqlVisitor
     * visitor object}.
     *
     * <p>The type parameter <code>R</code> must be consistent with the type
     * parameter of the visitor.
     */
    public abstract <R> R accept(SqlVisitor<R> visitor);

    /**
     * Returns whether this node is structurally equivalent to another node.
     * Some examples:
     *
     * <ul>
     * <li>1 + 2 is structurally equivalent to 1 + 2</li>
     * <li>1 + 2 + 3 is structurally equivalent to (1 + 2) + 3, but not to 1 +
     * (2 + 3), because the '+' operator is left-associative</li>
     * </ul>
     */
    public abstract boolean equalsDeep(SqlNode node, boolean fail);

    /**
     * Returns whether two nodes are equal (using {@link
     * #equalsDeep(SqlNode,boolean)}) or are both null.
     *
     * @param node1 First expression
     * @param node2 Second expression
     * @param fail Whether to throw {@link AssertionError} if expressions are
     * not equal
     */
    public static boolean equalDeep(
        SqlNode node1,
        SqlNode node2,
        boolean fail)
    {
        if (node1 == null) {
            return node2 == null;
        } else if (node2 == null) {
            return false;
        } else {
            return node1.equalsDeep(node2, fail);
        }
    }

    /**
     * Returns whether expression is always ascending, descending or constant.
     * This property is useful because it allows to safely aggregte infinite
     * streams of values.
     *
     * <p>The default implementation returns {@link
     * SqlMonotonicity#NotMonotonic}.
     */
    public SqlMonotonicity getMonotonicity(SqlValidatorScope scope)
    {
        return SqlMonotonicity.NotMonotonic;
    }
}

// End SqlNode.java
