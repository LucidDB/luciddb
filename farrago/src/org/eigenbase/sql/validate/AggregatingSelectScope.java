/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2004-2007 The Eigenbase Project
// Copyright (C) 2004-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
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
package org.eigenbase.sql.validate;

import java.util.*;

import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;


/**
 * Scope for resolving identifers within a SELECT statement which has a GROUP BY
 * clause.
 *
 * <p>The same set of identifiers are in scope, but it won't allow access to
 * identifiers or expressions which are not group-expressions.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 25, 2003
 */
public class AggregatingSelectScope
    extends DelegatingScope
    implements AggregatingScope
{
    //~ Instance fields --------------------------------------------------------

    private final SqlSelect select;
    private final boolean distinct;
    private final List<SqlNode> groupExprList;

    //~ Constructors -----------------------------------------------------------

    AggregatingSelectScope(
        SqlValidatorScope selectScope,
        SqlSelect select,
        boolean distinct)
    {
        // The select scope is the parent in the sense that all columns which
        // are available in the select scope are available. Whether they are
        // valid as aggregation expressions... now that's a different matter.
        super(selectScope);
        this.select = select;
        this.distinct = distinct;
        if (distinct) {
            groupExprList = null;
        } else if (select.getGroup() != null) {
            // We deep-copy the group-list in case subsequent validation
            // modifies it and makes it no longer equivalent. While copying,
            // we fully qualify all identifiers.
            SqlNodeList sqlNodeList =
                (SqlNodeList) this.select.getGroup().accept(
                    new SqlValidatorUtil.DeepCopier(parent));
            this.groupExprList = sqlNodeList.getList();
        } else {
            groupExprList = null;
        }
    }

    /**
     * Returns the expressions that are in the GROUP BY clause (or the
     * SELECT DISTINCT clause, if distinct) and that can therefore be
     * referenced without being wrapped in aggregate functions.
     *
     * <p>The expressions are fully-qualified, and any "*" in select clauses
     * are expanded.
     *
     * @return list of grouping expressions
     */
    private List<SqlNode> getGroupExprs()
    {
        if (distinct) {
            // Cannot compute this in the constructor: select list has not been
            // expanded yet.
            assert select.isDistinct();
            
            // Remove the AS operator so the expressions are consistent with
            // OrderExpressionExpander.
            List<SqlNode> groupExprs = new ArrayList<SqlNode>();
            for (SqlNode selectItem :
                ((SelectScope) parent).getExpandedSelectList())
            {
                if (SqlUtil.isCallTo(
                    selectItem,
                    SqlStdOperatorTable.asOperator))
                {
                    groupExprs.add(((SqlCall) selectItem).getOperands()[0]);
                } else {
                    groupExprs.add(selectItem);
                }
            }
            return groupExprs;
        } else if (select.getGroup() != null) {
            return groupExprList;
        } else {
            return Collections.emptyList();
        }
    }

    //~ Methods ----------------------------------------------------------------

    public SqlNode getNode()
    {
        return select;
    }

    public SqlValidatorScope getOperandScope(SqlCall call)
    {
        if (call.getOperator().isAggregator()) {
            // If we're the 'SUM' node in 'select a + sum(b + c) from t
            // group by a', then we should validate our arguments in
            // the non-aggregating scope, where 'b' and 'c' are valid
            // column references.
            return parent;
        } else if (call instanceof SqlWindow) {
            return parent;
        } else {
            // Check whether expression is constant within the group.
            //
            // If not, throws. Example, 'empno' in
            //    SELECT empno FROM emp GROUP BY deptno
            //
            // If it perfectly matches an expression in the GROUP BY
            // clause, we validate its arguments in the non-aggregating
            // scope. Example, 'empno + 1' in
            //
            //   SELET empno + 1 FROM emp GROUP BY empno + 1

            final boolean matches = checkAggregateExpr(call, false);
            if (matches) {
                return parent;
            }
        }
        return super.getOperandScope(call);
    }

    public boolean checkAggregateExpr(SqlNode expr, boolean deep)
    {
        // Fully-qualify any identifiers in expr.
        if (deep) {
            expr = validator.expand(expr, this);
        }

        // Make sure expression is valid, throws if not.
        List<SqlNode> groupExprs = getGroupExprs();
        final AggChecker aggChecker =
            new AggChecker(
                validator,
                this,
                groupExprs,
                distinct);
        if (deep) {
            expr.accept(aggChecker);
        }

        // Return whether expression exactly matches one of the group
        // expressions.
        return aggChecker.isGroupExpr(expr);
    }

    public void validateExpr(SqlNode expr)
    {
        checkAggregateExpr(expr, true);
    }
}

// End AggregatingSelectScope.java
