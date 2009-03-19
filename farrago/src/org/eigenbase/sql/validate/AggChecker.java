/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2004-2009 The Eigenbase Project
// Copyright (C) 2004-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
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

import org.eigenbase.resource.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.util.*;


/**
 * Visitor which throws an exception if any component of the expression is not a
 * group expression.
 *
 * @author jhyde
 * @version $Id$
 * @since Oct 28, 2004
 */
class AggChecker
    extends SqlBasicVisitor<Void>
{
    //~ Instance fields --------------------------------------------------------

    private final Stack<SqlValidatorScope> scopes =
        new Stack<SqlValidatorScope>();
    private final List<SqlNode> groupExprs;
    private boolean distinct;
    private SqlValidatorImpl validator;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates an AggChecker.
     *
     * @param validator Validator
     * @param scope Scope
     * @param groupExprs Expressions in GROUP BY (or SELECT DISTINCT) clause,
     * that are therefore available
     * @param distinct Whether aggregation checking is because of a SELECT
     * DISTINCT clause
     */
    AggChecker(
        SqlValidatorImpl validator,
        AggregatingScope scope,
        List<SqlNode> groupExprs,
        boolean distinct)
    {
        this.validator = validator;
        this.groupExprs = groupExprs;
        this.distinct = distinct;
        this.scopes.push(scope);
    }

    //~ Methods ----------------------------------------------------------------

    boolean isGroupExpr(SqlNode expr)
    {
        for (SqlNode groupExpr : groupExprs) {
            if (groupExpr.equalsDeep(expr, false)) {
                return true;
            }
        }
        return false;
    }

    public Void visit(SqlIdentifier id)
    {
        if (isGroupExpr(id)) {
            return null;
        }

        // If it '*' or 'foo.*'?
        if (id.isStar()) {
            assert false : "star should have been expanded";
        }

        // Is it a call to a parentheses-free function?
        SqlCall call =
            SqlUtil.makeCall(
                validator.getOperatorTable(),
                id);
        if (call != null) {
            return call.accept(this);
        }

        // Didn't find the identifer in the group-by list as is, now find
        // it fully-qualified.
        // TODO: It would be better if we always compared fully-qualified
        // to fully-qualified.
        final SqlIdentifier fqId = scopes.peek().fullyQualify(id);
        if (isGroupExpr(fqId)) {
            return null;
        }
        SqlNode originalExpr = validator.getOriginal(id);
        final String exprString = originalExpr.toString();
        throw validator.newValidationError(
            originalExpr,
            distinct
            ? EigenbaseResource.instance().NotSelectDistinctExpr.ex(
                exprString)
            : EigenbaseResource.instance().NotGroupExpr.ex(exprString));
    }

    public Void visit(SqlCall call)
    {
        if (call.getOperator().isAggregator()) {
            if (distinct) {
                // Cannot use agg fun in ORDER BY clause if have SELECT
                // DISTINCT.
                SqlNode originalExpr = validator.getOriginal(call);
                final String exprString = originalExpr.toString();
                throw validator.newValidationError(
                    call,
                    EigenbaseResource.instance().NotSelectDistinctExpr.ex(
                        exprString));
            }

            // For example, 'sum(sal)' in 'SELECT sum(sal) FROM emp GROUP
            // BY deptno'
            return null;
        }
        if (isGroupExpr(call)) {
            // This call matches an expression in the GROUP BY clause.
            return null;
        }
        if (call.isA(SqlKind.Query)) {
            // Allow queries for now, even though they may contain
            // references to forbidden columns.
            return null;
        }

        // Switch to new scope.
        SqlValidatorScope oldScope = scopes.peek();
        SqlValidatorScope newScope = oldScope.getOperandScope(call);
        scopes.push(newScope);

        // Visit the operands (only expressions).
        call.getOperator().acceptCall(
            this,
            call,
            true,
            ArgHandlerImpl.instance);

        // Restore scope.
        scopes.pop();
        return null;
    }
}

// End AggChecker.java
