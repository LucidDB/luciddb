/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2004-2005 The Eigenbase Project
// Copyright (C) 2004-2005 Disruptive Tech
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
package org.eigenbase.sql.validate;

import org.eigenbase.sql.util.SqlBasicVisitor;
import org.eigenbase.sql.*;
import org.eigenbase.resource.EigenbaseResource;

/**
 * Visitor which throws an exception if any component of the expression is
 * not a group expression.
 *
 * @author jhyde
 * @since Oct 28, 2004
 * @version $Id$
 */
class AggChecker extends SqlBasicVisitor<Void>
{
    private final AggregatingScope scope;
    private final SqlNodeList groupExprs;
    private SqlValidatorImpl validator;

    /**
     * Creates an AggChecker
     */
    AggChecker(
        SqlValidatorImpl validator,
        AggregatingScope scope,
        SqlNodeList groupExprs)
    {
        this.validator = validator;
        this.groupExprs = groupExprs;
        this.scope = scope;
    }

    boolean isGroupExpr(SqlNode expr) {
        for (int i = 0; i < groupExprs.size(); i++) {
            SqlNode groupExpr = groupExprs.get(i);
            if (groupExpr.equalsDeep(expr)) {
                return true;
            }
        }
        return false;
    }

    public Void visit(SqlIdentifier id) {
        if (isGroupExpr(id)) {
            return null;
        }
        // Is it a call to a parentheses-free function?
        SqlCall call = SqlUtil.makeCall(validator.getOperatorTable(), id);
        if (call != null) {
            return call.accept(this);
        }
        // Didn't find the identifer in the group-by list as is, now find
        // it fully-qualified.
        // TODO: It would be better if we always compared fully-qualified
        // to fully-qualified.
        final SqlIdentifier fqId = scope.fullyQualify(id);
        if (isGroupExpr(fqId)) {
            return null;
        }
        final String exprString = id.toString();
        throw scope.getValidator().newValidationError(id,
            EigenbaseResource.instance().NotGroupExpr.ex(exprString));
    }

    public Void visit(SqlCall call) {
        if (call.getOperator().isAggregator()) {
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
        // Visit the operands.
        return super.visit(call);
    }
}

// End AggChecker.java
