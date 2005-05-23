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

import org.eigenbase.sql.SqlSelect;
import org.eigenbase.sql.SqlNodeList;
import org.eigenbase.sql.SqlNode;

/**
 * Scope for resolving identifers within a SELECT statement which has a
 * GROUP BY clause.
 *
 * <p>The same set of identifiers are in scope, but it won't allow
 * access to identifiers or expressions which are not group-expressions.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 25, 2003
 */
class AggregatingSelectScope
    extends SelectScope
    implements AggregatingScope
{
    private final AggChecker aggChecker;

    AggregatingSelectScope(
        SqlValidatorScope parent,
        SqlSelect select)
    {
        super(parent, select);
        SqlNodeList groupExprs = select.getGroup();
        if (groupExprs == null) {
            groupExprs = SqlNodeList.Empty;
        }
        // We deep-copy the group-list in case subsequent validation
        // modifies it and makes it no longer equivalent.
        groupExprs = SqlValidatorUtil.deepCopy(groupExprs);
        aggChecker = new AggChecker(validator, this, groupExprs);
    }

    public SqlValidatorScope getScopeAboveAggregation() {
        return parent;
    }

    public boolean checkAggregateExpr(SqlNode expr) {
        // Make sure expression is valid, throws if not.
        expr.accept(aggChecker);
        return aggChecker.isGroupExpr(expr);
    }
}

// End AggregatingSelectScope.java

