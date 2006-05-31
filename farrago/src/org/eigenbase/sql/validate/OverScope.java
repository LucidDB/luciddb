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

import org.eigenbase.sql.*;

/**
 * The name-resolution scope of a OVER clause. The objects visible are
 * those in the parameters found on the left side of the over clause, and
 * objects inherited from the parent scope.
 * <p/>
 * <p>This object is both a {@link SqlValidatorScope} only.
 * In the query
 * <p/>
 * <blockquote>
 * <pre>SELECT name FROM (
 *     SELECT *
 *     FROM emp over (order by empno range between 2 preceding and 2 following)
 * <p/>
 * <p>we need to use the {@link OverScope} as a
 * {@link SqlValidatorNamespace} when resolving names used in the window
 * specification.
 * <p/>
 *
 * @author jack
 * @version $Id$
 * @since Mar 25, 2003
 */
public class OverScope extends ListScope
{
    private final SqlCall overCall;

    /**
     * Creates a scope corresponding to a SELECT clause.
     *
     * @param parent Parent scope, or null
     * @param overCall
     */
    OverScope(SqlValidatorScope parent,
        SqlCall overCall)
    {
        super(parent);
        this.overCall = overCall;
    }

    public SqlNode getNode()
    {
        return overCall;
    }

    public boolean isMonotonic(SqlNode expr)
    {
        if (expr.isMonotonic(this)) {
            return true;
        }

        if (children.size() == 1) {
            final SqlNodeList monotonicExprs =
                children.get(0).getMonotonicExprs();
            for (int i = 0; i < monotonicExprs.size(); i++) {
                SqlNode monotonicExpr = monotonicExprs.get(i);
                if (expr.equalsDeep(monotonicExpr, false)) {
                    return true;
                }
            }
        }
        return super.isMonotonic(expr);
    }
}

// End OverScope.java
