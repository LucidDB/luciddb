/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2004-2007 The Eigenbase Project
// Copyright (C) 2004-2007 SQLstream, Inc.
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
import org.eigenbase.util.*;


/**
 * The name-resolution scope of a OVER clause. The objects visible are those in
 * the parameters found on the left side of the over clause, and objects
 * inherited from the parent scope.
 *
 * <p>This object is both a {@link SqlValidatorScope} only. In the query
 *
 * <blockquote>
 * <pre>SELECT name FROM (
 *     SELECT *
 *     FROM emp OVER (
 *         ORDER BY empno
 *         RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING))
 * </pre>
 * </blockquote>
 *
 * <p/>
 * <p>We need to use the {@link OverScope} as a {@link SqlValidatorNamespace}
 * when resolving names used in the window specification.</p>
 *
 * @author jack
 * @version $Id$
 * @since Jun 29, 2005
 */
public class OverScope
    extends ListScope
{
    //~ Instance fields --------------------------------------------------------

    private final SqlCall overCall;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a scope corresponding to a SELECT clause.
     *
     * @param parent Parent scope, or null
     * @param overCall Call to OVER operator
     */
    OverScope(
        SqlValidatorScope parent,
        SqlCall overCall)
    {
        super(parent);
        this.overCall = overCall;
    }

    //~ Methods ----------------------------------------------------------------

    public SqlNode getNode()
    {
        return overCall;
    }

    public SqlMonotonicity getMonotonicity(SqlNode expr)
    {
        SqlMonotonicity monotonicity = expr.getMonotonicity(this);
        if (monotonicity != SqlMonotonicity.NotMonotonic) {
            return monotonicity;
        }

        if (children.size() == 1) {
            final SqlValidatorNamespace child = children.get(0);
            final List<Pair<SqlNode, SqlMonotonicity>> monotonicExprs =
                child.getMonotonicExprs();
            for (Pair<SqlNode, SqlMonotonicity> pair : monotonicExprs) {
                if (expr.equalsDeep(pair.left, false)) {
                    return pair.right;
                }
            }
        }
        return super.getMonotonicity(expr);
    }
}

// End OverScope.java
