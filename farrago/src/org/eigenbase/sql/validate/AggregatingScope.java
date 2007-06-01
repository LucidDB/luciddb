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
 * An extension to the {@link SqlValidatorScope} interface which indicates that
 * the scope is aggregating.
 *
 * <p>A scope which is aggregating must implement this interface. Such a scope
 * will return the same set of identifiers as its parent scope, but some of
 * those identifiers may not be accessible because they are not in the GROUP BY
 * clause.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 25, 2003
 */
public interface AggregatingScope
    extends SqlValidatorScope
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Checks whether an expression is constant within the GROUP BY clause. If
     * the expression completely matches an expression in the GROUP BY clause,
     * returns true. If the expression is constant within the group, but does
     * not exactly match, returns false. If the expression is not constant,
     * throws an exception. Examples:
     *
     * <ul>
     * <li>If we are 'f(b, c)' in 'SELECT a + f(b, c) FROM t GROUP BY a', then
     * the whole expression matches a group column. Return true.
     * <li>Just an ordinary expression in a GROUP BY query, such as 'f(SUM(a),
     * 1, b)' in 'SELECT f(SUM(a), 1, b) FROM t GROUP BY b'. Returns false.
     * <li>Illegal expression, such as 'f(5, a, b)' in 'SELECT f(a, b) FROM t
     * GROUP BY a'. Throws when it enounters the 'b' operand, because it is not
     * in the group clause.
     * </ul>
     */
    boolean checkAggregateExpr(SqlNode expr, boolean deep);
}

// End AggregatingScope.java
