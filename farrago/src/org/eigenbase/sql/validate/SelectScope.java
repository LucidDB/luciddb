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
import org.eigenbase.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.ListIterator;

/**
 * The name-resolution scope of a SELECT clause. The objects visible are
 * those in the FROM clause, and objects inherited from the parent scope.
 * <p/>
 * <p>This object is both a {@link SqlValidatorScope} and a
 * {@link SqlValidatorNamespace}. In the query
 * <p/>
 * <blockquote>
 * <pre>SELECT name FROM (
 *     SELECT *
 *     FROM emp
 *     WHERE gender = 'F')</code></blockquote>
 * <p/>
 * <p>we need to use the {@link SelectScope} as a
 * {@link SqlValidatorNamespace} when resolving 'name', and
 * as a {@link SqlValidatorScope} when resolving 'gender'.</p>
 * <p/>
 * <h3>Scopes</h3>
 * <p/>
 * <p>In the query
 * <p/>
 * <blockquote>
 * <pre>
 * SELECT expr1
 * FROM t1,
 *     t2,
 *     (SELECT expr2 FROM t3) AS q3
 * WHERE c1 IN (SELECT expr3 FROM t4)
 * ORDER BY expr4</pre></blockquote>
 * <p/>
 * The scopes available at various points of the query are as follows:<ul>
 * <li>expr1 can see t1, t2, q3</li>
 * <li>expr2 can see t3</li>
 * <li>expr3 can see t4, t1, t2</li>
 * <li>expr4 can see t1, t2, q3, plus (depending upon the dialect) any
 *     aliases defined in the SELECT clause</li>
 * </ul>
 * <p/>
 * <h3>Namespaces</h3>
 * <p/>
 * <p>In the above query, there are 4 namespaces:<ul>
 * <li>t1</li>
 * <li>t2</li>
 * <li>(SELECT expr2 FROM t3) AS q3</li>
 * <li>(SELECT expr3 FROM t4)</li>
 * </ul>
 *
 * @see SelectNamespace
 * @author jhyde
 * @version $Id$
 * @since Mar 25, 2003
 */
public class SelectScope extends ListScope
{
    private final SqlSelect select;
    protected final ArrayList windowNames = new ArrayList();

    /**
     * List of column names which sort this scope.
     * Empty if this scope is not sorted.
     * Null if has not been computed yet.
     */
    private SqlNodeList orderList;

    /**
     * Creates a scope corresponding to a SELECT clause.
     *
     * @param parent Parent scope, or null
     * @param select
     */
    SelectScope(SqlValidatorScope parent,
        SqlSelect select)
    {
        super(parent);
        this.select = select;
    }

    public SqlValidatorTable getTable()
    {
        return null;
    }

    public SqlNode getNode()
    {
        return select;
    }

    public SqlWindow lookupWindow(String name)
    {
        final SqlNodeList windowList = select.getWindowList();
        for (int i = 0; i < windowList.size(); i++) {
            SqlWindow window = (SqlWindow) windowList.get(i);
            final SqlIdentifier declId = window.getDeclName();
            assert declId.isSimple();
            if (declId.names[0].equals(name)) {
                return window;
            }
        }
        return super.lookupWindow(name);
    }

    public boolean isMonotonic(SqlNode expr)
    {
        if (expr.isMonotonic(this)) {
            return true;
        }

        // TODO: compare fully qualified names
        final SqlNodeList orderList = getOrderList();
        if (orderList.size() == 1 &&
            expr.equalsDeep((SqlNode) orderList.get(0), false)) {
            return true;
        }

        return super.isMonotonic(expr);
    }

    public SqlNodeList getOrderList()
    {
        if (orderList == null) {
            // Compute on demand first call.
            orderList = new SqlNodeList(SqlParserPos.ZERO);
            if (children.size() == 1) {
                final SqlNodeList monotonicExprs =
                    children.get(0).getMonotonicExprs();
                if (monotonicExprs.size() > 0) {
                    orderList.add(monotonicExprs.get(0));
                }
            }
        }
        return orderList;
    }

    public void addWindowName(String winName)
    {
        windowNames.add(winName);
    }

    public boolean existingWindowName(String winName)
    {
        String listName;
        ListIterator entry = windowNames.listIterator();
        while(entry.hasNext()) {
            listName = (String)entry.next();
            if (0 == listName.compareToIgnoreCase(winName)) {
                return true;
            }
        }
        // if the name wasn't found then check the parent(s)
        SqlValidatorScope walker = parent;
        while (null != walker && !(walker instanceof EmptyScope)) {
            if (walker instanceof SelectScope) {
                final SelectScope parentScope = (SelectScope) walker;
                return parentScope.existingWindowName(winName);
            }
            walker = parent;
        }

        return false;
    }
}

// End SelectScope.java
