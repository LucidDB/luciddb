/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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

package org.eigenbase.sql.test;

import org.eigenbase.sql.type.SqlTypeName;


/**
 * Abstract implementation of {@link SqlTester}. A derived class only needs
 * to implement {@link #check} and {@link #checkType}.
 *
 * @author wael
 * @since May 22, 2004
 * @version $Id$
 **/
public abstract class AbstractSqlTester implements SqlTester
{
    //~ Methods ---------------------------------------------------------------

    /**
     * Converts a scalar expression into a SQL query.
     *
     * <p>By default, "expr" becomes "VALUES (expr)". Derived
     * classes may override.
     */
    protected String buildQuery(String expression)
    {
        return "values (" + expression + ")";
    }

    public void checkScalarExact(
        String expression,
        String result)
    {
        String sql = buildQuery(expression);
        check(sql, result, SqlTypeName.Integer);
    }

    public void checkScalarApprox(
        String expression,
        String result)
    {
        String sql = buildQuery(expression);
        check(sql, result, SqlTypeName.Double);
    }

    public void checkBoolean(
        String expression,
        Boolean result)
    {
        String sql = buildQuery(expression);
        if (null == result) {
            checkNull(expression);
        } else {
            check(
                sql,
                result.toString(),
                SqlTypeName.Boolean);
        }
    }

    public void checkString(
        String expression,
        String result)
    {
        String sql = buildQuery(expression);
        check(sql, result, SqlTypeName.Boolean);
    }

    public void checkNull(String expression)
    {
        String sql = buildQuery(expression);

        //any SqlTypeName should do
        check(sql, null, SqlTypeName.Boolean);
    }

    public void checkScalar(
        String expression,
        Object result,
        String resultType)
    {
        checkType(expression, resultType);
        check(buildQuery(expression), result, null);
    }
}
