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

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;


/**
 * Represents the name-resolution context for expressions in an ORDER BY clause.
 *
 * <p>In some dialects of SQL, the ORDER BY clause can reference column aliases
 * in the SELECT clause. For example, the query
 *
 * <blockquote><code>SELECT empno AS x<br/>
 * FROM emp<br/>
 * ORDER BY x</code></blockquote>
 *
 * is valid.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 25, 2003
 */
public class OrderByScope
    extends DelegatingScope
{
    //~ Instance fields --------------------------------------------------------

    private final SqlNodeList orderList;
    private final SqlSelect select;

    //~ Constructors -----------------------------------------------------------

    OrderByScope(
        SqlValidatorScope parent,
        SqlNodeList orderList,
        SqlSelect select)
    {
        super(parent);
        this.orderList = orderList;
        this.select = select;
    }

    //~ Methods ----------------------------------------------------------------

    public SqlNode getNode()
    {
        return orderList;
    }

    public void findAllColumnNames(List<SqlMoniker> result)
    {
        final SqlValidatorNamespace ns = validator.getNamespace(select);
        addColumnNames(ns, result);
    }

    public SqlIdentifier fullyQualify(SqlIdentifier identifier)
    {
        // If it's a simple identifier, look for an alias.
        if (identifier.isSimple()
            && validator.getConformance().isSortByAlias())
        {
            String name = identifier.names[0];
            final SqlValidatorNamespace selectNs =
                validator.getNamespace(select);
            final RelDataType rowType = selectNs.getRowType();
            if (SqlValidatorUtil.lookupField(rowType, name) >= 0) {
                return identifier;
            }
        }
        return super.fullyQualify(identifier);
    }

    public RelDataType resolveColumn(String name, SqlNode ctx)
    {
        final SqlValidatorNamespace selectNs = validator.getNamespace(select);
        final RelDataType rowType = selectNs.getRowType();
        final RelDataType dataType =
            SqlValidatorUtil.lookupFieldType(rowType, name);
        if (dataType != null) {
            return dataType;
        }
        final SqlValidatorScope selectScope = validator.getSelectScope(select);
        return selectScope.resolveColumn(name, ctx);
    }

    public void validateExpr(SqlNode expr)
    {
        SqlNode expanded = validator.expandOrderExpr(select, expr);

        // expression needs to be valid in parent scope too
        parent.validateExpr(expanded);
    }
}

// End OrderByScope.java
