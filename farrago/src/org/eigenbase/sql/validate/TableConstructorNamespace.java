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

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;


/**
 * Namespace for a table constructor <code>VALUES (expr, expr, ...)</code>.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 25, 2003
 */
public class TableConstructorNamespace
    extends AbstractNamespace
{
    //~ Instance fields --------------------------------------------------------

    private final SqlCall values;
    private final SqlValidatorScope scope;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a TableConstructorNamespace.
     *
     * @param validator Validator
     * @param values VALUES parse tree node
     * @param scope Scope
     * @param enclosingNode Enclosing node
     */
    TableConstructorNamespace(
        SqlValidatorImpl validator,
        SqlCall values,
        SqlValidatorScope scope,
        SqlNode enclosingNode)
    {
        super(validator, enclosingNode);
        this.values = values;
        this.scope = scope;
    }

    //~ Methods ----------------------------------------------------------------

    protected RelDataType validateImpl()
    {
        return validator.getTableConstructorRowType(values, scope);
    }

    public SqlNode getNode()
    {
        return values;
    }

    /**
     * Returns the scope.
     *
     * @return scope
     */
    public SqlValidatorScope getScope()
    {
        return scope;
    }
}

// End TableConstructorNamespace.java
