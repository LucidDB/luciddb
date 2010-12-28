/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006 The Eigenbase Project
// Copyright (C) 2006 SQLstream, Inc.
// Copyright (C) 2006 Dynamo BI Corporation
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
import org.eigenbase.sql.util.*;


/**
 * Refinement to {@link SqlShuttle} which maintains a stack of scopes.
 *
 * <p>Derived class should override {@link #visitScoped(SqlCall)} rather than
 * {@link #visit(SqlCall)}.
 *
 * @author jhyde
 * @version $Id$
 * @since Jun 10, 2006
 */
public abstract class SqlScopedShuttle
    extends SqlShuttle
{
    //~ Instance fields --------------------------------------------------------

    private final Stack<SqlValidatorScope> scopes =
        new Stack<SqlValidatorScope>();

    //~ Constructors -----------------------------------------------------------

    protected SqlScopedShuttle(SqlValidatorScope initialScope)
    {
        scopes.push(initialScope);
    }

    //~ Methods ----------------------------------------------------------------

    public final SqlNode visit(SqlCall call)
    {
        SqlValidatorScope oldScope = scopes.peek();
        SqlValidatorScope newScope = oldScope.getOperandScope(call);
        scopes.push(newScope);
        SqlNode result = visitScoped(call);
        scopes.pop();
        return result;
    }

    /**
     * Visits an operator call. If the call has entered a new scope, the base
     * class will have already modified the scope.
     */
    protected SqlNode visitScoped(SqlCall call)
    {
        return super.visit(call);
    }

    /**
     * Returns the current scope.
     */
    protected SqlValidatorScope getScope()
    {
        return scopes.peek();
    }
}

// End SqlScopedShuttle.java
