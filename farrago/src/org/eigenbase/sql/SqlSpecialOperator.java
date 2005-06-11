/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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

package org.eigenbase.sql;

import java.util.List;

import org.eigenbase.util.Util;
import org.eigenbase.sql.type.*;


/**
 * Generic operator for nodes with special syntax.
 */
public class SqlSpecialOperator extends SqlOperator
{
    //~ Constructors ----------------------------------------------------------

    public SqlSpecialOperator(
        String name,
        SqlKind kind)
    {
        super(name, kind, 1, true, null, null, null);
    }

    public SqlSpecialOperator(
        String name,
        SqlKind kind,
        int pred)
    {
        super(name, kind, pred, true, null, null, null);
    }

    public SqlSpecialOperator(
        String name,
        SqlKind kind,
        int pred,
        boolean isLeftAssoc,
        SqlReturnTypeInference returnTypeInference,
        SqlOperandTypeInference operandTypeInference,
        SqlOperandTypeChecker operandTypeChecker)
    {
        super(name, kind, pred, isLeftAssoc, returnTypeInference,
            operandTypeInference, operandTypeChecker);
    }

    //~ Methods ---------------------------------------------------------------

    public SqlSyntax getSyntax()
    {
        return SqlSyntax.Special;
    }

    public void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec)
    {
        throw new UnsupportedOperationException(
            "unparse must be implemented by SqlCall subclass");
    }

    /**
     * Reduces a list of operators and arguments according to the rules of
     * precedence and associativity. Returns the ordinal of the node which
     * replaced the expression.
     *
     * <p>The default implementation throws
     * {@link UnsupportedOperationException}.
     * @param ordinal indicating the ordinal of the current operator
     * in the list on which a possible reduction can be made
     */
    public int reduceExpr(
        int ordinal,
        List list)
    {
        throw Util.needToImplement(this);
    }
}


// End SqlExplainOperator.java
