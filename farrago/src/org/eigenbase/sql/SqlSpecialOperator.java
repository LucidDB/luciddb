/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// (C) Copyright 2003-2004 John V. Sichi
// You must accept the terms in LICENSE.html to use this software.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

package org.eigenbase.sql;

import org.eigenbase.util.Util;

import java.util.List;

/**
 * Generic operator for nodes with special syntax.
 */
public abstract class SqlSpecialOperator extends SqlOperator
{
    //~ Constructors ----------------------------------------------------------

    public SqlSpecialOperator(String name,SqlKind kind)
    {
        super(name,kind,1,true, null,null, null);
    }

    public SqlSpecialOperator(String name,SqlKind kind, int pred)
    {
        super(name,kind,pred,true, null,null, null);
    }

    public SqlSpecialOperator(String name, SqlKind kind, int pred,
            boolean isLeftAssoc, TypeInference typeInference,
            ParamTypeInference paramTypeInference,
            AllowedArgInference argTypeInference)
    {
        super(name,kind,pred,isLeftAssoc,
              typeInference,paramTypeInference, argTypeInference);
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
     */
    public int reduceExpr(int ordinal, List list) {
        throw Util.needToImplement(this);
    }

}


// End SqlExplainOperator.java
