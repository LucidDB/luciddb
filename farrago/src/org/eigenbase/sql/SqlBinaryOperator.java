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

import org.eigenbase.util.Util;
import org.eigenbase.sql.type.UnknownParamInference;
import org.eigenbase.sql.type.ReturnTypeInference;
import org.eigenbase.sql.type.OperandsTypeChecking;


/**
 * <code>SqlBinaryOperator</code> is a binary operator.
 */
public class SqlBinaryOperator extends SqlOperator
{
    //~ Constructors ----------------------------------------------------------

    public SqlBinaryOperator(
        String name,
        SqlKind kind,
        int prec,
        boolean isLeftAssoc,
        ReturnTypeInference typeInference,
        UnknownParamInference paramTypeInference,
        OperandsTypeChecking argTypes)
    {
        super(name, kind, (2 * prec) + (isLeftAssoc ? 0 : 1),
            (2 * prec) + (isLeftAssoc ? 1 : 0), typeInference,
            paramTypeInference, argTypes);
    }

    //~ Methods ---------------------------------------------------------------

    public SqlSyntax getSyntax()
    {
        return SqlSyntax.Binary;
    }

    protected String getSignatureTemplate(final int operandsCount)
    {
        Util.discard(operandsCount);

        //op0 opname op1
        return "{1} {0} {2}";
    }

    boolean needsSpace()
    {
        return !name.equals(".");
    }
}


// End SqlBinaryOperator.java
