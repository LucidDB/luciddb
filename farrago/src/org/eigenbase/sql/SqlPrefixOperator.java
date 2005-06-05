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
import org.eigenbase.sql.type.*;


/**
 * A unary operator.
 */
public class SqlPrefixOperator extends SqlOperator
{
    //~ Constructors ----------------------------------------------------------

    public SqlPrefixOperator(
        String name,
        SqlKind kind,
        int precedence,
        SqlReturnTypeInference returnTypeInference,
        SqlOperandTypeInference operandTypeInference,
        SqlOperandTypeChecker operandTypeChecker)
    {
        super(name, kind, 0, precedence * 2, returnTypeInference,
            operandTypeInference, operandTypeChecker);
    }

    //~ Methods ---------------------------------------------------------------

    public SqlSyntax getSyntax()
    {
        return SqlSyntax.Prefix;
    }

    protected String getSignatureTemplate(final int operandsCount)
    {
        Util.discard(operandsCount);
        return "{0}{1}";
    }
}


// End SqlPrefixOperator.java
