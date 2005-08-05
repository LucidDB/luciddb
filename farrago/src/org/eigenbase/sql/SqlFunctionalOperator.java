/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
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
package org.eigenbase.sql;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.sql.util.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.test.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;
import org.eigenbase.resource.*;

import java.util.*;

/**
 * SqlFunctionalOperator is a base class for special operators which
 * use functional syntax.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class SqlFunctionalOperator extends SqlSpecialOperator
{
    public SqlFunctionalOperator(
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
    
    public void unparse(
        SqlWriter writer,
        SqlNode[] operands,
        int leftPrec,
        int rightPrec)
    {
        SqlUtil.unparseFunctionSyntax(this, writer, operands, true, null);
    }
}

// End SqlFunctionalOperator.java
