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
package org.eigenbase.sql.fun;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.sql.util.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.test.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;

import java.util.*;

/**
 * Base class for binary operators such as addition, subtraction, and
 * multiplication which are monotonic for the patterns <code>m op c</code>
 * and <code>c op m</code> where m is any monotonic expression and c
 * is a constant.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class SqlMonotonicBinaryOperator extends SqlBinaryOperator
{
    public SqlMonotonicBinaryOperator(
        String name,
        SqlKind kind,
        int prec,
        boolean isLeftAssoc,
        SqlReturnTypeInference typeInference,
        SqlOperandTypeInference paramTypeInference,
        SqlOperandTypeChecker argTypes)
    {
        super(
            name, kind, prec, isLeftAssoc, typeInference, paramTypeInference,
            argTypes);
    }
    
    public boolean isMonotonic(SqlCall call, SqlValidatorScope scope)
    {
        SqlValidator val = scope.getValidator();
        
        // First check for (m op c)
        if (val.isConstant(call.operands[1])) {
            SqlNode node = (SqlNode)call.operands[0];
            return scope.isMonotonic(node);
        }
        
        // Check the converse (c op m)
        if (val.isConstant(call.operands[0])) {
            SqlNode node = (SqlNode)call.operands[1];
            return scope.isMonotonic(node);
        }

        return super.isMonotonic(call, scope);
    }
}

// End SqlMonotonicBinaryOperator.java
