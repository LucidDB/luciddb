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
package org.eigenbase.sql.type;

import org.eigenbase.resource.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.reltype.*;
import org.eigenbase.util.*;

import java.util.*;

/**
 * Parameter type-checking strategy
 * type must be a literal (whether null is allowede is determined by
 * the constructor).
 * <code>CAST(NULL as ...)</code> is considered to be a NULL literal but not
 * <code>CAST(CAST(NULL as ...) AS ...)</code>
 *
 * @author Wael Chatila
 * @version $Id$
 */
public class LiteralOperandTypeChecker implements SqlSingleOperandTypeChecker
{
    private boolean allowNull;
    
    public LiteralOperandTypeChecker(boolean allowNull)
    {
        this.allowNull = allowNull;
    }
    
    public boolean checkSingleOperandType(
        SqlCallBinding callBinding,
        SqlNode node,
        int iFormalOperand,
        boolean throwOnFailure)
    {
        Util.discard(iFormalOperand);

        if (SqlUtil.isNullLiteral(node, true)) {
            if (allowNull) {
                return true;
            }
            if (throwOnFailure) {
                throw EigenbaseResource.instance().newArgumentMustNotBeNull(
                    callBinding.getOperator().getName());
            }
            return false;
        }
        if (!SqlUtil.isLiteral(node)) {
            if (throwOnFailure) {
                throw EigenbaseResource.instance().newArgumentMustBeLiteral(
                    callBinding.getOperator().getName());
            }
            return false;
        }

        return true;
    }

    public boolean checkOperandTypes(
        SqlCallBinding callBinding,
        boolean throwOnFailure)
    {
        return checkSingleOperandType(
            callBinding,
            callBinding.getCall().operands[0], 0, throwOnFailure);
    }

    public SqlOperandCountRange getOperandCountRange()
    {
        return SqlOperandCountRange.One;
    }

    public String getAllowedSignatures(SqlOperator op, String opName)
    {
        return "<LITERAL>";
    }
}

// End LiteralOperandTypeChecker.java
