/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
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
import org.eigenbase.resource.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;


/**
 * Multiset MEMBER OF. Checks to see if a element belongs to a multiset.<br>
 * Example:<br>
 * <code>'green' MEMBER OF MULTISET['red','almost green','blue']</code> returns
 * <code>false</code>.
 *
 * @author Wael Chatila
 * @version $Id$
 */
public class SqlMultisetMemberOfOperator
    extends SqlBinaryOperator
{
    //~ Constructors -----------------------------------------------------------

    public SqlMultisetMemberOfOperator()
    {
        //TODO check if precedence is correct
        super(
            "MEMBER OF",
            SqlKind.Other,
            30,
            true,
            SqlTypeStrategies.rtiNullableBoolean,
            null,
            null);
    }

    //~ Methods ----------------------------------------------------------------

    public boolean checkOperandTypes(
        SqlCallBinding callBinding,
        boolean throwOnFailure)
    {
        if (!SqlTypeStrategies.otcMultiset.checkSingleOperandType(
                callBinding,
                callBinding.getCall().operands[1],
                0,
                throwOnFailure))
        {
            return false;
        }

        MultisetSqlType mt =
            (MultisetSqlType) callBinding.getValidator().deriveType(
                callBinding.getScope(),
                callBinding.getCall().operands[1]);

        RelDataType t0 =
            callBinding.getValidator().deriveType(
                callBinding.getScope(),
                callBinding.getCall().operands[0]);
        RelDataType t1 = mt.getComponentType();

        if (t0.getFamily() != t1.getFamily()) {
            if (throwOnFailure) {
                throw callBinding.newValidationError(
                    EigenbaseResource.instance().TypeNotComparableNear.ex(
                        t0.toString(),
                        t1.toString()));
            }
            return false;
        }
        return true;
    }

    public SqlOperandCountRange getOperandCountRange()
    {
        return SqlOperandCountRange.Two;
    }
}

// End SqlMultisetMemberOfOperator.java
