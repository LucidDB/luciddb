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
package org.eigenbase.sql.type;

import org.eigenbase.reltype.*;
import org.eigenbase.resource.*;
import org.eigenbase.sql.*;


/**
 * Parameter type-checking strategy types must be [nullable] Multiset,
 * [nullable] Multiset and the two types must have the same element type
 *
 * @author Wael Chatila
 * @version $Id$
 * @see MultisetSqlType#getComponentType
 */
public class MultisetOperandTypeChecker
    implements SqlOperandTypeChecker
{
    //~ Methods ----------------------------------------------------------------

    public boolean checkOperandTypes(
        SqlCallBinding callBinding,
        boolean throwOnFailure)
    {
        SqlCall call = callBinding.getCall();
        SqlNode op0 = call.operands[0];
        if (!SqlTypeStrategies.otcMultiset.checkSingleOperandType(
                callBinding,
                op0,
                0,
                throwOnFailure))
        {
            return false;
        }

        SqlNode op1 = call.operands[1];
        if (!SqlTypeStrategies.otcMultiset.checkSingleOperandType(
                callBinding,
                op1,
                0,
                throwOnFailure))
        {
            return false;
        }

        RelDataType [] argTypes = new RelDataType[2];
        argTypes[0] =
            callBinding.getValidator().deriveType(
                callBinding.getScope(),
                op0).getComponentType();
        argTypes[1] =
            callBinding.getValidator().deriveType(
                callBinding.getScope(),
                op1).getComponentType();

        //TODO this wont work if element types are of ROW types and there is a
        //mismatch.
        RelDataType biggest =
            callBinding.getTypeFactory().leastRestrictive(
                argTypes);
        if (null == biggest) {
            if (throwOnFailure) {
                throw callBinding.newError(
                    EigenbaseResource.instance().TypeNotComparable.ex(
                        call.operands[0].getParserPosition().toString(),
                        call.operands[1].getParserPosition().toString()));
            }

            return false;
        }
        return true;
    }

    public SqlOperandCountRange getOperandCountRange()
    {
        return SqlOperandCountRange.Two;
    }

    public String getAllowedSignatures(SqlOperator op, String opName)
    {
        return "<MULTISET> " + opName + " <MULTISET>";
    }
}

// End MultisetOperandTypeChecker.java
