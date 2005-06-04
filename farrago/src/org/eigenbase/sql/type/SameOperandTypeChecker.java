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
 * Parameter type-checking strategy where all operand types must be
 * either the same or null.
 *
 * @author Wael Chatila
 * @version $Id$
 */
public class SameOperandTypeChecker extends ExplicitOperandTypeChecker
{
    private final int nOperands;

    public SameOperandTypeChecker(
        SqlTypeName [][] explicitTypes)
    {
        super(explicitTypes);
        nOperands = explicitTypes.length;
    }
    
    public boolean checkCall(
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlCall call, boolean throwOnFailure)
    {
        RelDataType [] types = new RelDataType[nOperands];
        for (int i = 0; i < nOperands; ++i) {
            types[i] =
                validator.deriveType(scope, call.operands[i]);
        }
        RelDataType nullType =
            validator.getTypeFactory().createSqlType(SqlTypeName.Null);
        int prev = -1;
        for (int i = 0; i < nOperands; ++i) {
            if (types[i] == nullType) {
                continue;
            }
            if (prev == -1) {
                prev = i;
            } else {
                RelDataTypeFamily family1 = types[i].getFamily();
                RelDataTypeFamily family2 = types[prev].getFamily();
                // REVIEW jvs 2-June-2005:  This is needed to keep
                // the Saffron type system happy.
                if (types[i].getSqlTypeName() != null) {
                    family1 = types[i].getSqlTypeName().getFamily();
                }
                if (types[prev].getSqlTypeName() != null) {
                    family2 = types[prev].getSqlTypeName().getFamily();
                }
                if (family1 == family2) {
                    continue;
                }
                if (!throwOnFailure) {
                    return false;
                }
                throw validator.newValidationError(
                    call,
                    EigenbaseResource.instance().newNeedSameTypeParameter());
            }
        }
        return true;
    }
}

// End SameOperandTypeChecker.java
