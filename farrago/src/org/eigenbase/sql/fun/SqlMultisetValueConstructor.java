/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2002 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2003 John V. Sichi
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
import org.eigenbase.sql.validate.*;


/**
 * Definition of the SQL:2003 standard MULTISET constructor, <code>MULTISET
 * [&lt;expr&gt;, ...]</code>.
 *
 * @author Wael Chatila
 * @version $Id$
 * @see SqlMultisetQueryConstructor
 * @since Oct 17, 2004
 */
public class SqlMultisetValueConstructor
    extends SqlSpecialOperator
{
    //~ Constructors -----------------------------------------------------------

    public SqlMultisetValueConstructor()
    {
        super(
            "MULTISET",
            SqlKind.MULTISET_VALUE_CONSTRUCTOR,
            MaxPrec,
            false,
            SqlTypeStrategies.rtiFirstArgType,
            null,
            SqlTypeStrategies.otcVariadic);
    }

    //~ Methods ----------------------------------------------------------------

    public RelDataType inferReturnType(
        SqlOperatorBinding opBinding)
    {
        RelDataType type =
            getComponentType(
                opBinding.getTypeFactory(),
                opBinding.collectOperandTypes());
        if (null == type) {
            return null;
        }
        return SqlTypeUtil.createMultisetType(
            opBinding.getTypeFactory(),
            type,
            false);
    }

    private RelDataType getComponentType(
        RelDataTypeFactory typeFactory,
        RelDataType [] argTypes)
    {
        return typeFactory.leastRestrictive(argTypes);
    }

    public boolean checkOperandTypes(
        SqlCallBinding callBinding,
        boolean throwOnFailure)
    {
        final RelDataType [] argTypes =
            SqlTypeUtil.deriveAndCollectTypes(
                callBinding.getValidator(),
                callBinding.getScope(),
                callBinding.getCall().operands);
        final RelDataType componentType =
            getComponentType(
                callBinding.getTypeFactory(),
                argTypes);
        if (null == componentType) {
            if (throwOnFailure) {
                throw callBinding.newValidationError(
                    EigenbaseResource.instance().NeedSameTypeParameter.ex());
            }
            return false;
        }
        return true;
    }

    public void unparse(
        SqlWriter writer,
        SqlNode [] operands,
        int leftPrec,
        int rightPrec)
    {
        writer.keyword("MULTISET");
        final SqlWriter.Frame frame = writer.startList("[", "]");
        for (int i = 0; i < operands.length; i++) {
            writer.sep(",");
            operands[i].unparse(writer, leftPrec, rightPrec);
        }
        writer.endList(frame);
    }
}

// End SqlMultisetValueConstructor.java
