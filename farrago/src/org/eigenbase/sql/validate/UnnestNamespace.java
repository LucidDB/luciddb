/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2004-2007 The Eigenbase Project
// Copyright (C) 2004-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
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
package org.eigenbase.sql.validate;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.type.*;


/**
 * Namespace for UNNEST.
 *
 * @author wael
 * @version $Id$
 * @since Mar 25, 2003
 */
class UnnestNamespace
    extends AbstractNamespace
{
    //~ Instance fields --------------------------------------------------------

    private final SqlCall unnest;
    private final SqlValidatorScope scope;

    //~ Constructors -----------------------------------------------------------

    UnnestNamespace(
        SqlValidatorImpl validator,
        SqlCall unnest,
        SqlValidatorScope scope)
    {
        super(validator);
        assert scope != null;
        assert unnest.getOperator() == SqlStdOperatorTable.unnestOperator;
        this.unnest = unnest;
        this.scope = scope;
    }

    //~ Methods ----------------------------------------------------------------

    protected RelDataType validateImpl()
    {
        // Validate the call and its arguments, and infer the return type.
        validator.validateCall(unnest, scope);
        RelDataType type =
            unnest.getOperator().validateOperands(validator, scope, unnest);

        if (type.isStruct()) {
            return type;
        }
        return validator.getTypeFactory().createStructType(
            new RelDataType[] { type },
            new String[] { validator.deriveAlias(unnest, 0) });
    }

    /**
     * Returns the type of the argument to UNNEST.
     */
    private RelDataType inferReturnType()
    {
        final SqlNode operand = unnest.getOperands()[0];
        RelDataType type = validator.getValidatedNodeType(operand);

        // If sub-query, pick out first column.
        // TODO: Handle this using usual sub-select validation.
        if (type.isStruct()) {
            type = type.getFields()[0].getType();
        }
        MultisetSqlType t = (MultisetSqlType) type;
        return t.getComponentType();
    }

    public SqlNode getNode()
    {
        return unnest;
    }
}

// End UnnestNamespace.java
