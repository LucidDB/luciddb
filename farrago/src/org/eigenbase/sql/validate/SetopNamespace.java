/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2004-2005 The Eigenbase Project
// Copyright (C) 2004-2005 Disruptive Tech
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
package org.eigenbase.sql.validate;

import org.eigenbase.sql.SqlCall;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.SqlKind;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.resource.EigenbaseResource;
import org.eigenbase.util.Util;

/**
 * Namespace based upon a set operation (UNION, INTERSECT, EXCEPT).
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 25, 2003
 */
class SetopNamespace extends AbstractNamespace
{
    private final SqlCall call;

    SetopNamespace(SqlValidatorImpl validator, SqlCall call)
    {
        super(validator);
        this.call = call;
    }

    public SqlNode getNode()
    {
        return call;
    }

    public RelDataType validateImpl()
    {
        switch (call.getKind().getOrdinal()) {
        case SqlKind.UnionORDINAL:
        case SqlKind.IntersectORDINAL:
        case SqlKind.ExceptORDINAL:
            for (int i = 0; i < call.operands.length; i++) {
                SqlNode operand = call.operands[i];
                if (!operand.getKind().isA(SqlKind.Query)) {
                    throw validator.newValidationError(operand,
                        EigenbaseResource.instance().newNeedQueryOp(
                            operand.toString()));
                }
                validator.validateQuery(operand);
            }
            final SqlValidatorScope scope = (SqlValidatorScope) validator.scopes.get(call);
            return call.operator.getType(validator, scope, call);
        default:
            throw Util.newInternal("Not a query: " + call.getKind());
        }
    }
}


// End SetopNamespace.java
