/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
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
package org.eigenbase.sql;

import org.eigenbase.reltype.*;
import org.eigenbase.resource.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.util.*;
import org.eigenbase.sql.validate.*;


/**
 * The <code>AS</code> operator associates an expression with an alias.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class SqlAsOperator
    extends SqlBinaryOperator
{
    //~ Constructors -----------------------------------------------------------

    public SqlAsOperator()
    {
        super(
            "AS",
            SqlKind.As,
            20,
            true,
            SqlTypeStrategies.rtiFirstArgType,
            SqlTypeStrategies.otiReturnType,
            SqlTypeStrategies.otcAnyX2);
    }

    //~ Methods ----------------------------------------------------------------

    public void validateCall(
        SqlCall call,
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlValidatorScope operandScope)
    {
        // The base method validates all operands. We override because
        // we don't want to validate the identifier.
        final SqlNode [] operands = call.operands;
        assert operands.length == 2;
        assert operands[1] instanceof SqlIdentifier;
        operands[0].validateExpr(validator, scope);
        SqlIdentifier id = (SqlIdentifier) operands[1];
        if (!id.isSimple()) {
            throw validator.newValidationError(
                id,
                EigenbaseResource.instance().AliasMustBeSimpleIdentifier.ex());
        }
    }

    public <R> void acceptCall(
        SqlVisitor<R> visitor,
        SqlCall call,
        boolean onlyExpressions,
        SqlBasicVisitor.ArgHandler<R> argHandler)
    {
        if (onlyExpressions) {
            // Do not visit operands[1] -- it is not an expression.
            argHandler.visitChild(visitor, call, 0, call.operands[0]);
        } else {
            super.acceptCall(visitor, call, onlyExpressions, argHandler);
        }
    }

    public RelDataType deriveType(
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlCall call)
    {
        // special case for AS:  never try to derive type for alias
        RelDataType nodeType = validator.deriveType(scope, call.operands[0]);
        assert nodeType != null;
        RelDataType type = validateOperands(validator, scope, call);
        return type;
    }

    public SqlMonotonicity getMonotonicity(
        SqlCall call,
        SqlValidatorScope scope)
    {
        return call.operands[0].getMonotonicity(scope);
    }
}

// End SqlAsOperator.java
