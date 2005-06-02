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
 * The <code>AS</code> operator associates an expression with an alias.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class SqlAsOperator extends SqlBinaryOperator
{
    public SqlAsOperator()
    {
        super(
            "AS",
            SqlKind.As,
            10,
            true,
            ReturnTypeInferenceImpl.useFirstArgType,
            UnknownParamInference.useReturnType,
            OperandsTypeChecking.typeAnyAny);
    }
    
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
        operands[0].validate(validator, scope);
        SqlIdentifier id = (SqlIdentifier) operands[1];
        if (!id.isSimple()) {
            throw validator.newValidationError(id,
                EigenbaseResource.instance()
                .newAliasMustBeSimpleIdentifier());
        }
    }

    public void acceptCall(SqlVisitor visitor, SqlCall call) {
        // Do not visit operands[1] -- it is not an expression.
        visitor.visitChild(call, 0, call.operands[0]);
    }
}

// End SqlAsOperator.java
