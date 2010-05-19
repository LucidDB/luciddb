/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2004 SQLstream, Inc.
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
package org.eigenbase.sql;

import openjava.mop.*;

import org.eigenbase.rel.*;
import org.eigenbase.resource.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.validate.*;


/**
 * Abstract base class for the definition of an aggregate function: an operator
 * which aggregates sets of values into a result.
 *
 * @author jhyde
 * @version $Id$
 */
public abstract class SqlAggFunction
    extends SqlFunction
    implements Aggregation
{
    //~ Constructors -----------------------------------------------------------

    public SqlAggFunction(
        String name,
        SqlKind kind,
        SqlReturnTypeInference returnTypeInference,
        SqlOperandTypeInference operandTypeInference,
        SqlOperandTypeChecker operandTypeChecker,
        SqlFunctionCategory funcType)
    {
        super(
            name,
            kind,
            returnTypeInference,
            operandTypeInference,
            operandTypeChecker,
            funcType);
    }

    //~ Methods ----------------------------------------------------------------

    public OJClass [] getStartParameterTypes()
    {
        return new OJClass[0];
    }

    public boolean isQuantifierAllowed()
    {
        return true;
    }

    // override SqlFunction
    public void validateCall(
        SqlCall call,
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlValidatorScope operandScope)
    {
        super.validateCall(call, validator, scope, operandScope);
        validator.validateAggregateParams(call, scope);
    }
}

// End SqlAggFunction.java
