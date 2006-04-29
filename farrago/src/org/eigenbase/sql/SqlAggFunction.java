/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
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

package org.eigenbase.sql;

import org.eigenbase.sql.type.*;
import org.eigenbase.sql.validate.SqlValidator;
import org.eigenbase.sql.validate.SqlValidatorScope;
import org.eigenbase.sql.validate.AggregatingScope;
import org.eigenbase.rel.Aggregation;
import org.eigenbase.reltype.RelDataType;
import openjava.mop.OJClass;


/**
 * Operator which aggregates sets of values into a result.
 *
 * @author jhyde
 * @version $Id$
 */
public abstract class SqlAggFunction extends SqlFunction implements Aggregation
{
    public SqlAggFunction(
        String name,
        SqlKind kind,
        SqlReturnTypeInference returnTypeInference,
        SqlOperandTypeInference operandTypeInference,
        SqlOperandTypeChecker operandTypeChecker,
        SqlFunctionCategory funcType)
    {
        super(
            name, kind, returnTypeInference,
            operandTypeInference, operandTypeChecker,
            funcType);
    }

    public OJClass [] getStartParameterTypes()
    {
        return new OJClass[0];
    }

    public boolean isQuantifierAllowed()
    {
        return true;
    }

    public RelDataType deriveType(
        SqlValidator validator, SqlValidatorScope scope, SqlCall call)
    {
        if (scope instanceof AggregatingScope) {
            AggregatingScope aggregatingScope = (AggregatingScope) scope;
            scope = aggregatingScope.getScopeAboveAggregation();
        }
        return super.deriveType(validator, scope, call);
    }
}

// End SqlAggFunction.java
