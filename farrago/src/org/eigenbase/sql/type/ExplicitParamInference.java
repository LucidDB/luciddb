/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2004-2005 John V. Sichi
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

import org.eigenbase.sql.*;
import org.eigenbase.sql.validate.SqlValidatorScope;
import org.eigenbase.sql.validate.SqlValidator;
import org.eigenbase.reltype.*;

/**
 * ExplicitParamInferences implements {@link UnknownParamInference}
 * by explicity supplying a type for each parameter.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class ExplicitParamInference implements UnknownParamInference
{
    private final RelDataType [] paramTypes;

    public ExplicitParamInference(RelDataType [] paramTypes)
    {
        this.paramTypes = paramTypes;
    }

    public void inferOperandTypes(
        SqlValidator validator,
        SqlValidatorScope scope,
        SqlCall call,
        RelDataType returnType,
        RelDataType [] operandTypes)
    {
        System.arraycopy(paramTypes, 0, operandTypes, 0, paramTypes.length);
    }
}

// End ExplicitParamInference.java
