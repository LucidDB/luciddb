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

import org.eigenbase.sql.type.OperandsTypeChecking;
import org.eigenbase.sql.type.ReturnTypeInference;
import org.eigenbase.sql.type.UnknownParamInference;
import org.eigenbase.rel.Aggregation;
import openjava.mop.OJClass;


/**
 * Operator which aggregates sets of values into a result.
 */
public abstract class SqlAggFunction extends SqlFunction implements Aggregation
{
    public SqlAggFunction(
        String name,
        SqlKind kind,
        ReturnTypeInference typeInference,
        UnknownParamInference paramTypeInference,
        OperandsTypeChecking paramTypes,
        SqlFunctionCategory funcType)
    {
        super(name, kind, typeInference, paramTypeInference, paramTypes,
            funcType);
    }

    public String getName()
    {
        return name;
    }

    public OJClass [] getStartParameterTypes()
    {
        return new OJClass[0];
    }
}

// End SqlAggFunction.java
