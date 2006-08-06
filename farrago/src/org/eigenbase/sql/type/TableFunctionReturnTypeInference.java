/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
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

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;

import java.util.*;

/**
 * TableFunctionReturnTypeInference implements rules for deriving
 * table function output row types by expanding references to cursor
 * parameters.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class TableFunctionReturnTypeInference
    extends ExplicitReturnTypeInference
{
    private final List<String> paramNames;
    
    public TableFunctionReturnTypeInference(
        RelDataType unexpandedOutputType,
        List<String> paramNames)
    {
        super(unexpandedOutputType);
        this.paramNames = paramNames;
    }
    
    public RelDataType inferReturnType(
        SqlOperatorBinding opBinding)
    {
        RelDataType unexpandedOutputType = getExplicitType();
        List<RelDataType> expandedOutputTypes = new ArrayList<RelDataType>();
        List<String> expandedFieldNames = new ArrayList<String>();
        for (RelDataTypeField field : unexpandedOutputType.getFieldList()) {
            RelDataType fieldType = field.getType();
            String fieldName = field.getName();
            if (fieldType.getSqlTypeName() != SqlTypeName.Cursor) {
                expandedOutputTypes.add(fieldType);
                expandedFieldNames.add(fieldName);
                continue;
            }
            // Look up position of cursor parameter with same name as output
            // field.
            int paramOrdinal = paramNames.indexOf(fieldName);
            assert(paramOrdinal != -1);
            // Translate to actual argument type.
            RelDataType cursorType = opBinding.getCursorOperand(paramOrdinal);
            // And expand (function output is always nullable).
            for (RelDataTypeField cursorField : cursorType.getFieldList()) {
                RelDataType nullableType =
                    opBinding.getTypeFactory().createTypeWithNullability(
                        cursorField.getType(),
                        true);
                expandedOutputTypes.add(nullableType);
                expandedFieldNames.add(cursorField.getName());
            }
        }
        return opBinding.getTypeFactory().createStructType(
            expandedOutputTypes,
            expandedFieldNames);
    }
}

// End TableFunctionReturnTypeInference.java
