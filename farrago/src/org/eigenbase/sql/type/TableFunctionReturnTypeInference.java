/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2007 The Eigenbase Project
// Copyright (C) 2006-2007 Disruptive Tech
// Copyright (C) 2006-2007 LucidEra, Inc.
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

import java.util.*;

import org.eigenbase.rel.metadata.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;


/**
 * TableFunctionReturnTypeInference implements rules for deriving table function
 * output row types by expanding references to cursor parameters.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class TableFunctionReturnTypeInference
    extends ExplicitReturnTypeInference
{
    //~ Instance fields --------------------------------------------------------

    private final List<String> paramNames;

    private Set<RelColumnMapping> columnMappings;

    //~ Constructors -----------------------------------------------------------

    public TableFunctionReturnTypeInference(
        RelDataType unexpandedOutputType,
        List<String> paramNames)
    {
        super(unexpandedOutputType);
        this.paramNames = paramNames;
    }

    //~ Methods ----------------------------------------------------------------

    public Set<RelColumnMapping> getColumnMappings()
    {
        return columnMappings;
    }

    public RelDataType inferReturnType(
        SqlOperatorBinding opBinding)
    {
        columnMappings = new HashSet<RelColumnMapping>();
        RelDataType unexpandedOutputType = getExplicitType();
        List<RelDataType> expandedOutputTypes = new ArrayList<RelDataType>();
        List<String> expandedFieldNames = new ArrayList<String>();
        for (RelDataTypeField field : unexpandedOutputType.getFieldList()) {
            RelDataType fieldType = field.getType();
            String fieldName = field.getName();
            if (fieldType.getSqlTypeName() != SqlTypeName.CURSOR) {
                expandedOutputTypes.add(fieldType);
                expandedFieldNames.add(fieldName);
                continue;
            }

            // Look up position of cursor parameter with same name as output
            // field, also counting how many cursors appear before it
            // (need this for correspondence with RelNode child position).
            int paramOrdinal = -1;
            int iCursor = 0;
            for (int i = 0; i < paramNames.size(); ++i) {
                if (paramNames.get(i).equals(fieldName)) {
                    paramOrdinal = i;
                    break;
                }
                RelDataType cursorType = opBinding.getCursorOperand(i);
                if (cursorType != null) {
                    ++iCursor;
                }
            }
            assert (paramOrdinal != -1);

            // Translate to actual argument type.
            RelDataType cursorType = opBinding.getCursorOperand(paramOrdinal);

            // And expand. Function output is always nullable... except system
            // fields.
            int iInputColumn = -1;
            for (RelDataTypeField cursorField : cursorType.getFieldList()) {
                ++iInputColumn;
                RelColumnMapping columnMapping = new RelColumnMapping();
                columnMapping.iOutputColumn = expandedFieldNames.size();
                columnMapping.iInputColumn = iInputColumn;
                columnMapping.iInputRel = iCursor;

                // we don't have any metadata on transformation effect,
                // so assume the worst
                columnMapping.isDerived = true;
                columnMappings.add(columnMapping);

                // As a special case, system fields are implicitly NOT NULL.
                // A badly behaved UDX can still provide NULL values, so the
                // system must ensure that each generated system field has a
                // reasonable value.
                boolean nullable = true;
                if (opBinding instanceof SqlCallBinding) {
                    SqlCallBinding sqlCallBinding = (SqlCallBinding) opBinding;
                    if (sqlCallBinding.getValidator().isSystemField(
                        cursorField)) {
                        nullable = false;
                    }
                }
                RelDataType nullableType =
                    opBinding.getTypeFactory().createTypeWithNullability(
                        cursorField.getType(),
                        nullable);
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
