/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006 The Eigenbase Project
// Copyright (C) 2006 SQLstream, Inc.
// Copyright (C) 2006 Dynamo BI Corporation
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
import org.eigenbase.resource.*;
import org.eigenbase.sql.*;
import org.eigenbase.util.Pair;


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

    private final boolean isPassthrough;

    //~ Constructors -----------------------------------------------------------

    public TableFunctionReturnTypeInference(
        RelDataType unexpandedOutputType,
        List<String> paramNames,
        boolean isPassthrough)
    {
        super(unexpandedOutputType);
        this.paramNames = paramNames;
        this.isPassthrough = isPassthrough;
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
        List<Map.Entry<String, RelDataType>> expandedFields =
            new ArrayList<Map.Entry<String, RelDataType>>();
        for (RelDataTypeField field : unexpandedOutputType.getFieldList()) {
            RelDataType fieldType = field.getType();
            String fieldName = field.getName();
            if (fieldType.getSqlTypeName() != SqlTypeName.CURSOR) {
                expandedFields.add(field);
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
            boolean isRowOp = false;
            List<String> columnNames = new ArrayList<String>();
            RelDataType cursorType = opBinding.getCursorOperand(paramOrdinal);
            if (cursorType == null) {
                isRowOp = true;
                String parentCursorName =
                    opBinding.getColumnListParamInfo(
                        paramOrdinal,
                        fieldName,
                        columnNames);
                assert (parentCursorName != null);
                paramOrdinal = -1;
                iCursor = 0;
                for (int i = 0; i < paramNames.size(); ++i) {
                    if (paramNames.get(i).equals(parentCursorName)) {
                        paramOrdinal = i;
                        break;
                    }
                    cursorType = opBinding.getCursorOperand(i);
                    if (cursorType != null) {
                        ++iCursor;
                    }
                }
                cursorType = opBinding.getCursorOperand(paramOrdinal);
                assert (cursorType != null);
            }

            // And expand. Function output is always nullable... except system
            // fields.
            int iInputColumn;
            if (isRowOp) {
                for (String columnName : columnNames) {
                    iInputColumn = -1;
                    RelDataTypeField cursorField = null;
                    for (RelDataTypeField cField : cursorType.getFieldList()) {
                        ++iInputColumn;
                        if (cField.getName().equals(columnName)) {
                            cursorField = cField;
                            break;
                        }
                    }
                    addOutputColumn(
                        expandedFields,
                        iInputColumn,
                        iCursor,
                        opBinding,
                        cursorField);
                }
            } else {
                iInputColumn = -1;
                for (RelDataTypeField cursorField : cursorType.getFieldList()) {
                    ++iInputColumn;
                    addOutputColumn(
                        expandedFields,
                        iInputColumn,
                        iCursor,
                        opBinding,
                        cursorField);
                }
            }
        }
        return opBinding.getTypeFactory().createStructType(expandedFields);
    }

    private void addOutputColumn(
        List<Map.Entry<String, RelDataType>> expandedFields,
        int iInputColumn,
        int iCursor,
        SqlOperatorBinding opBinding,
        RelDataTypeField cursorField)
    {
        RelColumnMapping columnMapping = new RelColumnMapping();
        columnMapping.iOutputColumn = expandedFields.size();
        columnMapping.iInputColumn = iInputColumn;
        columnMapping.iInputRel = iCursor;

        columnMapping.isDerived = isPassthrough ? false : true;
        columnMappings.add(columnMapping);

        // As a special case, system fields are implicitly NOT NULL.
        // A badly behaved UDX can still provide NULL values, so the
        // system must ensure that each generated system field has a
        // reasonable value.
        boolean nullable = true;
        if (opBinding instanceof SqlCallBinding) {
            SqlCallBinding sqlCallBinding = (SqlCallBinding) opBinding;
            if (sqlCallBinding.getValidator().isSystemField(cursorField)) {
                nullable = false;
            }
        }
        RelDataType nullableType =
            opBinding.getTypeFactory().createTypeWithNullability(
                cursorField.getType(),
                nullable);

        // Make sure there are no duplicates in the output column names
        for (Map.Entry<String, RelDataType> field : expandedFields) {
            if (field.getKey().equals(cursorField.getName())) {
                throw opBinding.newError(
                    EigenbaseResource.instance().DuplicateColumnName.ex(
                        cursorField.getName()));
            }
        }
        expandedFields.add(Pair.of(cursorField.getName(), nullableType));
    }
}

// End TableFunctionReturnTypeInference.java
