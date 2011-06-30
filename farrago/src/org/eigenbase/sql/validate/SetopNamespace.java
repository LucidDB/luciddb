/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2004 The Eigenbase Project
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
package org.eigenbase.sql.validate;

import java.util.*;

import org.eigenbase.reltype.*;
import org.eigenbase.resource.*;
import org.eigenbase.sql.*;
import org.eigenbase.util.*;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.util.mapping.*;


/**
 * Namespace based upon a set operation (UNION, INTERSECT, EXCEPT).
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 25, 2003
 */
public class SetopNamespace
    extends AbstractNamespace
{
    //~ Instance fields --------------------------------------------------------

    private final SqlCall call;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a <code>SetopNamespace</code>.
     *
     * @param validator Validator
     * @param call Call to set operator
     * @param enclosingNode Enclosing node
     */
    protected SetopNamespace(
        SqlValidatorImpl validator,
        SqlCall call,
        SqlNode enclosingNode)
    {
        super(validator, enclosingNode);
        this.call = call;
    }

    //~ Methods ----------------------------------------------------------------

    public SqlNode getNode()
    {
        return call;
    }

    public RelDataType validateImpl()
    {
        switch (call.getKind()) {
        case UNION:
        case INTERSECT:
        case EXCEPT:
            final SqlValidatorScope scope = validator.scopes.get(call);
            for (SqlNode operand : call.operands) {
                if (!(operand.isA(SqlKind.QUERY))) {
                    throw validator.newValidationError(
                        operand,
                        EigenbaseResource.instance().NeedQueryOp.ex(
                            operand.toString()));
                }
                validator.validateQuery(operand, scope);
            }
            final RelDataType rowType =
                call.getOperator().validateOperands(
                    validator, scope, call);

            // Permute row type according to the field names in row type as
            // written.
            final RelDataType nsRowTypeAsWritten =
                validator.getNamespace(call.operands[0]).getRowTypeAsWritten();
            final List<String> fieldNames =
                RelOptUtil.getFieldNameList(
                    nsRowTypeAsWritten);
            final RelDataType rowTypeAsWritten =
                validator.getTypeFactory().createStructType(
                    new AbstractList<Map.Entry<String, RelDataType>>()
                    {
                        public Map.Entry<String, RelDataType> get(int index)
                        {
                            return SqlValidatorUtil.lookupField(
                                rowType, fieldNames.get(index));
                        }

                        public int size()
                        {
                            return fieldNames.size();
                        }
                    });
            setRowType(rowType, rowTypeAsWritten);
            return rowType;
        default:
            throw Util.newInternal("Not a query: " + call.getKind());
        }
    }
}

// End SetopNamespace.java
