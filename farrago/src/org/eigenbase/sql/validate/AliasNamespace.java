/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2008-2008 The Eigenbase Project
// Copyright (C) 2008-2008 Disruptive Tech
// Copyright (C) 2008-2008 LucidEra, Inc.
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
import org.eigenbase.sql.fun.*;


/**
 * Namespace for an <code>AS t(c1, c2, ...)</code> clause.
 *
 * <p>A namespace is necessary only if there is a column list, in order to
 * re-map column names; a <code>relation AS t</code> clause just uses the same
 * namespace as <code>relation</code>.
 *
 * @author jhyde
 * @version $Id$
 * @since October 9, 2008
 */
public class AliasNamespace
    extends AbstractNamespace
{
    //~ Instance fields --------------------------------------------------------

    protected final SqlCall call;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates an AliasNamespace.
     *
     * @param validator Validator
     * @param call Call to AS operator
     * @param enclosingNode Enclosing node
     */
    protected AliasNamespace(
        SqlValidatorImpl validator,
        SqlCall call,
        SqlNode enclosingNode)
    {
        super(validator, enclosingNode);
        this.call = call;
        assert call.getOperator() == SqlStdOperatorTable.asOperator;
    }

    //~ Methods ----------------------------------------------------------------

    protected RelDataType validateImpl()
    {
        final List<String> nameList = new ArrayList<String>();
        final SqlValidatorNamespace childNs =
            validator.getNamespace(call.getOperands()[0]);
        final RelDataType rowType = childNs.getRowTypeSansSystemColumns();
        for (int i = 2; i < call.getOperands().length; ++i) {
            final SqlNode operand = call.getOperands()[i];
            String name = ((SqlIdentifier) operand).getSimple();
            if (nameList.contains(name)) {
                throw validator.newValidationError(
                    operand,
                    EigenbaseResource.instance().AliasListDuplicate.ex(
                        name));
            }
            nameList.add(name);
        }
        if (nameList.size() != rowType.getFieldCount()) {
            StringBuilder buf = new StringBuilder();
            buf.append("(");
            int k = 0;
            for (RelDataTypeField field : rowType.getFieldList()) {
                if (k++ > 0) {
                    buf.append(", ");
                }
                buf.append("'");
                buf.append(field.getName());
                buf.append("'");
            }
            buf.append(")");

            // Position error at first name in list.
            throw validator.newValidationError(
                call.getOperands()[2],
                EigenbaseResource.instance().AliasListDegree.ex(
                    rowType.getFieldCount(),
                    buf.toString(),
                    nameList.size()));
        }
        final List<RelDataType> typeList = new ArrayList<RelDataType>();
        for (RelDataTypeField field : rowType.getFieldList()) {
            typeList.add(field.getType());
        }
        return validator.getTypeFactory().createStructType(
            typeList,
            nameList);
    }

    public SqlNode getNode()
    {
        return call;
    }

    public String translate(String name)
    {
        final RelDataType underlyingRowType =
            validator.getValidatedNodeType(call.getOperands()[0]);
        int i = 0;
        for (RelDataTypeField field : rowType.getFieldList()) {
            if (field.getName().equals(name)) {
                return underlyingRowType.getFieldList().get(i).getName();
            }
            ++i;
        }
        throw new AssertionError(
            "unknown field '" + name
            + "' in rowtype " + underlyingRowType);
    }
}

// End AliasNamespace.java
