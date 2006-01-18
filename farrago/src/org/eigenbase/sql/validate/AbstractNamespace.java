/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2004-2005 The Eigenbase Project
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
package org.eigenbase.sql.validate;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.sql.SqlNodeList;
import org.eigenbase.sql.parser.SqlParserPos;
import org.eigenbase.util.Util;

// REVIEW jvs 12-May-2005:  I suggest you follow Intellij's recommendation
// and edit your template to match all of the other classes in this package.
/**
 * Created by IntelliJ IDEA.
 * User: jhyde
 * Date: Mar 3, 2005
 * Time: 1:22:43 PM
 * To change this template use File | Settings | File Templates.
 */
abstract class AbstractNamespace implements SqlValidatorNamespace
{
    protected final SqlValidatorImpl validator;
    /**
     * Whether this scope is currently being validated. Used to check for
     * cycles.
     */
    private SqlValidatorImpl.Status status = SqlValidatorImpl.Status.Unvalidated;
    /**
     * Type of the output row, which comprises the name and type of each
     * output column. Set on validate.
     */
    protected RelDataType rowType;
    private Object extra;

    private boolean forceNullable;

    /**
     * Creates an AbstractNamespace.
     */
    AbstractNamespace(SqlValidatorImpl validator)
    {
        this.validator = validator;
    }

    public SqlMoniker[] lookupHints(SqlParserPos pos)
    {
        return Util.emptySqlMonikerArray;
    }

    public void validate()
    {
        switch (status.getOrdinal()) {
        case SqlValidatorImpl.Status.Unvalidated_ordinal:
            try {
                status = SqlValidatorImpl.Status.InProgress;
                Util.permAssert(rowType == null,
                    "Namespace.rowType must be null before validate has been called");
                rowType = validateImpl();
                Util.permAssert(rowType != null,
                    "validateImpl() returned null");
                if (forceNullable) {
                    // REVIEW jvs 10-Oct-2005: This may not be quite right
                    // if it means that nullability will be forced in the
                    // ON clause where it doesn't belong.
                    rowType = validator.getTypeFactory().
                        createTypeWithNullability(
                            rowType, true);
                }
            } finally {
                status = SqlValidatorImpl.Status.Valid;
            }
            break;
        case SqlValidatorImpl.Status.InProgress_ordinal:
            throw Util
                .newInternal("todo: Cycle detected during type-checking");
        case SqlValidatorImpl.Status.Valid_ordinal:
            break;
        default:
            throw status.unexpected();
        }
    }

    /**
     * Validates this scope and returns the type of the records it returns.
     * External users should call {@link #validate}, which uses the
     * {@link #status} field to protect against cycles.
     *
     * @return record data type, never null
     * @post return != null
     */
    protected abstract RelDataType validateImpl();

    public RelDataType getRowType()
    {
        if (rowType == null) {
            validator.validateNamespace(this);
            Util.permAssert(rowType != null, "validate must set rowType");
        }
        return rowType;
    }

    public void setRowType(RelDataType rowType)
    {
        this.rowType = rowType;
    }

    public SqlValidatorTable getTable() {
        return null;
    }

    public SqlValidatorNamespace lookupChild(
        String name,
        SqlValidatorScope[] ancestorOut,
        int[] offsetOut)
    {
        return validator.lookupFieldNamespace(getRowType(), name, ancestorOut,
            offsetOut);
    }

    public boolean fieldExists(String name)
    {
        final RelDataType rowType = getRowType();
        final RelDataType dataType =
            SqlValidatorUtil.lookupField(rowType, name);
        return dataType != null;
    }

    public Object getExtra()
    {
        return extra;
    }

    public void setExtra(Object o)
    {
        this.extra = o;
    }

    public SqlNodeList getMonotonicExprs()
    {
        return SqlNodeList.Empty;
    }

    public boolean isMonotonic(String columnName)
    {
        return false;
    }
    
    public void makeNullable()
    {
        forceNullable = true;
    }
}
