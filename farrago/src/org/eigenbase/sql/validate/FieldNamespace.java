/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2007-2009 The Eigenbase Project
// Copyright (C) 2007-2009 SQLstream, Inc.
// Copyright (C) 2007-2009 LucidEra, Inc.
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

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;


/**
 * Implementation of {@link SqlValidatorNamespace} for a field of a record.
 *
 * <p>A field is not a very interesting namespace - except if the field has a
 * record or multiset type - but this class exists to make fields behave
 * similarly to other records for purposes of name resolution.
 *
 * @author jhyde
 * @version $Id$
 */
class FieldNamespace
    extends AbstractNamespace
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a FieldNamespace.
     *
     * @param validator Validator
     * @param dataType Data type of field
     */
    FieldNamespace(
        SqlValidatorImpl validator,
        RelDataType dataType)
    {
        super(validator, null);
        assert dataType != null;
        this.rowType = dataType;
    }

    //~ Methods ----------------------------------------------------------------

    public void setRowType(RelDataType rowType)
    {
        throw new UnsupportedOperationException();
    }

    protected RelDataType validateImpl()
    {
        return rowType;
    }

    public SqlNode getNode()
    {
        return null;
    }

    public SqlValidatorNamespace lookupChild(String name)
    {
        if (rowType.isStruct()) {
            return validator.lookupFieldNamespace(
                rowType,
                name);
        }
        return null;
    }

    public boolean fieldExists(String name)
    {
        return false;
    }
}

// End FieldNamespace.java
