/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2009-2009 The Eigenbase Project
// Copyright (C) 2009-2009 SQLstream, Inc.
// Copyright (C) 2009-2009 LucidEra, Inc.
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
 * Implements {@link org.eigenbase.sql.validate.SqlValidatorTable} by
 * delegating to a parent table.
 *
 * @author jhyde
 * @version $Id$
 * @since Mar 9, 2009
 */
public abstract class DelegatingSqlValidatorTable implements SqlValidatorTable
{
    protected final SqlValidatorTable table;

    /**
     * Creates a DelegatingSqlValidatorTable.
     *
     * @param table Parent table
     */
    public DelegatingSqlValidatorTable(SqlValidatorTable table)
    {
        this.table = table;
    }

    public RelDataType getRowType()
    {
        return table.getRowType();
    }

    public String[] getQualifiedName()
    {
        return table.getQualifiedName();
    }

    public SqlMonotonicity getMonotonicity(String columnName)
    {
        return table.getMonotonicity(columnName);
    }

    public SqlAccessType getAllowedAccess()
    {
        return table.getAllowedAccess();
    }
}

// End DelegatingSqlValidatorTable.java
