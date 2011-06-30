/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2011 The Eigenbase Project
// Copyright (C) 2011 SQLstream, Inc.
// Copyright (C) 2011 Dynamo BI Corporation
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

import org.eigenbase.sql.parser.SqlParserPos;

/**
 * Abstract base class for parse tree nodes representing DML operations
 * (INSERt, UPDATE, DELETE).
 *
 * @version $Id$
 */
public abstract class SqlDml
    extends SqlCall
{
    /**
     * Creates a SqlDml.
     *
     * @param operator Operator
     * @param operands Operands
     * @param pos Parse position
     */
    protected SqlDml(
        SqlOperator operator,
        SqlNode [] operands,
        SqlParserPos pos)
    {
        super(operator, operands, pos);
    }

    /**
     * @return the identifier for the target table of the DML operation
     */
    public abstract SqlIdentifier getTargetTable();

    /**
     * @return the alias for the target table of the DML operation
     */
    public abstract SqlIdentifier getAlias();

    /**
     * @return the list of target column names, or null for all columns in the
     *         target table
     */
    public abstract SqlNodeList getTargetColumnList();

    /**
     * @return the source expression for the data to be inserted, updated,
     *   deleted
     */
    public abstract SqlNode getSource();
}

// End SqlDml.java
