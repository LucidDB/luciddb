/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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
import org.eigenbase.sql.util.SqlVisitor;


/**
 * A <code>SqlDynamicParam</code> represents a dynamic parameter marker in an
 * SQL statement.  The textual order in which dynamic parameters appear
 * within an SQL statement is the only property which distinguishes them,
 * so this 0-based index is recorded as soon as the parameter is encountered.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class SqlDynamicParam extends SqlNode
{
    //~ Instance fields -------------------------------------------------------

    public final int index;

    //~ Constructors ----------------------------------------------------------

    public SqlDynamicParam(
        int index,
        SqlParserPos pos)
    {
        super(pos);
        this.index = index;
    }

    //~ Methods ---------------------------------------------------------------

    public SqlKind getKind()
    {
        return SqlKind.DynamicParam;
    }

    public void unparse(
        SqlWriter writer,
        int leftPrec,
        int rightPrec)
    {
        writer.print("?");
    }

    public void validate(SqlValidator validator, SqlValidator.Scope scope)
    {
        validator.validateDynamicParam(this);
    }

    public void accept(SqlVisitor visitor)
    {
        visitor.visit(this);
    }

    public boolean equalsDeep(SqlNode node)
    {
        if (node instanceof SqlDynamicParam) {
            SqlDynamicParam that = (SqlDynamicParam) node;
            return this.index == that.index;
        }
        return false;
    }
}


// End SqlDynamicParam.java
