/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2002-2003 Disruptive Technologies, Inc.
// You must accept the terms in LICENSE.html to use this software.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

package net.sf.saffron.sql;

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
    public final int index;
    
    public SqlDynamicParam(int index)
    {
        this.index = index;
    }

    public Object clone()
    {
        return new SqlDynamicParam(index);
    }

    public SqlKind getKind()
    {
        return SqlKind.DynamicParam;
    }
    
    public void unparse(SqlWriter writer,int leftPrec,int rightPrec)
    {
        writer.print("?");
    }
}

// End SqlDynamicParam.java
