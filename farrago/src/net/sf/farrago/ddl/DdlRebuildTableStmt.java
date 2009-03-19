/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 LucidEra, Inc.
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
package net.sf.farrago.ddl;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.relational.*;

import org.eigenbase.sql.*;
import org.eigenbase.sql.pretty.*;


/**
 * DdlRebuildTableStmt represents an ALTER TABLE ... REBUILD statement. The
 * statement compacts data stored in a table's indexes by removing deleted
 * entries.
 *
 * <p>Note: Although DdlRebuildTableStmt is an ALTER statement, it does not
 * extend {@link DdlAlterStmt}. This avoids the complexity of having subclasses
 * of DdlAlterStmt which may or may not also be implementations of {@link
 * DdlMultipleTransactionStmt}.
 *
 * @author John Pham
 * @version $Id$
 */
public class DdlRebuildTableStmt
    extends DdlReloadTableStmt
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Constructs a DdlRebuildTableStmt.
     */
    public DdlRebuildTableStmt(CwmTable table)
    {
        super(table);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Generates the query: "insert into T select * from T"
     */
    protected String getReloadDml(SqlPrettyWriter writer)
    {
        SqlIdentifier tableName =
            FarragoCatalogUtil.getQualifiedName(getTable());

        writer.print("insert into ");
        tableName.unparse(writer, 0, 0);
        writer.print(" select * from ");
        tableName.unparse(writer, 0, 0);
        String sql = writer.toString();
        return sql;
    }
}

// End DdlRebuildTableStmt.java
