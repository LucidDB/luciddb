/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
package net.sf.farrago.ddl;

/**
 * DdlVisitor implements the visitor pattern for DDL statements.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class DdlVisitor
{

    //~ Methods ----------------------------------------------------------------

    // visitor dispatch
    public void visit(DdlCreateStmt stmt)
    {
    }

    // visitor dispatch
    public void visit(DdlDropStmt stmt)
    {
    }

    // visitor dispatch
    public void visit(DdlAlterStmt stmt)
    {
    }

    // visitor dispatch
    public void visit(DdlTruncateStmt stmt)
    {
    }

    // visitor dispatch
    public void visit(DdlCommitStmt stmt)
    {
    }

    // visitor dispatch
    public void visit(DdlRollbackStmt stmt)
    {
    }

    // visitor dispatch
    public void visit(DdlSavepointStmt stmt)
    {
    }

    // visitor dispatch
    public void visit(DdlReleaseSavepointStmt stmt)
    {
    }

    // visitor dispatch
    public void visit(DdlSetCatalogStmt stmt)
    {
    }

    // visitor dispatch
    public void visit(DdlSetSchemaStmt stmt)
    {
    }

    // visitor dispatch
    public void visit(DdlSetPathStmt stmt)
    {
    }

    // visitor dispatch
    public void visit(DdlSetParamStmt stmt)
    {
    }

    // visitor dispatch
    public void visit(DdlSetSystemParamStmt stmt)
    {
    }

    // visitor dispatch
    public void visit(DdlSetSessionParamStmt stmt)
    {
    }

    // visitor dispatch
    public void visit(DdlCheckpointStmt stmt)
    {
    }

    // visitor dispatch
    public void visit(DdlExtendCatalogStmt stmt)
    {
    }

    // visitor dispatch
    public void visit(DdlReplaceCatalogStmt stmt)
    {
    }

    // visitor dispatch
    public void visit(DdlSetSessionImplementationStmt stmt)
    {
    }

    // visitor dispatch
    public void visit(DdlGrantStmt stmt)
    {
    }

    // visitor dispatch
    public void visit(DdlImportForeignSchemaStmt stmt)
    {
    }

    // visitor dispatch
    public void visit(DdlAnalyzeStmt stmt)
    {
    }
}

// End DdlVisitor.java
