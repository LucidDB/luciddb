/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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

import net.sf.farrago.cwm.core.*;
import net.sf.farrago.session.*;


/**
 * DdlStmt represents the output of DDL statement parsing. Most DDL statements
 * update the catalog directly as they are parsed. DdlStmt does not duplicate
 * the information written to the catalog; it only references and annotates it.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class DdlStmt
    implements FarragoSessionDdlStmt
{
    //~ Instance fields --------------------------------------------------------

    private final CwmModelElement modelElement;

    /**
     * True if the DDL statement is treated as a DML statement with respect to
     * how locking behaves
     */
    private final boolean runsAsDml;

    //~ Constructors -----------------------------------------------------------

    protected DdlStmt(CwmModelElement modelElement)
    {
        this(modelElement, false);
    }

    protected DdlStmt(CwmModelElement modelElement, boolean runsAsDml)
    {
        this.modelElement = modelElement;
        this.runsAsDml = runsAsDml;
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoSessionDdlStmt
    public CwmModelElement getModelElement()
    {
        return modelElement;
    }

    // implement FarragoSessionDdlStmt
    public boolean isDropRestricted()
    {
        return false;
    }

    // implement FarragoSessionDdlStmt
    public void preValidate(FarragoSessionDdlValidator ddlValidator)
    {
        // by default, assume everything has already been done during parsing
    }
    
    // implement FarragoSessionDdlStmt
    public void preExecute()
    {
        // caller does the work
    }

    // implement FarragoSessionDdlStmt
    public void postExecute()
    {
        // caller does the work
    }

    // implement FarragoSessionDdlStmt
    public void postCommit(FarragoSessionDdlValidator ddlValidator)
    {
        // by default, do nothing
    }

    // implement FarragoSessionDdlStmt
    public boolean requiresCommit()
    {
        return true;
    }

    // implement FarragoSessionDdlStmt
    public boolean runsAsDml()
    {
        return runsAsDml;
    }

    /**
     * Invokes a visitor on this statement.
     *
     * @param visitor DdlVisitor to invoke
     */
    public abstract void visit(DdlVisitor visitor);
}

// End DdlStmt.java
