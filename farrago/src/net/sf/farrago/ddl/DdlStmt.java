/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
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

package net.sf.farrago.ddl;

import net.sf.farrago.session.*;
import net.sf.farrago.cwm.core.*;

/**
 * DdlStmt represents the output of DDL statement parsing.  Most DDL statements
 * update the catalog directly as they are parsed.  DdlStmt does not duplicate
 * the information written to the catalog; it only references and annotates it.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class DdlStmt implements FarragoSessionDdlStmt
{
    private final CwmModelElement modelElement;

    protected DdlStmt(CwmModelElement modelElement)
    {
        this.modelElement = modelElement;
    }

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
    public boolean requiresCommit()
    {
        return true;
    }

    /**
     * Invoke a visitor on this statement.
     *
     * @param visitor DdlVisitor to invoke
     */
    public abstract void visit(DdlVisitor visitor);
}

// End DdlStmt.java
