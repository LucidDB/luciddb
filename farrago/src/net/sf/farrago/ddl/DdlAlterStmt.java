/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later Eigenbase-approved version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307  USA
*/
package net.sf.farrago.ddl;

import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.session.*;


/**
 * DdlAlterStmt represents a DDL ALTER statement.  Represents any kind
 * of ALTER statement except ALTER SYSTEM ..., which is handled by
 * {@link DdlSetSystemParamStmt}.
 *
 * @author Stephan Zuercher
 * @version $Id$
 */
public class DdlAlterStmt extends DdlStmt
{
    //~ Constructors ----------------------------------------------------------

    /**
     * Construct a new DdlAlterStmt.
     *
     * @param alterElement top-level element altered by this stmt
     */
    public DdlAlterStmt(CwmModelElement alterElement)
    {
        super(alterElement);
    }

    //~ Methods ---------------------------------------------------------------

    // implement DdlStmt
    public void visit(DdlVisitor visitor)
    {
        visitor.visit(this);
    }

    // implement DdlStmt
    public void preValidate(FarragoSessionDdlValidator ddlValidator)
    {
    }
}


// End DdlAlterStmt.java
