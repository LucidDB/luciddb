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

import net.sf.farrago.cwm.core.*;

/**
 * DdlSetQualifierStmt represents a statement that establishes a default
 * qualifier, such as SET SCHEMA or SET CATALOG.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class DdlSetQualifierStmt extends DdlStmt
{
    /**
     * Construct a new DdlSetQualifierStmt.
     *
     * @param qualifier reference to qualifier to set
     */
    public DdlSetQualifierStmt(CwmModelElement qualifier)
    {
        super(qualifier);
    }

    // override DdlStmt
    public boolean requiresCommit()
    {
        return false;
    }

    // implement DdlStmt
    public void visit(DdlVisitor visitor)
    {
        visitor.visit(this);
    }
}

// End DdlSetQualifierStmt.java
