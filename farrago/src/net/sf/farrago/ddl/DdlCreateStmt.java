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
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.session.*;


/**
 * DdlCreateStmt represents a DDL CREATE statement of any kind.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class DdlCreateStmt extends DdlStmt
{
    //~ Constructors ----------------------------------------------------------

    /**
     * Construct a new DdlCreateStmt.
     *
     * @param createdElement top-level element created by this stmt
     */
    public DdlCreateStmt(CwmModelElement createdElement)
    {
        super(createdElement);
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
        if (getModelElement() instanceof CwmSchema) {
            // for CREATE SCHEMA, override the default qualifier
            // with the new schema
            ddlValidator.getSessionVariables().schemaCatalogName =
                getModelElement().getNamespace().getName();
            ddlValidator.getSessionVariables().schemaName =
                getModelElement().getName();
        }
    }
}


// End DdlCreateStmt.java
