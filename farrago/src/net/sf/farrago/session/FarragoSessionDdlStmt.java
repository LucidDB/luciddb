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
package net.sf.farrago.session;

import net.sf.farrago.cwm.core.*;


/**
 * FarragoSessionDdlStmt represents the output of DDL statement parsing.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoSessionDdlStmt
{
    //~ Methods ---------------------------------------------------------------

    /**
     * @return the top-level CwmModelElement affected by this stmt, or null if
     * none
     */
    public CwmModelElement getModelElement();

    /**
     * @return whether DROP RESTRICT is in effect
     */
    public boolean isDropRestricted();

    /**
     * Called before generic validation.
     *
     * @param ddlValidator the object validating this stmt
     */
    public void preValidate(FarragoSessionDdlValidator ddlValidator);

    /**
     * Called before generic execution.
     */
    public void preExecute();

    /**
     * Called after generic execution.
     */
    public void postExecute();

    /**
     * @return true if this statement implies an auto-commit before and after
     */
    public boolean requiresCommit();
}


// End FarragoSessionDdlStmt.java
