/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2003-2009 John V. Sichi
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
    //~ Methods ----------------------------------------------------------------

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
     * Called immediately after generic execution.
     */
    public void postExecute();

    /**
     * Called after execution, after committing the repository transaction.
     *
     * @param ddlValidator the object validating this stmt
     */
    public void postCommit(FarragoSessionDdlValidator ddlValidator);

    /**
     * @return true if this statement implies an auto-commit before and after
     */
    public boolean requiresCommit();

    /**
     * @return true if this DDL statement should be treated like a DML statement
     * with respect to locking
     */
    public boolean runsAsDml();

    /**
     * Sets the root statement context to be used for invoking
     * reentrant SQL.
     *
     * @param rootStmtContext the root statement context in which
     * this DDL statement is being invoked
     */
    public void setRootStmtContext(FarragoSessionStmtContext rootStmtContext);
}

// End FarragoSessionDdlStmt.java
