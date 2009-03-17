/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2007 The Eigenbase Project
// Copyright (C) 2006-2007 Disruptive Tech
// Copyright (C) 2006-2007 LucidEra, Inc.
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
package net.sf.farrago.query;

import net.sf.farrago.catalog.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;


/**
 * FarragoReentrantStmt provides infrastructure for safely executing a reentrant
 * statement as part of the preparation of some outer statement. The caller is
 * required to define a subclass which executes the internal statement; the base
 * class takes care of releasing resources correctly regardless of how the
 * internal statement fares.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class FarragoReentrantStmt
{
    //~ Instance fields --------------------------------------------------------

    private FarragoSessionPreparingStmt preparingStmt;
    private FarragoSessionStmtContext stmtContext;
    private FarragoSessionStmtContext rootStmtContext;

    //~ Constructors -----------------------------------------------------------

    public FarragoReentrantStmt(FarragoSessionStmtContext rootStmtContext)
    {
        this.rootStmtContext = rootStmtContext;
    }

    //~ Methods ----------------------------------------------------------------

    protected FarragoSessionPreparingStmt getPreparingStmt()
    {
        return preparingStmt;
    }

    protected FarragoSessionStmtContext getStmtContext()
    {
        return stmtContext;
    }

    protected FarragoSessionStmtContext getRootStmtContext()
    {
        return rootStmtContext;
    }

    /**
     * Supplies subclass-specific behavior.
     */
    protected abstract void executeImpl()
        throws Exception;

    /**
     * Executes the reentrant statement; subclass specified what to do in {@link
     * #executeImpl}.
     *
     * @param session session on which to execute
     * @param allocateSession if true, allocate a re-entrant session; if false,
     * use the given session directly
     */
    public void execute(FarragoSession session, boolean allocateSession)
    {
        if (allocateSession) {
            session = session.getSessionFactory().newReentrantSession(session);
        }

        stmtContext = session.newStmtContext(null, rootStmtContext);
        FarragoSessionStmtValidator stmtValidator = session.newStmtValidator();
        FarragoReposTxnContext reposTxnContext =
            new FarragoReposTxnContext(session.getRepos());
        stmtValidator.setReposTxnContext(reposTxnContext);
        boolean rollback = true;
        try {
            preparingStmt =
                session.getPersonality().newPreparingStmt(
                    stmtContext,
                    rootStmtContext,
                    stmtValidator);
            preparingStmt.preImplement();
            executeImpl();
            rollback = false;
        } catch (Throwable ex) {
            throw FarragoResource.instance().SessionReentrantStmtFailed.ex(ex);
        } finally {
            if (rollback && !reposTxnContext.isReadTxnInProgress()) {
                reposTxnContext.rollback();
            } else {
                reposTxnContext.commit();
            }
            reposTxnContext.unlockAfterTxn();
            stmtContext.closeAllocation();
            stmtValidator.closeAllocation();
            if (allocateSession) {
                session.getSessionFactory().releaseReentrantSession(session);
            }
        }
    }
}

// End FarragoReentrantStmt.java
