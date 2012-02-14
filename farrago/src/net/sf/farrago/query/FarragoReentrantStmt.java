/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
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
