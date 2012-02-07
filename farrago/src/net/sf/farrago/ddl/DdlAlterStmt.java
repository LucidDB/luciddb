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
package net.sf.farrago.ddl;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;


/**
 * DdlAlterStmt represents some but not all DDL ALTER statements. For others,
 * see {@link DdlAlterTableStructureStmt}, {@link DdlRebuildTableStmt}, and
 * {@link DdlSetSystemParamStmt}.
 *
 * @author Stephan Zuercher
 * @version $Id$
 */
public abstract class DdlAlterStmt
    extends DdlStmt
{
    //~ Enums ------------------------------------------------------------------

    private enum ActionType
    {
        ALTER_IDENTITY
    }

    //~ Instance fields --------------------------------------------------------

    private ActionType action;
    private CwmColumn column;
    private FarragoSequenceOptions identityOptions;

    //~ Constructors -----------------------------------------------------------

    /**
     * Constructs a new DdlAlterStmt.
     *
     * @param alterElement top-level element altered by this stmt
     */
    public DdlAlterStmt(CwmModelElement alterElement)
    {
        super(alterElement);
    }

    // REVIEW: SWZ: 2008-02-26: Eliminate this constructor if no red-zone
    // subclasses require it.
    /**
     * @deprecated
     */
    public DdlAlterStmt(CwmModelElement alterElement, boolean runsAsDml)
    {
        super(alterElement, runsAsDml);
    }

    //~ Methods ----------------------------------------------------------------

    public void setColumn(CwmColumn column)
    {
        this.column = column;
    }

    public void alterIdentityColumn(FarragoSequenceOptions options)
    {
        action = ActionType.ALTER_IDENTITY;
        identityOptions = options;
    }

    // implement DdlStmt
    public void visit(DdlVisitor visitor)
    {
        visitor.visit(this);
    }

    // implement DdlStmt
    public void preValidate(FarragoSessionDdlValidator ddlValidator)
    {
        // REVIEW: SWZ: 2008-02-26: It may be possible to eliminate this
        // reentrant session if DdlAlterGenericStmt (and its red-zone
        // subclasses) don't require it.

        // Use a reentrant session to simplify cleanup.
        FarragoSession session = ddlValidator.newReentrantSession();
        try {
            execute(ddlValidator, session);
        } catch (Throwable ex) {
            throw FarragoResource.instance().ValidatorAlterFailed.ex(ex);
        } finally {
            ddlValidator.releaseReentrantSession(session);
        }
    }

    // refine DdlStmt
    public void postCommit(FarragoSessionDdlValidator ddlValidator)
    {
        if (ddlValidator instanceof DdlValidator) {
            DdlValidator val = (DdlValidator) ddlValidator;
            val.handlePostCommit(getModelElement(), "Alter");
        }
    }

    /**
     * Execute the alter statement
     *
     * @param ddlValidator the session validator
     * @param session a reentrant session
     */
    protected abstract void execute(
        FarragoSessionDdlValidator ddlValidator,
        FarragoSession session);
}

// End DdlAlterStmt.java
