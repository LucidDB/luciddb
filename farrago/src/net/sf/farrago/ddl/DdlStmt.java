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

    protected FarragoSessionStmtContext rootStmtContext;

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

    // implement FarragoSessionDdlStmt
    public void setRootStmtContext(FarragoSessionStmtContext rootStmtContext)
    {
        this.rootStmtContext = rootStmtContext;
    }
}

// End DdlStmt.java
