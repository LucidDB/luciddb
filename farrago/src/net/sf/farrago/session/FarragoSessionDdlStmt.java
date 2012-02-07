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
