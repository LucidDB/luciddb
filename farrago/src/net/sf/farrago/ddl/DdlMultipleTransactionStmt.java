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
import net.sf.farrago.session.*;


/**
 * DdlMultipleTransactionStmt represents a DdlStmt that requires its work to
 * divided among multiple repository transactions to avoid holding the
 * repository transaction lock for excessive periods of time (and thereby
 * blocking other statements).
 *
 * <p>NOTE jvs 11-Dec-2008: the implementations of executeUnlocked and
 * completeAfterExecuteUnlocked should not reuse references to repository
 * objects obtained during the initial repository transaction, as they may be
 * stale. Instead, MOFID's should be used to reload references as needed.
 *
 * @author Stephan Zuercher
 * @version $Id$
 */
public interface DdlMultipleTransactionStmt
    extends FarragoSessionDdlStmt
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Provides access to the repository in preparation for the execution of
     * DdlStmt. This method is invoked within the context the original
     * repository transaction for the DDL statement. Whether that transaction is
     * a write transaction depends on the DDL statement being executed.
     *
     * @param ddlValidator DDL validator for this statement
     *
     * @see net.sf.farrago.catalog.FarragoReposTxnContext
     */
    public void prepForExecuteUnlocked(
        FarragoSessionDdlValidator ddlValidator,
        FarragoSession session);

    /**
     * Executes long-running DDL actions. This method is invoked outside the
     * context of any repository transaction.
     *
     * @param ddlValidator DDL validator for this statement
     * @param session reentrant Farrago session which may be used to execute DML
     * statements
     */
    public void executeUnlocked(
        FarragoSessionDdlValidator ddlValidator,
        FarragoSession session);

    /**
     * Checks whether the {@link #completeAfterExecuteUnlocked(
     * FarragoSessionDdlValidator, FarragoSession, boolean)} method requires a
     * repository write transaction.
     *
     * @return true if a write txn must be started before executing the
     * completion step, false if a read txn is sufficient
     */
    public boolean completeRequiresWriteTxn();

    /**
     * Provides access to the repository after execution of the DDL. Typically
     * implementations of this method modify the repository to store the results
     * of {@link #executeUnlocked(FarragoSessionDdlValidator, FarragoSession)}.
     * This method is invoked in a {@link
     * FarragoReposTxnContext#beginLockedTxn(boolean) locked} repository
     * transaction. The method {@link #completeRequiresWriteTxn()} controls
     * whether the transaction read-only or not. This method may not access
     * and/or modify repository objects loaded in a previous transaction unless
     * they are reloaded by MOF ID. Be aware that objects may have been modified
     * by another session unless some external mechanism (for instance, the
     * "table-in-use" collection) guarantees that they have not been modified by
     * another statement.
     *
     * <p>Note that any repository modifications made during the execution of
     * this method <b>will not</b> be post-processed by {@link DdlValidator}.
     * For instance, {@link DdlValidator#checkJmiConstraints(RefObject)} is not
     * called, and therefore any mandatory default primitives are not
     * automatically set, which will cause errors later if the attributes have
     * not been explicitly initialized. See {@link
     * org.eigenbase.jmi.JmiObjUtil#setMandatoryPrimitiveDefaults}.
     *
     * @param ddlValidator DDL validator for this statement
     * @param session reentrant Farrago session which may be used to execute DML
     * statements
     * @param success whether the execution succeeded; detection of failure can
     * be used to recover
     */
    public void completeAfterExecuteUnlocked(
        FarragoSessionDdlValidator ddlValidator,
        FarragoSession session,
        boolean success);
}

// End DdlMultipleTransactionStmt.java
