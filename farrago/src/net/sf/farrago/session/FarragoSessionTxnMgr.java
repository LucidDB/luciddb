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

import org.eigenbase.relopt.*;


/**
 * FarragoSessionTxnMgr defines the interface for transaction management across
 * sessions. It is under development and currently only addresses table access,
 * so it is likely to change drastically. In particular, it will be refined to
 * allow different data wrappers to use different transaction managers; once
 * that happens, most extensions will probably use a common implementation which
 * knows how to coordinate two-phase commits across wrapper-level managers.
 * Until then, transaction management is up to each extension. Another major
 * change required is coordination with Fennel's notion of transactions.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoSessionTxnMgr
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Adds a listener for transaction events.
     *
     * @param listener new listener
     */
    public void addListener(
        FarragoSessionTxnListener listener);

    /**
     * Removes a listener for transaction events.
     *
     * @param listener listener to remove
     */
    public void removeListener(
        FarragoSessionTxnListener listener);

    /**
     * Begins a new transaction.
     *
     * @param session session initiating the transaction
     *
     * @return transaction ID
     */
    public FarragoSessionTxnId beginTxn(
        FarragoSession session);

    /**
     * Notifies transaction manager that a collection of tables is about to be
     * accessed.
     *
     * @param txnId ID of accessing transaction
     * @param tableAccessMap information about planned table accesses
     */
    public void accessTables(
        FarragoSessionTxnId txnId,
        TableAccessMap tableAccessMap);

    /**
     * Notifies transaction manager that a transaction is ending.
     *
     * @param txnId ID of ending transaction
     * @param endType how transaction is ending
     */
    public void endTxn(
        FarragoSessionTxnId txnId,
        FarragoSessionTxnEnd endType);
}

// End FarragoSessionTxnMgr.java
