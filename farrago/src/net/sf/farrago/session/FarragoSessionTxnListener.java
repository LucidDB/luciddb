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

import java.util.*;

import org.eigenbase.relopt.*;


/**
 * FarragoSessionTxnListener defines an interface for listening to events on a
 * {@link FarragoSessionTxnMgr}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public interface FarragoSessionTxnListener
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Notifies listener of a call to FarragoSessionTxnMgr.beginTxn.
     *
     * @param session session initiating transaction
     * @param txnId new transaction ID
     */
    public void transactionBegun(
        FarragoSession session,
        FarragoSessionTxnId txnId);

    /**
     * Notifies listener of the effect of a call to
     * FarragoSessionTxnMgr.accessTable.
     *
     * @param txnId ID of transaction in which access is occurring
     * @param localTableName qualified name of table as it is known in the local
     * catalog
     * @param accessType type of table access
     */
    public void tableAccessed(
        FarragoSessionTxnId txnId,
        List<String> localTableName,
        TableAccessMap.Mode accessType);

    /**
     * Notifies listener of a call to FarragoSessionTxnMgr.endTxn.
     *
     * @param txnId ID of ending transaction
     * @param endType how transaction is ending
     */
    public void transactionEnded(
        FarragoSessionTxnId txnId,
        FarragoSessionTxnEnd endType);
}

// End FarragoSessionTxnListener.java
