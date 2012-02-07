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
package net.sf.farrago.session.mock;

import net.sf.farrago.catalog.*;


/**
 * MockReposTxnContext overrides FarragoReposTxnContext so that no FarragoRepos
 * implementation is needed. Note that this class does not actually create any
 * transaction or lock. See {@link MockSessionStmtValidator}.
 *
 * @author stephan/jack
 * @version $Id$
 * @since Dec 8, 2006
 */
public class MockReposTxnContext
    extends FarragoReposTxnContext
{
    //~ Instance fields --------------------------------------------------------

    private boolean isRead;
    private boolean isWrite;
    private boolean locked;

    //~ Constructors -----------------------------------------------------------

    public MockReposTxnContext()
    {
        super(null);
    }

    //~ Methods ----------------------------------------------------------------

    public void beginReadTxn()
    {
        assert (!isRead && !isWrite);
        isRead = true;
    }

    public void beginWriteTxn()
    {
        assert (!isRead && !isWrite);
        isWrite = true;
    }

    public void commit()
    {
        isRead = isWrite = locked = false;
    }

    public boolean isReadTxnInProgress()
    {
        return isRead;
    }

    public boolean isTxnInProgress()
    {
        return isRead || isWrite || locked;
    }

    public void rollback()
    {
        isRead = isWrite = locked = false;
    }

    public void beginLockedTxn(boolean readOnly)
    {
        assert !isRead && !isWrite && !locked;
        locked = true;
        if (readOnly) {
            isRead = true;
        } else {
            isWrite = true;
        }
    }

    public void unlockAfterTxn()
    {
        isRead = isWrite = locked = false;
    }
}

// End MockReposTxnContext.java
