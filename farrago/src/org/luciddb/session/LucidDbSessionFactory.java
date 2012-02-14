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
package org.luciddb.session;

import java.util.*;

import net.sf.farrago.db.*;
import net.sf.farrago.defimpl.*;
import net.sf.farrago.session.*;


/**
 * LucidDbSessionFactory extends {@link FarragoDbSessionFactory} with
 * LucidDB-specific behavior.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class LucidDbSessionFactory
    extends FarragoDefaultSessionFactory
{
    //~ Methods ----------------------------------------------------------------

    // implement FarragoSessionPersonalityFactory
    public FarragoSessionPersonality newSessionPersonality(
        FarragoSession session,
        FarragoSessionPersonality defaultPersonality)
    {
        return new LucidDbSessionPersonality(
            (FarragoDbSession) session,
            defaultPersonality,
            false);
    }

    // implement FarragoSessionFactory
    public void applyFennelExtensionParameters(Properties map)
    {
        super.applyFennelExtensionParameters(map);

        // Tell Fennel to checkpoint after each transaction.
        map.put("forceTxns", "true");
    }

    // implement FarragoSessionFactory
    public FarragoSessionTxnMgr newTxnMgr()
    {
        return new LucidDbTxnMgr();
    }
}

// End LucidDbSessionFactory.java
