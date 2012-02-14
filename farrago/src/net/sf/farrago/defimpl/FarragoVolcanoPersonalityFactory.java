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
package net.sf.farrago.defimpl;

import net.sf.farrago.db.*;
import net.sf.farrago.session.*;


/**
 * FarragoVolcanoPersonalityFactory implements the {@link
 * FarragoSessionPersonalityFactory} interface by using Volcano to implement
 * cost-based optimization.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoVolcanoPersonalityFactory
    implements FarragoSessionPersonalityFactory
{
    //~ Methods ----------------------------------------------------------------

    // implement FarragoSessionPersonalityFactory
    public FarragoSessionPersonality newSessionPersonality(
        FarragoSession session,
        FarragoSessionPersonality defaultPersonality)
    {
        return new FarragoVolcanoSessionPersonality((FarragoDbSession) session);
    }

    //~ Inner Classes ----------------------------------------------------------

    private static class FarragoVolcanoSessionPersonality
        extends FarragoDefaultSessionPersonality
    {
        protected FarragoVolcanoSessionPersonality(FarragoDbSession session)
        {
            super(session);
        }

        // implement FarragoSessionPersonality
        public FarragoSessionPlanner newPlanner(
            FarragoSessionPreparingStmt stmt,
            boolean init)
        {
            FarragoDefaultPlanner planner = new FarragoDefaultPlanner(stmt);
            if (init) {
                planner.init();
            }
            return planner;
        }
    }
}

// End FarragoVolcanoPersonalityFactory.java
