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
package net.sf.farrago.plannerviz;

import net.sf.farrago.session.*;

import org.eigenbase.util.*;

import java.lang.reflect.*;
import java.util.*;

/**
 * FarragoPlannervizPluginFactory implements the
 * {@link FarragoSessionPersonalityFactory} interface by producing
 * a session personality which adds in the necessary listener for
 * visualizing planner events.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoPlannervizPluginFactory
    implements FarragoSessionPersonalityFactory
{
    // implement FarragoSessionPersonalityFactory
    public FarragoSessionPersonality newSessionPersonality(
        FarragoSession session,
        FarragoSessionPersonality defaultPersonality)
    {
        // create a delegating proxy, overriding only the methods
        // we're interested in
        return (FarragoSessionPersonality)
            Proxy.newProxyInstance(
                FarragoPlannervizPluginFactory.class.getClassLoader(),
                new Class[] {
                    FarragoSessionPersonality.class,
                },
                new PlannervizPersonality(defaultPersonality));
    }

    public static class PlannervizPersonality
        extends DelegatingInvocationHandler
    {
        private final FarragoSessionPersonality defaultPersonality;

        PlannervizPersonality(FarragoSessionPersonality defaultPersonality)
        {
            this.defaultPersonality = defaultPersonality;
        }

        // implement DelegatingInvocationHandler
        protected Object getTarget()
        {
            return defaultPersonality;
        }

        // implement FarragoSessionPersonality
        public void definePlannerListeners(FarragoSessionPlanner planner)
        {
            defaultPersonality.definePlannerListeners(planner);
            planner.addListener(
                new FarragoPlanVisualizer());
        }
    }
}

// End FarragoPlannervizPluginFactory.java
