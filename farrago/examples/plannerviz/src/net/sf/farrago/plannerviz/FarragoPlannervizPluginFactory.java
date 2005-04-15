/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
//
// This program is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation; either version 2 of the License, or (at your option)
// any later version approved by The Eigenbase Project.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
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
            planner.addListener(
                new FarragoPlanVisualizer());
        }
    }
}

// End FarragoPlannervizPluginFactory.java
