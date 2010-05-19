/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006 The Eigenbase Project
// Copyright (C) 2006 SQLstream, Inc.
// Copyright (C) 2006 Dynamo BI Corporation
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
