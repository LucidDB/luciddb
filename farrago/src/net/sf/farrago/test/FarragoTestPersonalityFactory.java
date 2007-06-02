/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2007 The Eigenbase Project
// Copyright (C) 2006-2007 Disruptive Tech
// Copyright (C) 2006-2007 LucidEra, Inc.
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
package net.sf.farrago.test;

import java.util.*;

import net.sf.farrago.db.*;
import net.sf.farrago.defimpl.*;
import net.sf.farrago.query.*;
import net.sf.farrago.session.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;


/**
 * FarragoTestPersonalityFactory implements the {@link
 * FarragoSessionPersonalityFactory} interface with some tweaks just for
 * testing.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoTestPersonalityFactory
    implements FarragoSessionPersonalityFactory
{
    //~ Methods ----------------------------------------------------------------

    // implement FarragoSessionPersonalityFactory
    public FarragoSessionPersonality newSessionPersonality(
        FarragoSession session,
        FarragoSessionPersonality defaultPersonality)
    {
        return new FarragoTestSessionPersonality((FarragoDbSession) session);
    }

    //~ Inner Classes ----------------------------------------------------------

    private static class FarragoTestSessionPersonality
        extends FarragoDefaultSessionPersonality
    {
        protected FarragoTestSessionPersonality(FarragoDbSession session)
        {
            super(session);
        }

        // implement FarragoSessionPersonality
        public void registerRelMetadataProviders(
            ChainedRelMetadataProvider chain)
        {
            chain.addProvider(new FarragoTestRelMetadataProvider());
        }
    }

    public static class FarragoTestRelMetadataProvider
        extends ReflectiveRelMetadataProvider
    {
        public Double getRowCount(AggregateRelBase rel)
        {
            // Lie and say aggregates always returns a million rows.
            return 1000000.0;
        }
    }
}

// End FarragoTestPersonalityFactory.java
