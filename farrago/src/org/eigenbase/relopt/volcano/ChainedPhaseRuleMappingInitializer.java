/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006 The Eigenbase Project
// Copyright (C) 2006 SQLstream, Inc.
// Copyright (C) 2009 Dynamo BI Corporation
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
package org.eigenbase.relopt.volcano;

import java.util.*;


/**
 * ChainedPhaseRuleMappingInitializer is an abstract implementation of {@link
 * VolcanoPlannerPhaseRuleMappingInitializer} that allows additional rules to be
 * layered ontop of those configured by a subordinate
 * VolcanoPlannerPhaseRuleMappingInitializer.
 *
 * @author Stephan Zuercher
 * @see VolcanoPlannerPhaseRuleMappingInitializer
 */
public abstract class ChainedPhaseRuleMappingInitializer
    implements VolcanoPlannerPhaseRuleMappingInitializer
{
    //~ Instance fields --------------------------------------------------------

    private final VolcanoPlannerPhaseRuleMappingInitializer subordinate;

    //~ Constructors -----------------------------------------------------------

    public ChainedPhaseRuleMappingInitializer(
        VolcanoPlannerPhaseRuleMappingInitializer subordinate)
    {
        this.subordinate = subordinate;
    }

    //~ Methods ----------------------------------------------------------------

    public final void initialize(
        Map<VolcanoPlannerPhase, Set<String>> phaseRuleMap)
    {
        // Initialize subordinate's mappings.
        subordinate.initialize(phaseRuleMap);

        // Initialize our mappings.
        chainedInitialize(phaseRuleMap);
    }

    /**
     * Extend this method to provide phase-to-rule mappings beyond what is
     * provided by this initializer's subordinate.
     *
     * <p>When this method is called, the map will already be pre-initialized
     * with empty sets for each VolcanoPlannerPhase. Implementations must not
     * return having added or removed keys from the map, although it is safe to
     * temporarily add or remove keys.
     *
     * @param phaseRuleMap the {@link VolcanoPlannerPhase}-rule description map
     *
     * @see VolcanoPlannerPhaseRuleMappingInitializer
     */
    public abstract void chainedInitialize(
        Map<VolcanoPlannerPhase, Set<String>> phaseRuleMap);
}

// End ChainedPhaseRuleMappingInitializer.java
