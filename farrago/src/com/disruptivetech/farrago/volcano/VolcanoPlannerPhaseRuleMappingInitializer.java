/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2009 SQLstream, Inc.
// Copyright (C) 2006-2009 The Eigenbase Project
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
package com.disruptivetech.farrago.volcano;

import java.util.*;


/**
 * VolcanoPlannerPhaseRuleMappingInitializer describes an inteface for
 * initializing the mapping of {@link VolcanoPlannerPhase}s to sets of rule
 * descriptions.
 *
 * <p><b>Note:</b> Rule descriptions are obtained via {@link
 * org.eigenbase.relopt.RelOptRule#toString()}. By default they are the class's
 * simple name (e.g. class name sans package), unless the class is an inner
 * class, in which case the default is the inner class's simple name. Some rules
 * explicitly provide alternate descriptions by setting {@link
 * org.eigenbase.relopt.RelOptRule#description} directly.
 *
 * @author Stephan Zuercher
 */
public interface VolcanoPlannerPhaseRuleMappingInitializer
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Initializes a {@link VolcanoPlannerPhase}-to-rule map. Rules are
     * specified by description (see above). When this method is called, the map
     * will already be pre-initialized with empty sets for each
     * VolcanoPlannerPhase. Implementations must not return having added or
     * removed keys from the map, although it is safe to temporarily add or
     * remove keys.
     *
     * @param phaseRuleMap a {@link VolcanoPlannerPhase}-to-rule map
     */
    public void initialize(Map<VolcanoPlannerPhase, Set<String>> phaseRuleMap);
}

// End VolcanoPlannerPhaseRuleMappingInitializer.java
