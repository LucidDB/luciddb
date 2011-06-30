/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2006 Dynamo BI Corporation
// Portions Copyright (C) 2006 John V. Sichi
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
package org.eigenbase.rex;

import org.eigenbase.util.mapping.Mappings;


/**
 * Visitor which replaces {@link RexLocalRef} objects after the expressions in a
 * {@link RexProgram} have been reordered.
 *
 * @author jhyde
 * @version $Id$
 * @see RexPermuteInputsShuttle
 */
public class RexPermutationShuttle
    extends RexShuttle
{
    //~ Instance fields --------------------------------------------------------

    private final Mappings.TargetMapping mapping;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a RexPermutationShuttle.
     *
     * <p>The mapping provides at most one target for every source. If a source
     * has no targets and is referenced in the expression,
     * {@link org.eigenbase.util.mapping.Mappings.TargetMapping#getTarget(int)}
     * will give an error. Otherwise the mapping gives a unique target.
     *
     * @param mapping Mapping
     */
    public RexPermutationShuttle(Mappings.TargetMapping mapping)
    {
        this.mapping = mapping;
    }

    //~ Methods ----------------------------------------------------------------

    public RexNode visitLocalRef(RexLocalRef local)
    {
        final int index = local.getIndex();
        int target = mapping.getTarget(index);
        return new RexLocalRef(
            target,
            local.getType());
    }
}

// End RexPermutationShuttle.java
