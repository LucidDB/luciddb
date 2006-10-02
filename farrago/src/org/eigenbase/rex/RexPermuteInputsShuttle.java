/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
// Portions Copyright (C) 2006-2006 John V. Sichi
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

import org.eigenbase.util.Permutation;

/**
 * Shuttle which applies a permutation to its input fields.
 *
 * @see RexPermutationShuttle
 *
 * @author jhyde
 * @version $Id$
 */
public class RexPermuteInputsShuttle
    extends RexShuttle
{
    private final Permutation permutation;

    public RexPermuteInputsShuttle(Permutation permutation)
    {
        this.permutation = permutation;
    }

    public RexNode visitInputRef(RexInputRef local)
    {
        final int index = local.getIndex();
        int target = permutation.getTarget(index);
        return new RexInputRef(
                target,
                local.getType());
    }
}

// End RexPermuteInputsShuttle.java
