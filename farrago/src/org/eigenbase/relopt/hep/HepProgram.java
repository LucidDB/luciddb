/*
// $Id$
// Package org.eigenbase is a class library of data management components.
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
package org.eigenbase.relopt.hep;

import java.util.*;


/**
 * HepProgram specifies the order in which rules should be attempted by {@link
 * HepPlanner}. Use {@link HepProgramBuilder} to create a new instance of
 * HepProgram.
 *
 * <p>Note that the structure of a program is immutable, but the planner uses it
 * as read/write during planning, so a program can only be in use by a single
 * planner at a time.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class HepProgram
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * Symbolic constant for matching until no more matches occur.
     */
    public static final int MATCH_UNTIL_FIXPOINT = Integer.MAX_VALUE;

    //~ Instance fields --------------------------------------------------------

    final List<HepInstruction> instructions;

    int matchLimit;

    HepMatchOrder matchOrder;

    HepInstruction.EndGroup group;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new empty HepProgram. The program has an initial match order of
     * {@link org.eigenbase.relopt.hep.HepMatchOrder#ARBITRARY}, and an initial
     * match limit of {@link #MATCH_UNTIL_FIXPOINT}.
     */
    HepProgram(List<HepInstruction> instructions)
    {
        this.instructions = instructions;
    }

    //~ Methods ----------------------------------------------------------------

    void initialize(boolean clearCache)
    {
        matchLimit = MATCH_UNTIL_FIXPOINT;
        matchOrder = HepMatchOrder.ARBITRARY;
        group = null;

        for (HepInstruction instruction : instructions) {
            instruction.initialize(clearCache);
        }
    }
}

// End HepProgram.java
