/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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

package org.eigenbase.rel;

import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexProgram;

/**
 * A relational expression which computes project expressions and also filters.
 *
 * <p>This relational expression combines the functionality of
 * {@link ProjectRel} and {@link FilterRel}. It should be created in the
 * latter stages of optimization, by merging consecutive {@link ProjectRel}
 * and {@link FilterRel} nodes together.
 *
 * <p>The following rules relate to <code>CalcRel</code>:<ul>
 * <li>{@link FilterToCalcRule} creates this from a {@link FilterRel}</li>
 * <li>{@link ProjectToCalcRule} creates this from a {@link FilterRel}</li>
 * <li>{@link MergeFilterOntoCalcRule} merges this with a
 *     {@link FilterRel}</li>
 * <li>{@link MergeProjectOntoCalcRule} merges this with a
 *     {@link ProjectRel}</li>
 * </ul></p>
 *
 * @author jhyde
 * @since Mar 7, 2004
 * @version $Id$
 **/
public final class CalcRel extends CalcRelBase
{
    //~ Constructors ----------------------------------------------------------

    public CalcRel(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelNode child,
        RelDataType rowType,
        RexProgram program)
    {
        super(cluster, traits, child, rowType, program);
    }

    //~ Methods ---------------------------------------------------------------

    public Object clone()
    {
        return new CalcRel(
            getCluster(), cloneTraits(), getChild(), rowType,
            program.copy());
    }
}

// End CalcRel.java
