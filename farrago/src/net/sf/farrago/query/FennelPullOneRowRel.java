/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
package net.sf.farrago.query;

import org.eigenbase.rel.AbstractRelNode;
import org.eigenbase.rel.RelFieldCollation;
import org.eigenbase.rel.OneRowRel;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.CallingConvention;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.oj.util.OJUtil;
import org.eigenbase.oj.rel.IterOneRowRel;
import net.sf.farrago.fem.fennel.FemExecutionStreamDef;
import net.sf.farrago.fem.fennel.FemMockTupleStreamDef;
import net.sf.farrago.fem.fennel.FemTupleDescriptor;
import net.sf.farrago.catalog.FarragoRepos;
import openjava.mop.OJClass;
import openjava.ptree.*;

import java.util.Collections;

/**
 * <code>OneRowRel</code> always returns one row, one column (containing
 * the value 0).
 *
 * @author Wael Chatila
 * @since Feb 4, 2005
 * @version $Id$
 */
public class FennelPullOneRowRel extends AbstractRelNode implements FennelPullRel
{
    public FennelPullOneRowRel(RelOptCluster cluster)
    {
        super(cluster, new RelTraitSet(FENNEL_PULL_CONVENTION));
    }

    // override Object
    public Object clone()
    {
        FennelPullOneRowRel clone = new FennelPullOneRowRel(getCluster());
        clone.inheritTraitsFrom(this);
        return clone;
    }

    protected RelDataType deriveRowType()
    {
        return OneRowRel.deriveOneRowType(getCluster().getTypeFactory());
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        return planner.makeTinyCost();
    }

    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FarragoRepos repos = FennelRelUtil.getRepos(this);
        FemMockTupleStreamDef streamDef =
            repos.newFemMockTupleStreamDef();

        streamDef.setRowCount(1);
        return streamDef;
    }

    public Object implementFennelChild(FennelRelImplementor implementor)
    {
        return Literal.constantNull();
    }

    public RelFieldCollation[] getCollations()
    {
        return RelFieldCollation.emptyCollationArray;
    }
}
