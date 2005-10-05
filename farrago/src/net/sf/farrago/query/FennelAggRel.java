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

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;

import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.catalog.*;

/**
 * FennelAggRel represents the Fennel implementation of aggregation.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FennelAggRel extends AggregateRelBase implements FennelRel
{
    public FennelAggRel(
        RelOptCluster cluster,
        RelNode child,
        int groupCount,
        Call [] aggCalls)
    {
        super(
            cluster, new RelTraitSet(FennelRel.FENNEL_EXEC_CONVENTION),
            child, groupCount, aggCalls);
    }

    public Object clone()
    {
        FennelAggRel clone = new FennelAggRel(
            getCluster(),
            RelOptUtil.clone(getChild()),
            groupCount,
            aggCalls);
        clone.inheritTraitsFrom(this);
        return clone;
    }

    // implement FennelRel
    public RelFieldCollation [] getCollations()
    {
        // TODO jvs 5-Oct-2005: if input is sorted, non-hash agg will preserve
        // it
        return RelFieldCollation.emptyCollationArray;
    }

    // implement FennelRel
    public Object implementFennelChild(FennelRelImplementor implementor)
    {
        return implementor.visitChild(this, 0, getChild());
    }
    
    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        final FarragoRepos repos = FennelRelUtil.getRepos(this);
        FemAggStreamDef aggStream = repos.newFemAggStreamDef();
        for (int i = 0; i < aggCalls.length; ++i) {
            Call call = aggCalls[i];
            assert(!call.isDistinct());
            assert(call.args.length == 1);
            AggFunction func = AggFunctionEnum.forName(
                call.getAggregation().getName());
            FemAggInvocation aggInvocation = repos.newFemAggInvocation();
            aggInvocation.setFunction(func);
            aggInvocation.setInputAttributeIndex(call.args[0]);
            aggStream.getAggInvocation().add(aggInvocation);
        }
        aggStream.getInput().add(
            implementor.visitFennelChild((FennelRel) getChild()));
        return aggStream;
    }
}

// End FennelAggRel.java
