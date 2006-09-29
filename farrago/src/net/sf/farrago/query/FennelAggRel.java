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

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.fennel.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;


/**
 * FennelAggRel represents the Fennel implementation of aggregation.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FennelAggRel
    extends AggregateRelBase
    implements FennelRel
{

    //~ Instance fields --------------------------------------------------------

    protected final FarragoRepos repos;

    //~ Constructors -----------------------------------------------------------

    public FennelAggRel(
        RelOptCluster cluster,
        RelNode child,
        int groupCount,
        Call [] aggCalls)
    {
        super(
            cluster,
            new RelTraitSet(FennelRel.FENNEL_EXEC_CONVENTION),
            child,
            groupCount,
            aggCalls);
        repos = FennelRelUtil.getRepos(this);
    }

    //~ Methods ----------------------------------------------------------------

    public FennelAggRel clone()
    {
        FennelAggRel clone =
            new FennelAggRel(
                getCluster(),
                getChild().clone(),
                groupCount,
                aggCalls);
        clone.inheritTraitsFrom(this);
        return clone;
    }

    // implement FennelRel
    public RelFieldCollation [] getCollations()
    {
        // TODO jvs 5-Oct-2005: if group keys are already sorted,
        // non-hash agg will preserve them; full-table agg is
        // trivially sorted (only one row of output)
        return RelFieldCollation.emptyCollationArray;
    }

    // implement FennelRel
    public Object implementFennelChild(FennelRelImplementor implementor)
    {
        return implementor.visitChild(
                this,
                0,
                getChild());
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FemSortedAggStreamDef aggStream = repos.newFemSortedAggStreamDef();
        defineAggStream(aggStream);
        implementor.addDataFlowFromProducerToConsumer(
            implementor.visitFennelChild((FennelRel) getChild()),
            aggStream);

        return aggStream;
    }

    protected void defineAggStream(FemAggStreamDef aggStream)
    {
        aggStream.setGroupingPrefixSize(groupCount);
        for (int i = 0; i < aggCalls.length; ++i) {
            Call call = aggCalls[i];
            assert (!call.isDistinct());

            // allow 0 for COUNT(*)
            assert (call.args.length <= 1);
            AggFunction func = lookupAggFunction(call);
            FemAggInvocation aggInvocation = repos.newFemAggInvocation();
            aggInvocation.setFunction(func);
            if (call.args.length == 1) {
                aggInvocation.setInputAttributeIndex(call.args[0]);
            } else {
                // COUNT(*) ignores input
                aggInvocation.setInputAttributeIndex(-1);
            }
            aggStream.getAggInvocation().add(aggInvocation);
        }
    }

    public static AggFunction lookupAggFunction(
        AggregateRel.Call call)
    {
        return
            AggFunctionEnum.forName(
                "AGG_FUNC_" + call.getAggregation().getName());
    }
}

// End FennelAggRel.java
