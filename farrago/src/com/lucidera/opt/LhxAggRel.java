/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2006 LucidEra, Inc.
// Copyright (C) 2006-2006 The Eigenbase Project
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
package com.lucidera.opt;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;


/**
 * LhxAggRel represents hash aggregation.
 *
 * @author Rushan Chen
 * @version $Id$
 */
public class LhxAggRel
    extends AggregateRelBase
    implements FennelRel
{

    //~ Instance fields --------------------------------------------------------

    /**
     * row count of the input
     */
    long numInputRows;

    /**
     * cardinality of the group by key
     */
    long cndGroupByKey;

    //~ Constructors -----------------------------------------------------------

    public LhxAggRel(
        RelOptCluster cluster,
        RelNode child,
        int groupCount,
        Call [] aggCalls,
        long numInputRows,
        long cndGroupByKey)
    {
        super(
            cluster,
            new RelTraitSet(FennelRel.FENNEL_EXEC_CONVENTION),
            child,
            groupCount,
            aggCalls);
        this.numInputRows = numInputRows;
        this.cndGroupByKey = cndGroupByKey;
    }

    //~ Methods ----------------------------------------------------------------

    public Object clone()
    {
        LhxAggRel clone =
            new LhxAggRel(
                getCluster(),
                RelOptUtil.clone(getChild()),
                groupCount,
                aggCalls,
                numInputRows,
                cndGroupByKey);
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
        final FarragoRepos repos = FennelRelUtil.getRepos(this);
        FemLhxAggStreamDef aggStream = repos.newFemLhxAggStreamDef();
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

        aggStream.setNumRows(numInputRows);
        aggStream.setCndGroupByKeys(cndGroupByKey);

        implementor.addDataFlowFromProducerToConsumer(
            implementor.visitFennelChild((FennelRel) getChild()),
            aggStream);

        return aggStream;
    }

    public static AggFunction lookupAggFunction(
        AggregateRel.Call call)
    {
        return
            AggFunctionEnum.forName(
                "AGG_FUNC_" + call.getAggregation().getName());
    }
}

// End LhxAggRel.java
