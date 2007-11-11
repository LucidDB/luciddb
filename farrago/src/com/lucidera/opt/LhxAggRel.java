/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2007 LucidEra, Inc.
// Copyright (C) 2006-2007 The Eigenbase Project
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

import java.util.List;
import java.util.Arrays;


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

    /**
     * Creates a LhxAggRel.
     *
     * @param cluster Cluster
     * @param child Child
     * @param groupCount Size of grouping key
     * @param aggCalls Collection of calls to aggregate functions
     * @param numInputRows Row count of the input
     * @param cndGroupByKey Cardinality of the grouping key
     */
    public LhxAggRel(
        RelOptCluster cluster,
        RelNode child,
        int groupCount,
        List<AggregateCall> aggCalls,
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

    public LhxAggRel clone()
    {
        LhxAggRel clone =
            new LhxAggRel(
                getCluster(),
                getChild().clone(),
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
        FennelRelUtil.defineAggStream(aggCalls, groupCount, repos, aggStream);
        aggStream.setNumRows(numInputRows);
        aggStream.setCndGroupByKeys(cndGroupByKey);

        implementor.addDataFlowFromProducerToConsumer(
            implementor.visitFennelChild((FennelRel) getChild(), 0),
            aggStream);

        return aggStream;
    }
}

// End LhxAggRel.java
