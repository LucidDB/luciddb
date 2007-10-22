/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
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

import java.util.List;
import java.util.Arrays;


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

    /**
     * @deprecated todo: remove - not used in green or red DT code
     */
    public FennelAggRel(
        RelOptCluster cluster,
        RelNode child,
        int groupCount,
        AggregateCall[] aggCalls)
    {
        this(
            cluster,
            child,
            groupCount,
            Arrays.asList(aggCalls));
    }

    /**
     * Creates a FennelAggRel.
     *
     * @param cluster Cluster
     * @param child Child
     * @param groupCount Size of grouping key
     * @param aggCalls Collection of calls to aggregate functions
     */
    public FennelAggRel(
        RelOptCluster cluster,
        RelNode child,
        int groupCount,
        List<AggregateCall> aggCalls)
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
        FennelRelUtil.defineAggStream(aggCalls, groupCount, repos, aggStream);
        implementor.addDataFlowFromProducerToConsumer(
            implementor.visitFennelChild((FennelRel) getChild(), 0),
            aggStream);

        return aggStream;
    }

}

// End FennelAggRel.java
