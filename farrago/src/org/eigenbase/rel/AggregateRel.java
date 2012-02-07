/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/
package org.eigenbase.rel;

import java.util.*;

import org.eigenbase.relopt.*;


/**
 * <code>AggregateRel</code> is a relational operator which eliminates
 * duplicates and computes totals.
 *
 * <p>Rules:
 *
 * <ul>
 * <li>{@link org.eigenbase.rel.rules.PullConstantsThroughAggregatesRule}
 * <li>{@link org.eigenbase.rel.rules.RemoveDistinctAggregateRule}
 * <li>{@link org.eigenbase.rel.rules.ReduceAggregatesRule}.
 *
 * @author jhyde
 * @version $Id$
 * @since 3 February, 2002
 */
public final class AggregateRel
    extends AggregateRelBase
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates an AggregateRel.
     *
     * @param cluster {@link RelOptCluster}  this relational expression belongs
     * to
     * @param child input relational expression
     * @param groupCount Number of columns to group on
     * @param aggCalls Array of aggregates to compute
     *
     * @pre aggCalls != null
     */
    public AggregateRel(
        RelOptCluster cluster,
        RelNode child,
        int groupCount,
        List<AggregateCall> aggCalls)
    {
        super(
            cluster,
            new RelTraitSet(CallingConvention.NONE),
            child,
            groupCount,
            aggCalls);
    }

    //~ Methods ----------------------------------------------------------------

    public AggregateRel clone()
    {
        AggregateRel clone =
            new AggregateRel(
                getCluster(),
                getChild().clone(),
                groupCount,
                aggCalls);
        clone.inheritTraitsFrom(this);
        return clone;
    }
}

// End AggregateRel.java
