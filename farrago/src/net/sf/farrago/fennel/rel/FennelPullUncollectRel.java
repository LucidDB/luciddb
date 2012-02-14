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
package net.sf.farrago.fennel.rel;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;


/**
 * FennelPullUncollectRel is the relational expression corresponding to an
 * UNNEST (Uncollect) implemented inside of Fennel.
 *
 * <p>Rules:
 *
 * <ul>
 * <li>{@link FennelUncollectRule} creates this from a rex call which has the
 * operator {@link
 * org.eigenbase.sql.fun.SqlStdOperatorTable#unnestOperator}</li>
 * </ul>
 * </p>
 *
 * @author Wael Chatila
 * @version $Id$
 * @since Dec 12, 2004
 */
public class FennelPullUncollectRel
    extends FennelSingleRel
{
    //~ Constructors -----------------------------------------------------------

    public FennelPullUncollectRel(RelOptCluster cluster, RelNode child)
    {
        super(
            cluster,
            new RelTraitSet(FENNEL_EXEC_CONVENTION),
            child);
        assert deriveRowType() != null : "invalid child rowtype";
    }

    //~ Methods ----------------------------------------------------------------

    protected RelDataType deriveRowType()
    {
        return UncollectRel.deriveUncollectRowType(getChild());
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        return planner.makeTinyCost();
    }

    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        final FarragoRepos repos = FennelRelUtil.getRepos(this);
        FemUncollectTupleStreamDef uncollectStream =
            repos.newFemUncollectTupleStreamDef();

        implementor.addDataFlowFromProducerToConsumer(
            implementor.visitFennelChild((FennelRel) getChild(), 0),
            uncollectStream);

        return uncollectStream;
    }

    // override Object (public, does not throw CloneNotSupportedException)
    public FennelPullUncollectRel clone()
    {
        FennelPullUncollectRel clone =
            new FennelPullUncollectRel(
                getCluster(),
                getChild().clone());
        clone.inheritTraitsFrom(this);
        return clone;
    }
}

// End FennelPullUncollectRel.java
