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
import org.eigenbase.sql.type.*;


/**
 * FennelPullCollectRel is the relational expression corresponding to a collect
 * implemented inside of Fennel.
 *
 * <p>Rules:
 *
 * <ul>
 * <li>{@link FennelCollectRule} creates this from a {@link CollectRel}.</li>
 * </ul>
 * </p>
 *
 * @author Wael Chatila
 * @version $Id$
 * @since Dec 11, 2004
 */
public class FennelPullCollectRel
    extends FennelSingleRel
{
    //~ Instance fields --------------------------------------------------------

    final String fieldName;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a FennelPullCollectRel.
     *
     * @param cluster Cluster
     * @param child Child relational expression
     * @param fieldName Name of the sole output field
     */
    public FennelPullCollectRel(
        RelOptCluster cluster,
        RelNode child,
        String fieldName)
    {
        super(
            cluster,
            new RelTraitSet(FENNEL_EXEC_CONVENTION),
            child);
        this.fieldName = fieldName;
    }

    //~ Methods ----------------------------------------------------------------

    protected RelDataType deriveRowType()
    {
        return CollectRel.deriveCollectRowType(this, fieldName);
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        return planner.makeTinyCost();
    }

    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        final FarragoRepos repos = FennelRelUtil.getRepos(this);
        FemCollectTupleStreamDef collectStreamDef =
            repos.newFemCollectTupleStreamDef();

        implementor.addDataFlowFromProducerToConsumer(
            implementor.visitFennelChild((FennelRel) getChild(), 0),
            collectStreamDef);

        // The column containing the packaged multiset is always VARCHAR(4096)
        // NOT NULL. Even an empty multiset is not represented as NULL.
        FemTupleDescriptor outTupleDesc = repos.newFemTupleDescriptor();
        RelDataType type =
            getCluster().getTypeFactory().createSqlType(
                SqlTypeName.VARBINARY,
                4096);
        FennelRelUtil.addTupleAttrDescriptor(repos, outTupleDesc, type);
        collectStreamDef.setOutputDesc(outTupleDesc);
        return collectStreamDef;
    }

    // override Object (public, does not throw CloneNotSupportedException)
    public FennelPullCollectRel clone()
    {
        FennelPullCollectRel clone =
            new FennelPullCollectRel(
                getCluster(),
                getChild().clone(),
                fieldName);
        clone.inheritTraitsFrom(this);
        return clone;
    }
}

// End FennelPullCollectRel.java
