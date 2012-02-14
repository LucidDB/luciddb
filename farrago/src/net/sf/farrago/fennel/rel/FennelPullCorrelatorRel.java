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

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;


/**
 * FennelPullCorrelatorRel is the relational expression corresponding to a
 * correlation between two streams implemented inside of Fennel.
 *
 * @author Wael Chatila
 * @version $Id$
 * @since Feb 1, 2005
 */
public class FennelPullCorrelatorRel
    extends FennelDoubleRel
{
    //~ Instance fields --------------------------------------------------------

    protected final List<CorrelatorRel.Correlation> correlations;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a Correlator.
     *
     * @param cluster {@link RelOptCluster}  this relational expression belongs
     * to
     * @param left left input relational expression
     * @param right right input relational expression
     * @param correlations set of expressions to set as variables each time a
     * row arrives from the left input
     */
    public FennelPullCorrelatorRel(
        RelOptCluster cluster,
        RelNode left,
        RelNode right,
        List<CorrelatorRel.Correlation> correlations)
    {
        super(cluster, left, right);
        this.correlations = correlations;
    }

    //~ Methods ----------------------------------------------------------------

    // implement Cloneable
    public FennelPullCorrelatorRel clone()
    {
        FennelPullCorrelatorRel clone =
            new FennelPullCorrelatorRel(
                getCluster(),
                left.clone(),
                right.clone(),
                new ArrayList<CorrelatorRel.Correlation>(correlations));
        clone.inheritTraitsFrom(this);
        return clone;
    }

    // override RelNode
    public void explain(RelOptPlanWriter pw)
    {
        pw.explain(
            this,
            new String[] { "left", "right" },
            new Object[] {});
    }

    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        double rowCount = RelMetadataQuery.getRowCount(this);
        return planner.makeCost(
            rowCount,
            0,
            rowCount * getRowType().getFieldList().size());
    }

    // implement RelNode
    public double getRows()
    {
        return left.getRows() * right.getRows();
    }

    protected RelDataType deriveRowType()
    {
        return JoinRel.deriveJoinRowType(
            left.getRowType(),
            right.getRowType(),
            JoinRelType.INNER,
            getCluster().getTypeFactory(),
            null,
            Collections.<RelDataTypeField>emptyList());
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FarragoRepos repos = FennelRelUtil.getRepos(this);
        FemCorrelationJoinStreamDef streamDef =
            repos.newFemCorrelationJoinStreamDef();

        for (CorrelatorRel.Correlation correlation : correlations) {
            FemCorrelation newFemCorrelation = repos.newFemCorrelation();
            newFemCorrelation.setId(correlation.getId());
            newFemCorrelation.setOffset(correlation.getOffset());
            streamDef.getCorrelations().add(newFemCorrelation);

            // Tell the graph which variable(s) we write.
            final FemDynamicParamUse dynamicParamUse =
                repos.newFemDynamicParamUse();
            dynamicParamUse.setDynamicParamId(correlation.getId());
            dynamicParamUse.setRead(false);
            streamDef.getDynamicParamUse().add(dynamicParamUse);
        }

        FemExecutionStreamDef leftInput =
            implementor.visitFennelChild((FennelRel) left, 0);
        implementor.addDataFlowFromProducerToConsumer(
            leftInput,
            streamDef);
        FemExecutionStreamDef rightInput =
            implementor.visitFennelChild((FennelRel) right, 1);
        implementor.addDataFlowFromProducerToConsumer(
            rightInput,
            streamDef);

        return streamDef;
    }
}

// End FennelPullCorrelatorRel.java
