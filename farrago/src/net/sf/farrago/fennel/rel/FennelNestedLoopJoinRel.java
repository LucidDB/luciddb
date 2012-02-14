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
import net.sf.farrago.fennel.*;
import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.util.*;


/**
 * FennelNestedLoopJoinRel represents the Fennel implementation of a nested loop
 * join.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class FennelNestedLoopJoinRel
    extends FennelMultipleRel
{
    //~ Instance fields --------------------------------------------------------

    private final JoinRelType joinType;

    private final Integer [] leftJoinKeys;

    private final FennelRelParamId [] joinKeyParamIds;

    private final FennelRelParamId rootPageIdParamId;

    private final double rowCount;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FennelNestedLoopJoinRel object.
     *
     * @param cluster RelOptCluster for this rel
     * @param inputs inputs into the nested loop join
     * @param joinType type of join
     * @param fieldNameList if not null, the row type will have these field
     * names
     * @param leftJoinKeys column offsets corresponding to join keys from LHS
     * input
     * @param joinKeyParamIds dynamic parameters corresponding to LHS join keys
     * @param rootPageIdParamId dynamic parameter corresponding to the root of
     * the temp index
     * @param rowCount rowCount returned from the join
     */
    public FennelNestedLoopJoinRel(
        RelOptCluster cluster,
        RelNode [] inputs,
        JoinRelType joinType,
        List<String> fieldNameList,
        Integer [] leftJoinKeys,
        FennelRelParamId [] joinKeyParamIds,
        FennelRelParamId rootPageIdParamId,
        double rowCount)
    {
        super(cluster, inputs);
        assert joinType != null;
        this.joinType = joinType;
        this.leftJoinKeys = leftJoinKeys;
        this.joinKeyParamIds = joinKeyParamIds;
        this.rootPageIdParamId = rootPageIdParamId;
        this.rowCount = rowCount;
        this.rowType =
            JoinRel.deriveJoinRowType(
                inputs[0].getRowType(),
                inputs[1].getRowType(),
                joinType,
                cluster.getTypeFactory(),
                fieldNameList,
                Collections.<RelDataTypeField>emptyList());
    }

    //~ Methods ----------------------------------------------------------------

    // implement Cloneable
    public FennelNestedLoopJoinRel clone()
    {
        FennelNestedLoopJoinRel clone =
            new FennelNestedLoopJoinRel(
                getCluster(),
                inputs.clone(),
                joinType,
                RelOptUtil.getFieldNameList(rowType),
                leftJoinKeys.clone(),
                joinKeyParamIds.clone(),
                rootPageIdParamId,
                rowCount);
        clone.inheritTraitsFrom(this);
        return clone;
    }

    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        // TODO:  provide more realistic costing; currently using a formula
        // similar to LhxJoinRel but with a higher join selectivity
        double joinSelectivity = 0.2;
        return planner.makeCost(
            rowCount,
            0,
            rowCount * getRowType().getFieldList().size()
            * joinSelectivity);
    }

    // implement RelNode
    public double getRows()
    {
        return rowCount;
    }

    // override RelNode
    public void explain(RelOptPlanWriter pw)
    {
        int nInputs = getInputs().length;
        String [] nameList = new String[nInputs + 3];
        Object [] objects = new Object[3];
        for (int i = 0; i < nInputs; i++) {
            nameList[i] = "child";
        }
        nameList[nInputs] = "joinType";
        nameList[nInputs + 1] = "leftJoinKeys";
        nameList[nInputs + 2] = "joinKeyParamIds";
        objects[0] = joinType;
        objects[1] = Arrays.asList(leftJoinKeys);
        objects[2] = Arrays.asList(joinKeyParamIds);
        pw.explain(this, nameList, objects);
    }

    // implement RelNode
    protected RelDataType deriveRowType()
    {
        throw Util.newInternal("row type should have been set already");
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FarragoRepos repos = FennelRelUtil.getRepos(this);
        FemNestedLoopJoinStreamDef streamDef =
            repos.newFemNestedLoopJoinStreamDef();

        // Translate all of the dynamic parameters upfront so the translated
        // objects are available to the nested loop join inputs.  Although
        // this stream doesn't reference the dynamic parameter corresponding
        // to the rootPageId of the temporary index, we need to also
        // translate that upfront.
        for (int i = 0; i < joinKeyParamIds.length; i++) {
            FemCorrelation joinKeyParam = repos.newFemCorrelation();
            joinKeyParam.setId(
                implementor.translateParamId(joinKeyParamIds[i]).intValue());
            joinKeyParam.setOffset(leftJoinKeys[i]);
            streamDef.getLeftJoinKey().add(joinKeyParam);
        }
        FennelDynamicParamId dynRootPageIdParamId =
            implementor.translateParamId(rootPageIdParamId);

        RelNode [] inputs = getInputs();
        for (int i = 0; i < inputs.length; i++) {
            implementor.addDataFlowFromProducerToConsumer(
                implementor.visitFennelChild((FennelRel) inputs[i], i),
                streamDef);
        }

        if (inputs.length == 3) {
            // Add an implicit dataflow from the stream that creates the
            // temporary index to the stream that searches the index.  This
            // way, the dynamic parameter used to pass the rootPageId of the
            // temporary index will be created by the index write stream
            // before it's accessed by the index search stream.  Note that
            // when doing translation on that parameter in those streams,
            // we associated the producer and consumer with the parameter,
            // so that's how we can now add that implicit dataflow.
            implementor.addDataFlowFromProducerToConsumer(
                dynRootPageIdParamId.getProducerStream(),
                dynRootPageIdParamId.getConsumerStream(),
                true);
        }

        streamDef.setLeftOuter(joinType == JoinRelType.LEFT);

        streamDef.setOutputDesc(
            FennelRelUtil.createTupleDescriptorFromRowType(
                repos,
                getCluster().getTypeFactory(),
                this.getRowType()));

        return streamDef;
    }

    // implement RelNode
    public Object implementFennelChild(FennelRelImplementor implementor)
    {
        // Translate parameters upfront so they're available to the children
        // inputs
        for (int i = 0; i < joinKeyParamIds.length; i++) {
            implementor.translateParamId(joinKeyParamIds[i]);
        }
        implementor.translateParamId(rootPageIdParamId);

        return super.implementFennelChild(implementor);
    }
}

// End FennelNestedLoopJoinRel.java
