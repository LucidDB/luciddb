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
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;


/**
 * FennelTempIdxSearchRel searches a temporary index that is built during
 * runtime using search keys read through dynamic parameters and search
 * directives passed in through its input.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
class FennelTempIdxSearchRel
    extends FennelSingleRel
{
    //~ Instance fields --------------------------------------------------------

    final RelNode sourceRel;
    final Integer [] indexKeys;
    final Integer [] inputKeyProj;
    final Integer [] inputDirectiveProj;
    final FennelRelParamId [] searchKeyParamIds;
    final Integer [] keyOffsets;
    final FennelRelParamId rootPageIdParamId;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FennelTempIdxSearchRel object.
     *
     * @param sourceRel underlying RelNode that will be used to populate the
     * temporary index to be searched
     * @param indexKeys the projection from the sourceRel corresponding to the
     * temporary index keys
     * @param child input which produces the search directives and specifies the
     * types of the search keys
     * @param inputKeyProj the projection of input fields corresponding to the
     * search keys
     * @param inputDirectiveProj the projection of input fields specifying the
     * search directives
     * @param searchKeyParamIds dynamic parameter ids corresponding to the
     * search keys
     * @param keyOffsets offset within the search key that each dynamic
     * parameter corresponds to
     * @param rootPageIdParamId dynamic parameter id that will be used to pass
     * along the rootPageId of the temporary index to be searched
     */
    public FennelTempIdxSearchRel(
        RelNode sourceRel,
        Integer [] indexKeys,
        RelNode child,
        Integer [] inputKeyProj,
        Integer [] inputDirectiveProj,
        FennelRelParamId [] searchKeyParamIds,
        Integer [] keyOffsets,
        FennelRelParamId rootPageIdParamId)
    {
        super(
            sourceRel.getCluster(),
            child);
        this.sourceRel = sourceRel;
        this.indexKeys = indexKeys;
        this.inputKeyProj = inputKeyProj;
        this.inputDirectiveProj = inputDirectiveProj;
        this.searchKeyParamIds = searchKeyParamIds;
        this.keyOffsets = keyOffsets;
        this.rootPageIdParamId = rootPageIdParamId;
    }

    //~ Methods ----------------------------------------------------------------

    // override Rel
    public double getRows()
    {
        // TODO: factor in search key directives
        return RelMetadataQuery.getRowCount(sourceRel);
    }

    // implement Cloneable
    public FennelTempIdxSearchRel clone()
    {
        FennelTempIdxSearchRel clone =
            new FennelTempIdxSearchRel(
                sourceRel.clone(),
                indexKeys,
                getChild().clone(),
                inputKeyProj,
                inputDirectiveProj,
                searchKeyParamIds,
                keyOffsets,
                rootPageIdParamId);
        clone.inheritTraitsFrom(this);
        return clone;
    }

    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        double dRows = getRows();

        // CPU cost is proportional to number of columns projected
        // I/O cost is proportional to pages of index scanned
        double dCpu = dRows * getRowType().getFieldList().size();
        double dIo = dRows * inputKeyProj.length / 2;

        return planner.makeCost(dRows, dCpu, dIo);
    }

    // implement RelNode
    public RelDataType deriveRowType()
    {
        return sourceRel.getRowType();
    }

    // implement RelNode
    public void explain(RelOptPlanWriter pw)
    {
        pw.explain(
            this,
            new String[] {
                "child",
                "indexKeys",
                "inputKeyProj",
                "inputDirectiveProj",
                "searchKeyParamIds",
                "keyOffsets",
                "rootPageIdParamId"
            },
            new Object[] {
                Arrays.asList(indexKeys),
                Arrays.asList(inputKeyProj),
                Arrays.asList(inputDirectiveProj),
                Arrays.asList(searchKeyParamIds),
                Arrays.asList(keyOffsets),
                rootPageIdParamId
            });
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FarragoRepos repos = FennelRelUtil.getRepos(this);

        FemIndexSearchDef searchStream = repos.newFemIndexSearchDef();

        searchStream.setUniqueKey(false);
        searchStream.setOuterJoin(false);
        searchStream.setPrefetch(true);
        searchStream.setInputKeyProj(
            FennelRelUtil.createTupleProjection(repos, inputKeyProj));
        searchStream.setInputDirectiveProj(
            FennelRelUtil.createTupleProjection(repos, inputDirectiveProj));

        // Indicate that the rootPageId parameter is consumed by this stream
        searchStream.setRootPageIdParamId(
            implementor.translateParamId(
                rootPageIdParamId,
                searchStream,
                FennelDynamicParamId.StreamType.CONSUMER).intValue());
        searchStream.setRootPageId(-1);

        // Even though this stream reads data, it needs to be able to see
        // the data that was created by the same stream graph.
        searchStream.setReadOnlyCommittedData(false);

        RelDataType sourceRowType = sourceRel.getRowType();
        FemTupleDescriptor tupleDesc =
            FennelRelUtil.createTupleDescriptorFromRowType(
                repos,
                getCluster().getTypeFactory(),
                sourceRowType);
        searchStream.setTupleDesc(tupleDesc);
        searchStream.setKeyProj(
            FennelRelUtil.createTupleProjection(repos, indexKeys));

        Integer [] outputProj =
            FennelRelUtil.newIotaProjection(sourceRowType.getFieldCount());
        searchStream.setOutputProj(
            FennelRelUtil.createTupleProjection(repos, outputProj));
        FemTupleDescriptor outputDesc =
            FennelRelUtil.createTupleDescriptorFromRowType(
                repos,
                getCluster().getTypeFactory(),
                sourceRowType);
        searchStream.setOutputDesc(outputDesc);

        for (int i = 0; i < searchKeyParamIds.length; i++) {
            FemCorrelation searchKeyParam = repos.newFemCorrelation();
            searchKeyParam.setId(
                implementor.translateParamId(searchKeyParamIds[i]).intValue());
            searchKeyParam.setOffset(keyOffsets[i]);
            searchStream.getSearchKeyParameter().add(searchKeyParam);
        }

        implementor.addDataFlowFromProducerToConsumer(
            implementor.visitFennelChild((FennelRel) getChild(), 0),
            searchStream);

        return searchStream;
    }
}

// End FennelTempIdxSearchRel.java
