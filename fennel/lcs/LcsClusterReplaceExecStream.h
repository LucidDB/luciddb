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

#ifndef Fennel_LcsClusterReplaceExecStream_Included
#define Fennel_LcsClusterReplaceExecStream_Included

#include "fennel/lcs/LcsClusterAppendExecStream.h"
#include "fennel/segment/SnapshotRandomAllocationSegment.h"
#include "fennel/exec/DynamicParam.h"
#include "fennel/tuple/TupleProjectionAccessor.h"
#include "fennel/tuple/UnalignedAttributeAccessor.h"
#include "fennel/tuple/TupleDataWithBuffer.h"

FENNEL_BEGIN_NAMESPACE

struct LcsClusterReplaceExecStreamParams
    : public LcsClusterAppendExecStreamParams
{
};

/**
 * Given a stream of tuples corresponding to the column values in a cluster,
 * creates a new cluster, replacing the pre-existing cluster with the new
 * input tuples.  Each tuple contains in its first column a rid value that
 * identifies which row will be replaced.  If there are gaps in the rid
 * sequence, then the row corresponding to that gap will be replaced with a
 * tuple that has the same values as the existing tuple in the original
 * cluster at that same rid position.
 *
 * <p>After processing all input, the rid to cluster pageId btree map
 * corresponding to the cluster is versioned off of the original btree's
 * rootPageId.  So, this execution stream requires the underlying segment
 * corresponding to the cluster to be a snapshot segment.
 */
class FENNEL_LCS_EXPORT LcsClusterReplaceExecStream
    : public LcsClusterAppendExecStream
{
    /**
     * Dynamic parameter id corresponding to the root pageId of the new cluster,
     * if the pageId is required downstream.  Set to 0 if there's no need for
     * the parameter.
     */
    DynamicParamId newClusterRootParamId;

    /**
     * Tuple descriptor representing the rid column plus the cluster columns
     * to be loaded
     */
    TupleDescriptor projInputTupleDesc;

    /**
     * Tuple data for the projected input tuple
     */
    TupleData projInputTupleData;

    /**
     * Accessors for loading column values from the original cluster
     */
    std::vector<UnalignedAttributeAccessor> attrAccessors;

    /**
     * The underlying snapshot segment for the cluster
     */
    SnapshotRandomAllocationSegment *pSnapshotSegment;

    /**
     * Reader for the original cluster
     */
    SharedLcsClusterReader pOrigClusterReader;

    /**
     * Number of rows in the original cluster
     */
    RecordNum origNumRows;

    /**
     * The rootPageId of the original rid to pageId btree map
     */
    PageId origRootPageId;

    /**
     * The current rid being loaded
     */
    LcsRid currLoadRid;

    /**
     * True if a new tuple needs to be provided for the load
     */
    bool needTuple;

    /**
     * The rid value of the last input row read
     */
    LcsRid currInputRid;

    /**
     * Accessor for projecting cluster tuple data from the input row
     */
    TupleProjectionAccessor clusterColsTupleAccessor;

    /**
     * TupleData used to load column values from the original cluster
     */
    TupleDataWithBuffer origClusterTupleData;

    /**
     * True if at least one existing row is being replaced with a new value
     */
    bool newData;

    /**
     * Initializes member fields corresponding to the data to be loaded,
     * taking into account the extra rid column that identifies each input
     * tuple.
     *
     * @param inputProj projection of the input tuple that's relevant to
     * this cluster append
     */
    virtual void initTupleLoadParams(const TupleProjection &inputProj);

    /**
     * Retrieves the tuple that will be loaded into the cluster.  The tuple
     * either originates from the input stream or contains the original
     * values at the current rid position being loaded.
     */
    virtual ExecStreamResult getTupleForLoad();

    /**
     * Performs post-processing after a tuple has been loaded.
     */
    virtual void postProcessTuple();

    /**
     * Reads the cluster columns for the current row being loaded from the
     * original cluster.
     */
    void readOrigClusterRow();

    virtual void close();

public:
    virtual void prepare(LcsClusterReplaceExecStreamParams const &params);
    virtual void open(bool restart);
    virtual void getResourceRequirements(
        ExecStreamResourceQuantity &minQuantity,
        ExecStreamResourceQuantity &optQuantity);
};


FENNEL_END_NAMESPACE

#endif

// End LcsClusterReplaceExecStream.h
