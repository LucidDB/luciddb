/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Copyright (C) 2005-2009 The Eigenbase Project
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

#ifndef Fennel_LcsClusterReplaceExecStream_Included
#define Fennel_LcsClusterReplaceExecStream_Included

#include "fennel/lucidera/colstore/LcsClusterAppendExecStream.h"
#include "fennel/segment/SnapshotRandomAllocationSegment.h"
#include "fennel/exec/DynamicParam.h"
#include "fennel/tuple/TupleProjectionAccessor.h"
#include "fennel/tuple/UnalignedAttributeAccessor.h"
#include "fennel/tuple/TupleDataWithBuffer.h"

FENNEL_BEGIN_NAMESPACE

struct LcsClusterReplaceExecStreamParams :
    public LcsClusterAppendExecStreamParams
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
