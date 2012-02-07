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

#ifndef Fennel_LbmUnionWorkspace_Included
#define Fennel_LbmUnionWorkspace_Included

#include "fennel/common/ByteBuffer.h"
#include "fennel/lbm/LbmByteSegment.h"
#include "fennel/lbm/LbmSegment.h"

FENNEL_BEGIN_NAMESPACE

typedef ByteWindow<LbmByteNumberPrimitive> LbmUnionMergeArea;

/**
 * The union workspace merges byte segments
 *
 * @author John Pham
 * @version $Id$
 */
class FENNEL_LBM_EXPORT LbmUnionWorkspace
{
    /**
     * Buffer used to merge segments, indexed by ByteNumber
     */
    LbmUnionMergeArea mergeArea;

    /**
     * Maximum size of a segment that can be produced by this workspace
     */
    uint maxSegmentSize;

    /**
     * Whether there is a limit on production
     */
    bool limited;

    /**
     * Byte number of the last byte which can be produced
     */
    LbmByteNumber productionLimitByte;

    /**
     * A segment that can be returned by the workspace
     */
    LbmByteSegment segment;

    /**
     * Advance the workspace to the requested byte number
     */
    void advanceToByteNum(LbmByteNumber requestedByteNum);

public:
    /**
     * Initialize the workspace
     */
    void init(SharedByteBuffer pBuffer, uint maxSegmentSize);

    /**
     * Empty the workspace
     */
    void reset();

    /**
     * Advance the workspace to the requested Srid
     */
    void advanceToSrid(LcsRid requestedSrid);

    /**
     * Advance the workspace past the current workspace segment
     * Precondition is that segment must be set.
     */
    void advancePastSegment();

    /**
     * Increases the upper bound of production. This allows the workspace
     * to produce byte segments up to (but not including) the byte
     * containing productionLimitRid. The workspace will not allow a
     * segment to be added within the bounds of production
     */
    void setProductionLimit(LcsRid productionLimitRid);

    /**
     * Remove production limit; this allows the workspace to flush
     * its entire contents; no more segments can be added after this
     * call
     */
    void removeLimit();

    /**
     * Whether the workspace is completely empty
     */
    bool isEmpty() const;

    /**
     * Whether the workspace is able to produce a segment
     */
    bool canProduce();

    /**
     * Returns the current segment
     */
    const LbmByteSegment &getSegment();

    /**
     * Returns the current contiguous segment
     */
    const LbmByteSegment &getContiguousSegment();

    /**
     * Adds a segment to the workspace; the segment must not fall within
     * the current bounds of production
     */
    bool addSegment(const LbmByteSegment &segment);
};

FENNEL_END_NAMESPACE

#endif

// End LbmUnionWorkspace.h
