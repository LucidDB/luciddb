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

#ifndef Fennel_LbmUnionWorkspace_Included
#define Fennel_LbmUnionWorkspace_Included

#include "fennel/common/ByteBuffer.h"
#include "fennel/lucidera/bitmap/LbmByteSegment.h"
#include "fennel/lucidera/bitmap/LbmSegment.h"

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
