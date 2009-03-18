/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2006-2007 LucidEra, Inc.
// Copyright (C) 2006-2007 The Eigenbase Project
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

#ifndef Fennel_LbmIntersectExecStream_Included
#define Fennel_LbmIntersectExecStream_Included

#include "fennel/lucidera/bitmap/LbmBitOpExecStream.h"

FENNEL_BEGIN_NAMESPACE

/**
 * LbmIntersectExecStreamParams defines parameters for instantiating a
 * LbmIntersectExecStream
 */
struct LbmIntersectExecStreamParams : public LbmBitOpExecStreamParams
{
};

/**
 * LbmIntersectExecStream is the execution stream used to perform
 * intersection on two or more bitmap stream inputs
 *
 * @author Zelaine Fong
 * @version $Id$
 */
class LbmIntersectExecStream : public LbmBitOpExecStream
{
    /**
     * Number of inputs with overlapping rid values.  Must be equal to
     * nInputs for an intersection to take place.
     */
    uint nMatches;

    /**
     * Minimum length of overlapping bitmap segments found thus far
     */
    uint minLen;

    /**
     * Performs intersect operation on all segments
     *
     * @param len length of intersecting segments
     *
     * @return false if buffer overflow occurred writing out a segment
     */
    bool intersectSegments(uint len);

public:
    virtual void prepare(LbmIntersectExecStreamParams const &params);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
    virtual void closeImpl();
};

FENNEL_END_NAMESPACE

#endif

// End LbmIntersectExecStream.h
