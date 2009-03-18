/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 1999-2007 John V. Sichi
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

#ifndef Fennel_SegmentMap_Included
#define Fennel_SegmentMap_Included

FENNEL_BEGIN_NAMESPACE

/**
 * SegmentMap defines an interface for mapping a SegmentId to a loaded Segment
 * instance.
 */
class SegmentMap
{
public:
    virtual ~SegmentMap() {};

    /**
     * Finds a segment by its SegmentId.
     *
     * @param segmentId the SegmentId to find
     *
     * @param pDataSegment the specific segment associated with a statement,
     * if a specific segment must be used
     *
     * @return loaded segment, or a singular SharedSegment if not found
     */
    virtual SharedSegment getSegmentById(
        SegmentId segmentId,
        SharedSegment pDataSegment) = 0;
};

FENNEL_END_NAMESPACE

#endif

// End SegmentMap.h
