/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
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

#ifndef Fennel_CrcSegInputStream_Included
#define Fennel_CrcSegInputStream_Included

#include "fennel/segment/SegInputStream.h"

#include <boost/crc.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * CrcSegInputStream extends SegInputStream by verifying checksum
 * information on each page read.  An invalid page is interpreted as end of
 * stream.
 */
class CrcSegInputStream : public SegInputStream
{
    PseudoUuid onlineUuid;
    
    // TODO:  use a 64-bit crc instead
    boost::crc_32_type crcComputer;
    
    explicit CrcSegInputStream(
        SegmentAccessor const &segmentAccessor,
        PseudoUuid onlineUuid,
        PageId beginPageId);
    
    inline bool lockBufferParanoid();
    
    virtual void lockBuffer();
    
public:
    /**
     * Creates a new CrcSegInputStream.
     *
     * @param segmentAccessor accessor for the segment containing the stream
     * data
     *
     * @param onlineUuid PseudoUuid which each page should match
     *
     * @param beginPageId the first page of stream data; if the default
     * FIRST_LINEAR_PAGE_ID is passed, the segment must support
     * LINEAR_ALLOCATION, and the stream starts at the first page of the
     * segment; if NULL_PAGE_ID is passed, an empty stream is returned
     *
     * @return shared_ptr to new SegInputStream
     */
    static SharedSegInputStream newCrcSegInputStream(
        SegmentAccessor const &segmentAccessor,
        PseudoUuid onlineUuid,
        PageId beginPageId = FIRST_LINEAR_PAGE_ID);
};

FENNEL_END_NAMESPACE

#endif

// End CrcSegInputStream.h
