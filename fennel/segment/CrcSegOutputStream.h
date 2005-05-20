/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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

#ifndef Fennel_CrcSegOutputStream_Included
#define Fennel_CrcSegOutputStream_Included

#include "fennel/segment/SegOutputStream.h"

#include <boost/crc.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * CrcSegOutputStream extends SegOutputStream by stamping each page with a
 * CRC/PseudoUuid/PageId combination.  This can be used to reliably
 * detect end-of-stream during recovery.
 */
class CrcSegOutputStream : public SegOutputStream
{
    PseudoUuid onlineUuid;
    
    // TODO:  use a 64-bit crc instead
    boost::crc_32_type crcComputer;
    
    virtual void writeExtraHeaders(SegStreamNode &node);
    
    explicit CrcSegOutputStream(
        SegmentAccessor const &segmentAccessor,
        PseudoUuid onlineUuid);
    
public:
    /**
     * Creates a new CrcSegOutputStream.
     *
     * @param segmentAccessor accessor for the segment in which to store the
     * data
     *
     * @param onlineUuid PseudoUuid with which to stamp each page
     *
     * @return shared_ptr to new SegOutputStream
     */
    static SharedSegOutputStream newCrcSegOutputStream(
        SegmentAccessor const &segmentAccessor,
        PseudoUuid onlineUuid);
};

FENNEL_END_NAMESPACE

#endif

// End CrcSegOutputStream.h
