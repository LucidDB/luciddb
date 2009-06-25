/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 1999-2009 John V. Sichi
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

#include "fennel/common/CommonPreamble.h"
#include "fennel/segment/CrcSegOutputStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

SharedSegOutputStream CrcSegOutputStream::newCrcSegOutputStream(
    SegmentAccessor const &segmentAccessor,
    PseudoUuid onlineUuid)
{
    return SharedSegOutputStream(
        new CrcSegOutputStream(
            segmentAccessor, onlineUuid),
        ClosableObjectDestructor());
}

CrcSegOutputStream::CrcSegOutputStream(
    SegmentAccessor const &segmentAccessorInit,
    PseudoUuid onlineUuidInit)
    : SegOutputStream(segmentAccessorInit, sizeof(SegStreamCrc))
{
    onlineUuid = onlineUuidInit;
}

void CrcSegOutputStream::writeExtraHeaders(SegStreamNode &node)
{
    SegStreamCrc *pCrc = reinterpret_cast<SegStreamCrc *>(&node+1);
    pCrc->onlineUuid = onlineUuid;
    pCrc->pageId = lastPageId;
    crcComputer.reset();
    crcComputer.process_bytes(pCrc + 1, node.cbData);
    pCrc->checksum = crcComputer.checksum();
}

FENNEL_END_CPPFILE("$Id$");

// End CrcSegOutputStream.cpp
