/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

#include "fennel/common/CommonPreamble.h"
#include "fennel/segment/SegStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

SegStream::SegStream(
    SegmentAccessor const &segmentAccessorInit,uint cbExtraHeader)
    : segmentAccessor(segmentAccessorInit),
      pageLock(segmentAccessor)
{
    cbPageHeader = sizeof(SegStreamNode) + cbExtraHeader;
}

void SegStream::closeImpl()
{
    pageLock.unlock();
    segmentAccessor.reset();
}

SharedSegment SegStream::getSegment() const
{
    return segmentAccessor.pSegment;
}

SegStreamMarker::SegStreamMarker(SegStream const &segStream)
    : ByteStreamMarker(segStream)
{
    segPos.segByteId = SegByteId(MAXU);
    segPos.cbOffset = MAXU;
}

FileSize SegStreamMarker::getOffset() const
{
    return segPos.cbOffset;
}

FENNEL_END_CPPFILE("$Id$");

// End SegStream.cpp
