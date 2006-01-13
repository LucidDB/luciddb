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

#include "fennel/common/CommonPreamble.h"
#include "fennel/cache/Cache.h"
#include "fennel/cache/CachePage.h"
#include "fennel/common/CompoundId.h"

#include <algorithm>

FENNEL_BEGIN_CPPFILE("$Id$");

CachePage::CachePage(Cache &cacheInit,PBuffer pBufferInit)
    : cache(cacheInit)
{
    nReferences = 0;
    dataStatus = DATA_INVALID;
    blockId = NULL_BLOCK_ID;
    pBuffer = pBufferInit;
    pMappedPageListener = NULL;
}

CachePage::~CachePage()
{
    assert(!nReferences);
    assert(blockId == NULL_BLOCK_ID);
    assert(dataStatus == DATA_INVALID);
}

void CachePage::notifyTransferCompletion(bool bSuccess)
{
    getCache().notifyTransferCompletion(*this,bSuccess);
}

PBuffer CachePage::getBuffer() const
{
    return pBuffer;
}

uint CachePage::getBufferSize() const
{
    return cache.getPageSize();
}

void CachePage::swapBuffers(CachePage &other)
{
    assert(isExclusiveLockHeld());
    assert(other.isExclusiveLockHeld());
    std::swap(pBuffer,other.pBuffer);
}

bool CachePage::isScratchLocked() const
{
    return CompoundId::getDeviceId(getBlockId()) == Cache::NULL_DEVICE_ID;
}

FENNEL_END_CPPFILE("$Id$");

// End Page.cpp
