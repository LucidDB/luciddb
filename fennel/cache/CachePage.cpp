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
    std::swap(pBuffer, other.pBuffer);
}

bool CachePage::isScratchLocked() const
{
    return CompoundId::getDeviceId(getBlockId()) == Cache::NULL_DEVICE_ID;
}

FENNEL_END_CPPFILE("$Id$");

// End CachePage.cpp
