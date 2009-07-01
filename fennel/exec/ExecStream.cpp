/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2004-2009 John V. Sichi
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
#include "fennel/exec/ExecStream.h"
#include "fennel/exec/ExecStreamGraph.h"
#include "fennel/exec/ExecStreamScheduler.h"
#include "fennel/cache/CacheAccessor.h"
#include "fennel/txn/LogicalTxn.h"

FENNEL_BEGIN_CPPFILE("$Id$");

ExecStreamParams::ExecStreamParams()
{
}

ExecStreamParams::~ExecStreamParams()
{
}

ExecStream::ExecStream()
{
    pGraph = NULL;
    id = MAXU;
    isOpen = false;
    name = "";
}

bool ExecStream::canEarlyClose()
{
    return true;
}

void ExecStream::closeImpl()
{
    isOpen = false;

    // REVIEW jvs 19-July-2004:  It would be nice to be able to do this, making
    // sure no cache access is attempted while stream is closed.  However,
    // it currently causes trouble with TableWriters, which need
    // cache access for txn replay.
    /*
    if (pQuotaAccessor) {
        pQuotaAccessor->setMaxLockedPages(0);
    }
    if (pScratchQuotaAccessor) {
        pScratchQuotaAccessor->setMaxLockedPages(0);
    }
    */
}

void ExecStream::checkAbort() const
{
    if (!pGraph) {
        return;
    }
    ExecStreamScheduler *pScheduler = pGraph->getScheduler();
    if (!pScheduler) {
        return;
    }
    pScheduler->checkAbort();
}

void ExecStream::prepare(ExecStreamParams const &params)
{
    if (pGraph) {
        pDynamicParamManager = pGraph->getDynamicParamManager();
    }
    pQuotaAccessor = params.pCacheAccessor;
    pScratchQuotaAccessor = params.scratchAccessor.pCacheAccessor;
}

void ExecStream::getResourceRequirements(
    ExecStreamResourceQuantity &minQuantity,
    ExecStreamResourceQuantity &optQuantity,
    ExecStreamResourceSettingType &optType)
{
    getResourceRequirements(minQuantity, optQuantity);
    optType = EXEC_RESOURCE_ACCURATE;
}

void ExecStream::getResourceRequirements(
    ExecStreamResourceQuantity &minQuantity,
    ExecStreamResourceQuantity &optQuantity)
{
    minQuantity.nThreads = 0;
    minQuantity.nCachePages = 0;
    optQuantity = minQuantity;
}

void ExecStream::setResourceAllocation(
    ExecStreamResourceQuantity &quantity)
{
    resourceAllocation = quantity;
    if (pQuotaAccessor) {
        pQuotaAccessor->setMaxLockedPages(quantity.nCachePages);
    }
    if (pScratchQuotaAccessor) {
        pScratchQuotaAccessor->setMaxLockedPages(quantity.nCachePages);
    }
}

uint ExecStream::getCacheConsciousPageRation(
    CacheAccessor &cacheAccessor,
    ExecStreamResourceQuantity const &allocatedQuantity)
{
    // TODO jvs 30-Jun-2009:  Make the cache-consciousness below
    // aware of other activities going on in parallel.

    // NOTE jvs 30-Jun-2009: Here we cap the number of pages to be used for
    // in-memory algorithms such as quicksort and hashing to the size of the
    // processor cache (for better locality of reference).  Additional buffer
    // pool pages assigned to us won't be wasted, as they will reduce (or
    // possibly eliminate) disk I/O from external sort/partitioning.
    uint processorCacheBytes = cacheAccessor.getProcessorCacheBytes();
    uint processorCachePages =
        processorCacheBytes / cacheAccessor.getCache()->getPageSize();
    uint nPages = allocatedQuantity.nCachePages;
    if (nPages > processorCachePages) {
        nPages = processorCachePages;
    }
    return nPages;
}

void ExecStream::open(bool restart)
{
    if (restart) {
        // REVIEW jvs 3-Jan-2007:  We used to be able to assert this,
        // but now that we've introduced early close to release
        // resources, we can't.
#if 0
        assert(isOpen);
#endif
    } else {
        // NOTE: this assertion is bad because in case of multiple
        // inheritance, open can be called twice.  So we rely on the
        // corresponding assertion in ExecStreamGraph instead, unless
        // someone can come up with something better.
#if 0
        assert(!isOpen);
#endif
        isOpen = true;
        needsClose = true;
    }
    if (pGraph) {
        pTxn = pGraph->getTxn();
        TxnId txnId = pGraph->getTxnId();
        if (txnId != NULL_TXN_ID) {
            if (pQuotaAccessor) {
                pQuotaAccessor->setTxnId(txnId);
            }
            if (pScratchQuotaAccessor) {
                pScratchQuotaAccessor->setTxnId(txnId);
            }
        }
    }
}

void ExecStream::setName(std::string const &nameInit)
{
    name = nameInit;
}

std::string const &ExecStream::getName() const
{
    return name;
}

bool ExecStream::mayBlock() const
{
    return false;
}

ExecStreamBufProvision ExecStream::getOutputBufProvision() const
{
    return BUFPROV_NONE;
}

ExecStreamBufProvision ExecStream::getOutputBufConversion() const
{
    return BUFPROV_NONE;
}

ExecStreamBufProvision ExecStream::getInputBufProvision() const
{
    return BUFPROV_NONE;
}

FENNEL_END_CPPFILE("$Id$");

// End ExecStream.cpp
