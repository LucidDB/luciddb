/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2004-2005 John V. Sichi
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
#include "fennel/cache/CacheAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

ExecStreamParams::ExecStreamParams()
{
    enforceQuotas = true;
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

void ExecStream::prepare(ExecStreamParams const &params)
{
    if (params.enforceQuotas) {
        pQuotaAccessor = params.pCacheAccessor;
        pScratchQuotaAccessor = params.scratchAccessor.pCacheAccessor;
    }
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

void ExecStream::open(bool restart)
{
    if (restart) {
        assert(isOpen);
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

ExecStreamBufProvision ExecStream::getInputBufProvision() const
{
    return BUFPROV_NONE;
}

FENNEL_END_CPPFILE("$Id$");

// End ExecStream.cpp
