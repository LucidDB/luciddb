/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2003-2005 Disruptive Tech
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

#include "fennel/common/CommonPreamble.h"
#include "fennel/xo/ExecutionStream.h"
#include "fennel/cache/CacheAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

ExecutionStreamParams::~ExecutionStreamParams()
{}
    
ExecutionStream::ExecutionStream()
{
    pGraph = NULL;
    id = MAXU;
    isOpen = false;
    name = "";
}

void ExecutionStream::closeImpl()
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

SharedExecutionStream ExecutionStream::getStreamInput(uint ordinal)
{
    return pGraph->getStreamInput(getStreamId(),ordinal);
}
    
ExecutionStream::~ExecutionStream()
{
}

void ExecutionStream::prepare(ExecutionStreamParams const &params)
{
    if (params.enforceQuotas) {
        pQuotaAccessor = params.pCacheAccessor;
        pScratchQuotaAccessor = params.scratchAccessor.pCacheAccessor;
    }
}
    
void ExecutionStream::getResourceRequirements(
    ExecutionStreamResourceQuantity &minQuantity,
    ExecutionStreamResourceQuantity &optQuantity)
{
    minQuantity.nThreads = 0;
    minQuantity.nCachePages = 0;
    optQuantity = minQuantity;
}

void ExecutionStream::setResourceAllocation(
    ExecutionStreamResourceQuantity const &quantity)
{
    resourceAllocation = quantity;
    if (pQuotaAccessor) {
        pQuotaAccessor->setMaxLockedPages(quantity.nCachePages);
    }
    if (pScratchQuotaAccessor) {
        pScratchQuotaAccessor->setMaxLockedPages(quantity.nCachePages);
    }
}

void ExecutionStream::open(bool restart)
{
    if (restart) {
        assert(isOpen);
    } else {
        // NOTE: this assertion is bad because in case of multiple
        // inheritance, open can be called twice.  So we rely on the
        // corresponding assertion in TupleStreamGraph instead, unless
        // someone can come up with something better.
#if 0
        assert(!isOpen);
#endif
        isOpen = true;
        needsClose = true;
    }
}

ExecutionStreamId ExecutionStream::getStreamId() const
{
    return id;
}

void ExecutionStream::setName(std::string const &nameIn)
{
    name = nameIn;
}

std::string const &ExecutionStream::getName() const
{
    return name;
}
    
TupleFormat ExecutionStream::getOutputFormat() const
{
    return TUPLE_FORMAT_STANDARD;
}

ByteInputStream &ExecutionStream::getProducerResultStream()
{
    permAssert(false);
}

bool ExecutionStream::writeResultToConsumerBuffer(
    ByteOutputStream &resultOutputStream) 
{
    permAssert(false);
}

ExecutionStream::BufferProvision 
ExecutionStream::getInputBufferRequirement() const
{
    return NO_PROVISION;
}

ExecutionStream *ExecutionStream::getImpl()
{
    return this;
}

FENNEL_END_CPPFILE("$Id$");

// End ExecutionStream.h
