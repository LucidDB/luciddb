/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2004-2007 John V. Sichi
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
#include "fennel/common/FennelExcn.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/exec/MockResourceExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"

#include <sstream>

FENNEL_BEGIN_CPPFILE("$Id$");

void MockResourceExecStream::prepare(
    MockResourceExecStreamParams const &params)
{
    SingleOutputExecStream::prepare(params);
    minReqt = params.minReqt;
    optReqt = params.optReqt;
    expected = params.expected;

    optTypeInput = params.optTypeInput;
    
    scratchAccessor = params.scratchAccessor;
    scratchLock.accessSegment(scratchAccessor);

    // setup output tuple
    assert(pOutAccessor->getTupleDesc().size() == 1);
    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor expectedOutputDesc(
        stdTypeFactory.newDataType(STANDARD_TYPE_INT_8));
    assert(pOutAccessor->getTupleDesc()[0] == expectedOutputDesc);
    outputTuple.compute(pOutAccessor->getTupleDesc());
    outputTupleAccessor = &pOutAccessor->getScratchTupleAccessor();
}

void MockResourceExecStream::getResourceRequirements(
    ExecStreamResourceQuantity &minQuantity,
    ExecStreamResourceQuantity &optQuantity,
    ExecStreamResourceSettingType &optType)
{
    minQuantity = minReqt;
    optQuantity = optReqt;
    optType = optTypeInput;
}

void MockResourceExecStream::setResourceAllocation(
    ExecStreamResourceQuantity &quantity)
{
    numToAllocate = quantity.nCachePages;
}

void MockResourceExecStream::open(bool restart)
{
    SingleOutputExecStream::open(restart);
    isDone = false;
    if (!restart) {
        outputTupleBuffer.reset(
            new FixedBuffer[outputTupleAccessor->getMaxByteCount()]);
    }
}

ExecStreamResult MockResourceExecStream::execute(
    ExecStreamQuantum const &quantum)
{
    if (isDone) {
        pOutAccessor->markEOS();
        return EXECRC_EOS;
    }

    uint numAllocated = 0;

    if (numToAllocate == expected.nCachePages) {
        for (uint i = 0; i < numToAllocate; i++) {

            // REVIEW jvs 8-Sept--2006: The NULL_PAGE_ID case will never
            // happen, because allocatePage asserts; you probably meant to use
            // tryAllocatePage instead.  But the case must never actually be
            // tested anyway, because on lu/dev after I changed the cache back
            // to retry forever on lockScratchPage, no test hangs.
            
            PageId page = scratchLock.allocatePage();
            // if we can't allocate a page, break out of the loop; the stream
            // will return 0 instead of 1
            if (page == NULL_PAGE_ID) {
                break;
            }
            numAllocated++;
        }
    }

    int8_t outputIndicator = (numAllocated == numToAllocate) ? 1 : 0;
    outputTuple[0].pData = (PConstBuffer) &outputIndicator;
    outputTupleAccessor->marshal(outputTuple, outputTupleBuffer.get());
    pOutAccessor->provideBufferForConsumption(
         outputTupleBuffer.get(),
         outputTupleBuffer.get() + outputTupleAccessor->getCurrentByteCount());
    isDone = true;
    return EXECRC_BUF_OVERFLOW;
}

void MockResourceExecStream::closeImpl()
{
    SingleOutputExecStream::closeImpl();
    if (scratchAccessor.pSegment) {
        scratchAccessor.pSegment->deallocatePageRange(
            NULL_PAGE_ID, NULL_PAGE_ID);
    }
    outputTupleBuffer.reset();
}

ExecStreamBufProvision MockResourceExecStream::getOutputBufProvision() const
{
    return BUFPROV_PRODUCER;
}

FENNEL_END_CPPFILE("$Id$");

// End MockResourceExecStream.cpp
