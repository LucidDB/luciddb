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
#include "fennel/exec/SegBufferReaderExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/exec/DynamicParam.h"
#include "fennel/exec/SegBufferReader.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void SegBufferReaderExecStream::prepare(
    SegBufferReaderExecStreamParams const &params)
{
    ConduitExecStream::prepare(params);
    bufferSegmentAccessor = params.scratchAccessor;
    readerRefCountParamId = params.readerRefCountParamId;

    assert(pInAccessor->getTupleDesc().size() == 1);
    inputTuple.compute(pInAccessor->getTupleDesc());
}

void SegBufferReaderExecStream::getResourceRequirements(
    ExecStreamResourceQuantity &minQuantity,
    ExecStreamResourceQuantity &optQuantity)
{
    ConduitExecStream::getResourceRequirements(minQuantity, optQuantity);

    // set aside 1 page for I/O
    minQuantity.nCachePages += 1;
    optQuantity = minQuantity;
}

void SegBufferReaderExecStream::open(bool restart)
{
    assert(pOutAccessor);
    assert(pOutAccessor->getProvision() == BUFPROV_PRODUCER);

    if (!restart) {
        firstBufferPageId = NULL_PAGE_ID;
        pDynamicParamManager->incrementCounterParam(readerRefCountParamId);
        ConduitExecStream::open(restart);
    } else {
        pOutAccessor->clear();
        if (pSegBufferReader) {
            // reread from beginning
            pSegBufferReader->open(false);
        } else {
            // the buffered data hasn't been read yet, so treat this the
            // same as first open
            ConduitExecStream::open(restart);
        }
    }
}

void SegBufferReaderExecStream::closeImpl()
{
    pDynamicParamManager->decrementCounterParam(readerRefCountParamId);
    pSegBufferReader.reset();
    ConduitExecStream::closeImpl();
}

ExecStreamResult SegBufferReaderExecStream::execute(ExecStreamQuantum const &)
{
    // Retrieve the first pageId of the buffered data from the writer stream,
    // if it hasn't already been retrieved, and then setup a buffer reader.
    if (firstBufferPageId == NULL_PAGE_ID) {
        if (!pInAccessor->demandData()) {
            return EXECRC_BUF_UNDERFLOW;
        }
        pInAccessor->unmarshalTuple(inputTuple);
        firstBufferPageId =
            *reinterpret_cast<PageId const *> (inputTuple[0].pData);
        pInAccessor->consumeTuple();
        pSegBufferReader =
            SegBufferReader::newSegBufferReader(
                pOutAccessor,
                bufferSegmentAccessor,
                firstBufferPageId);
        pSegBufferReader->open(false);
    }

    // Read the buffered data
    return pSegBufferReader->read();
}

ExecStreamBufProvision SegBufferReaderExecStream::getOutputBufProvision() const
{
    return BUFPROV_PRODUCER;
}

FENNEL_END_CPPFILE("$Id$")

// End SegBufferReaderExecStream.cpp
