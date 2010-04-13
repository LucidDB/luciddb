/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2004 John V. Sichi
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
#include "fennel/exec/SegBufferExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/exec/ExecStreamGraphImpl.h"
#include "fennel/exec/SegBufferReader.h"
#include "fennel/exec/SegBufferWriter.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void SegBufferExecStream::prepare(SegBufferExecStreamParams const &params)
{
    ConduitExecStream::prepare(params);
    bufferSegmentAccessor = params.scratchAccessor;

    multipass = params.multipass;
    firstPageId = NULL_PAGE_ID;
}

void SegBufferExecStream::getResourceRequirements(
    ExecStreamResourceQuantity &minQuantity,
    ExecStreamResourceQuantity &optQuantity)
{
    ConduitExecStream::getResourceRequirements(minQuantity, optQuantity);

    // set aside 1 page for I/O
    minQuantity.nCachePages += 1;

    optQuantity = minQuantity;
}

void SegBufferExecStream::open(bool restart)
{
    assert(pInAccessor);
    assert(pInAccessor->getProvision() == BUFPROV_CONSUMER);

    assert(pOutAccessor);
    assert(pOutAccessor->getProvision() == BUFPROV_PRODUCER);

    // TODO jvs 1-June-2006:  generalize SegStreamAllocation to handle
    // the multipass usage requirements here

    if (restart) {
        pOutAccessor->clear();
        if (multipass) {
            if (pSegBufferReader) {
                // reread from beginning
                openBufferForRead(false);
            } else {
                // nothing was ever buffered, so treat this the
                // same as first open
                ConduitExecStream::open(restart);
            }
        } else {
            // for a single-pass buffer, a restart means forget any buffered
            // contents
            destroyBuffer();
            ConduitExecStream::open(restart);
        }
    } else {
        ConduitExecStream::open(restart);
    }
}

void SegBufferExecStream::closeImpl()
{
    destroyBuffer();
    ConduitExecStream::closeImpl();
}

void SegBufferExecStream::destroyBuffer()
{
    if (pSegBufferWriter || (multipass && (firstPageId != NULL_PAGE_ID))) {
        // this is to make sure that buffer storage gets deallocated in all
        // cases
        openBufferForRead(true);
    }
    pSegBufferReader.reset();
    firstPageId = NULL_PAGE_ID;
}

void SegBufferExecStream::openBufferForRead(bool destroy)
{
    if (firstPageId == NULL_PAGE_ID) {
        firstPageId = pSegBufferWriter->getFirstPageId();
        pSegBufferWriter.reset();
        pSegBufferReader =
            SegBufferReader::newSegBufferReader(
                pOutAccessor,
                bufferSegmentAccessor,
                firstPageId);
    }
    pSegBufferReader->open(destroy);
}

ExecStreamResult SegBufferExecStream::execute(ExecStreamQuantum const &)
{
    if (!pSegBufferReader) {
        if (!pSegBufferWriter) {
            pSegBufferWriter =
                SegBufferWriter::newSegBufferWriter(
                    pInAccessor,
                    bufferSegmentAccessor,
                    false);
        }
        ExecStreamResult rc = pSegBufferWriter->write();
        if (rc != EXECRC_EOS) {
            return rc;
        }

        ExecStreamGraphImpl &graphImpl =
            dynamic_cast<ExecStreamGraphImpl&>(getGraph());
        graphImpl.closeProducers(getStreamId());
        openBufferForRead(!multipass);
    }

    return pSegBufferReader->read();
}

ExecStreamBufProvision SegBufferExecStream::getOutputBufProvision() const
{
    return BUFPROV_PRODUCER;
}

ExecStreamBufProvision SegBufferExecStream::getInputBufProvision() const
{
    return BUFPROV_CONSUMER;
}

FENNEL_END_CPPFILE("$Id$");

// End SegBufferExecStream.cpp
