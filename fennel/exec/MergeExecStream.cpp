/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
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
#include "fennel/exec/MergeExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void MergeExecStream::prepare(
    MergeExecStreamParams const &params)
{
    ConfluenceExecStream::prepare(params);
    assert(!inAccessors.empty());
    for (uint i = 0; i < inAccessors.size(); ++i) {
        assert(
            inAccessors[i]->getTupleDesc() == pOutAccessor->getTupleDesc());
        assert(
            inAccessors[i]->getTupleFormat() == pOutAccessor->getTupleFormat());
    }
}

void MergeExecStream::open(
    bool restart)
{
    ConfluenceExecStream::open(restart);
    iInput = 0;
    pLastConsumptionEnd = NULL;
}

ExecStreamResult MergeExecStream::execute(
    ExecStreamQuantum const &)
{
    switch(pOutAccessor->getState()) {
    case EXECBUF_NONEMPTY:
    case EXECBUF_OVERFLOW:
        return EXECRC_BUF_OVERFLOW;
    case EXECBUF_UNDERFLOW:
    case EXECBUF_EMPTY:
        if (pLastConsumptionEnd) {
            // Since our output buf is empty, the downstream consumer
            // must have consumed everything up to the last byte we
            // told it was available; pass that information on to our
            // upstream producer.
            inAccessors[iInput]->consumeData(pLastConsumptionEnd);
            pLastConsumptionEnd = NULL;
        }
        break;
    case EXECBUF_EOS:
        return EXECRC_EOS;
    }

    while (iInput < inAccessors.size()) {
        switch(inAccessors[iInput]->getState()) {
        case EXECBUF_OVERFLOW:
        case EXECBUF_NONEMPTY:
            // Pass through current input buf to our downstream consumer.
            pLastConsumptionEnd = inAccessors[iInput]->getConsumptionEnd();
            pOutAccessor->provideBufferForConsumption(
                inAccessors[iInput]->getConsumptionStart(),
                pLastConsumptionEnd);
            return EXECRC_BUF_OVERFLOW;
        case EXECBUF_UNDERFLOW:
            return EXECRC_BUF_UNDERFLOW;
        case EXECBUF_EMPTY:
            inAccessors[iInput]->requestProduction();
            return EXECRC_BUF_UNDERFLOW;
        case EXECBUF_EOS:
            // Current input is exhausted; move on to the next one.
            ++iInput;
            break;
        default:
            permAssert(false);
        }
    }
    pOutAccessor->markEOS();
    return EXECRC_EOS;
}

ExecStreamBufProvision MergeExecStream::getOutputBufProvision() const
{
    return BUFPROV_PRODUCER;
}

FENNEL_END_CPPFILE("$Id$");

// End MergeExecStream.cpp
