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
#include "fennel/exec/ConduitExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/exec/ExecStreamGraph.h"

FENNEL_BEGIN_CPPFILE("$Id$");
void ConduitExecStream::setInputBufAccessors(
    std::vector<SharedExecStreamBufAccessor> const &inAccessors)
{
    SingleInputExecStream::setInputBufAccessors(inAccessors);
}

void ConduitExecStream::setOutputBufAccessors(
    std::vector<SharedExecStreamBufAccessor> const &outAccessors)
{
    SingleOutputExecStream::setOutputBufAccessors(outAccessors);
}

void ConduitExecStream::prepare(ConduitExecStreamParams const &params)
{
    SingleInputExecStream::prepare(params);

    if (params.outputTupleDesc.empty()) {
        pOutAccessor->setTupleShape(
            pInAccessor->getTupleDesc(),
            pInAccessor->getTupleFormat());
    }

    SingleOutputExecStream::prepare(params);
}

void ConduitExecStream::open(bool restart)
{
    SingleOutputExecStream::open(restart);
    SingleInputExecStream::open(restart);
}

ExecStreamResult ConduitExecStream::precheckConduitBuffers()
{
    switch (pInAccessor->getState()) {
    case EXECBUF_EMPTY:
        pInAccessor->requestProduction();
        return EXECRC_BUF_UNDERFLOW;
    case EXECBUF_UNDERFLOW:
        return EXECRC_BUF_UNDERFLOW;
    case EXECBUF_EOS:
        pOutAccessor->markEOS();
        return EXECRC_EOS;
    case EXECBUF_NONEMPTY:
    case EXECBUF_OVERFLOW:
        break;
    default:
        permAssert(false);
    }
    if (pOutAccessor->getState() == EXECBUF_OVERFLOW) {
        return EXECRC_BUF_OVERFLOW;
    }
    return EXECRC_YIELD;
}

FENNEL_END_CPPFILE("$Id$");

// End ConduitExecStream.cpp
