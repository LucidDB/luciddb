/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

#include "fennel/common/CommonPreamble.h"
#include "fennel/exec/ConduitExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/exec/ExecStreamGraph.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void ConduitExecStream::setInputBufAccessors(
    std::vector<SharedExecStreamBufAccessor> const &inAccessors)
{
    assert(inAccessors.size() == 1);
    pInAccessor = inAccessors[0];
}

void ConduitExecStream::prepare(ConduitExecStreamParams const &params)
{
    assert(pInAccessor);
    assert(pInAccessor->getProvision() == getInputBufProvision());

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
    if (restart) {
        // restart input
        pInAccessor->clear();
        pGraph->getStreamInput(getStreamId(),0)->open(true);
    }
}

ExecStreamBufProvision ConduitExecStream::getInputBufProvision() const
{
    return BUFPROV_PRODUCER;
}

ExecStreamResult ConduitExecStream::precheckConduitInput()
{
    switch (pInAccessor->getState()) {
    case EXECBUF_IDLE:
        pInAccessor->requestProduction();
        // NOTE:  fall through
    case EXECBUF_NEED_PRODUCTION:
        return EXECRC_NEED_INPUT;
    case EXECBUF_EOS:
        pOutAccessor->markEOS();
        return EXECRC_EOS;
    case EXECBUF_NEED_CONSUMPTION:
        return EXECRC_OUTPUT;
    default:
        permAssert(false);
    }
}

FENNEL_END_CPPFILE("$Id$");

// End ConduitExecStream.cpp
