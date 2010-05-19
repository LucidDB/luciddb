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
#include "fennel/exec/SingleOutputExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

SingleOutputExecStreamParams::SingleOutputExecStreamParams()
{
    outputTupleFormat = TUPLE_FORMAT_STANDARD;
}

void SingleOutputExecStream::setInputBufAccessors(
    std::vector<SharedExecStreamBufAccessor> const &inAccessors)
{
    assert(inAccessors.size() == 0);
}

void SingleOutputExecStream::setOutputBufAccessors(
    std::vector<SharedExecStreamBufAccessor> const &outAccessors)
{
    assert(outAccessors.size() == 1);
    pOutAccessor = outAccessors[0];
}

void SingleOutputExecStream::prepare(SingleOutputExecStreamParams const &params)
{
    ExecStream::prepare(params);
    assert(pOutAccessor);
    assert(pOutAccessor->getProvision() == getOutputBufProvision());
    if (pOutAccessor->getTupleDesc().empty()) {
        assert(!params.outputTupleDesc.empty());
        pOutAccessor->setTupleShape(
            params.outputTupleDesc,
            params.outputTupleFormat);
    }
}

void SingleOutputExecStream::open(bool restart)
{
    ExecStream::open(restart);
    if (restart) {
        pOutAccessor->clear();
    }
}

ExecStreamBufProvision SingleOutputExecStream::getOutputBufProvision() const
{
    return BUFPROV_CONSUMER;
}

FENNEL_END_CPPFILE("$Id$");

// End SingleOutputExecStream.cpp
