/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2007-2007 The Eigenbase Project
// Copyright (C) 2007-2007 SQLstream, Inc.
// Copyright (C) 2007-2007 LucidEra, Inc.
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
#include "fennel/exec/BernoulliSamplingExecStream.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/exec/ExecStreamBufAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void BernoulliSamplingExecStream::prepare(
    BernoulliSamplingExecStreamParams const &params)
{
    ConduitExecStream::prepare(params);

    samplingRate = params.samplingRate;
    isRepeatable = params.isRepeatable;
    repeatableSeed = params.repeatableSeed;

    samplingRng.reset(new BernoulliRng(samplingRate));

    assert(pInAccessor->getTupleDesc() == pOutAccessor->getTupleDesc());

    data.compute(pOutAccessor->getTupleDesc());
}

void BernoulliSamplingExecStream::open(bool restart)
{
    ConduitExecStream::open(restart);

    if (isRepeatable) {
        samplingRng->reseed(repeatableSeed);
    } else if (!restart) {
        samplingRng->reseed(static_cast<uint32_t>(time(0)));
    }

    producePending = false;
}

ExecStreamResult BernoulliSamplingExecStream::execute(
    ExecStreamQuantum const &quantum)
{
    ExecStreamResult rc = precheckConduitBuffers();
    if (rc != EXECRC_YIELD) {
        return rc;
    }

    if (producePending) {
        if (!pOutAccessor->produceTuple(data)) {
            return EXECRC_BUF_OVERFLOW;
        }
        pInAccessor->consumeTuple();
        producePending = false;
    }

    for (uint i = 0; i < quantum.nTuplesMax; i++) {
        if (!pInAccessor->demandData()) {
            return EXECRC_BUF_UNDERFLOW;
        }

        pInAccessor->accessConsumptionTuple();

        if (!samplingRng->nextValue()) {
            pInAccessor->consumeTuple();
            continue;
        }

        pInAccessor->getConsumptionTupleAccessor().unmarshal(data);

        producePending = true;
        if (!pOutAccessor->produceTuple(data)) {
            return EXECRC_BUF_OVERFLOW;
        }
        producePending = false;
        pInAccessor->consumeTuple();
    }

    return EXECRC_QUANTUM_EXPIRED;
}

FENNEL_END_CPPFILE("$Id$");

// End BernoulliSamplingExecStream.cpp
