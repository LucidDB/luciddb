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
#include "fennel/exec/DiffluenceExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/exec/ExecStreamGraph.h"

FENNEL_BEGIN_CPPFILE("$Id$");

DiffluenceExecStreamParams::DiffluenceExecStreamParams()
{
    outputTupleFormat = TUPLE_FORMAT_STANDARD;
}

void DiffluenceExecStream::setOutputBufAccessors(
    std::vector<SharedExecStreamBufAccessor> const &outAccessorsInit)
{
    outAccessors = outAccessorsInit;
}

void DiffluenceExecStream::prepare(DiffluenceExecStreamParams const &params)
{
    SingleInputExecStream::prepare(params);

    // By default, shape for all outputs is the same as the input if the
    // outputTupleDesc wasn't explicitly set.
    TupleDescriptor tupleDesc;
    TupleFormat tupleFormat;
    if (params.outputTupleDesc.empty()) {
        tupleDesc = pInAccessor->getTupleDesc();
        tupleFormat = pInAccessor->getTupleFormat();
    } else {
        tupleDesc = params.outputTupleDesc;
        tupleFormat = params.outputTupleFormat;
    }
    for (uint i = 0; i < outAccessors.size(); ++i) {
        assert(outAccessors[i]->getProvision() == getOutputBufProvision());
        outAccessors[i]->setTupleShape(tupleDesc, tupleFormat);
    }
}

void DiffluenceExecStream::open(bool restart)
{
    SingleInputExecStream::open(restart);

    if (restart) {
        // restart outputs
        for (uint i = 0; i < outAccessors.size(); ++i) {
            outAccessors[i]->clear();
        }
    }
}

ExecStreamBufProvision DiffluenceExecStream::getOutputBufProvision() const
{
    /*
     * Indicate to the consumer that buffer should be provided by the consumer.
     * By default, DiffluenceExecStream does not have any associated buffers.
     */
    return BUFPROV_CONSUMER;
}


FENNEL_END_CPPFILE("$Id$");

// End DiffluenceExecStream.cpp
