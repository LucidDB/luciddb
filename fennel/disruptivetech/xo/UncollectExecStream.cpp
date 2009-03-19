/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 The Eigenbase Project
// Portions Copyright (C) 1999-2007 John V. Sichi
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
#include "fennel/disruptivetech/xo/UncollectExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/tuple/TuplePrinter.h"


FENNEL_BEGIN_CPPFILE("$Id$");

void UncollectExecStream::prepare(UncollectExecStreamParams const &params)
{
    ConduitExecStream::prepare(params);

    FENNEL_TRACE(
        TRACE_FINER,
        "uncollect xo input TupleDescriptor = "
        << pInAccessor->getTupleDesc());

    FENNEL_TRACE(
        TRACE_FINER,
        "uncollect xo output TupleDescriptor = "
        << pOutAccessor->getTupleDesc());

    StandardTypeDescriptorOrdinal ordinal =
        StandardTypeDescriptorOrdinal(
        pInAccessor->getTupleDesc()[0].pTypeDescriptor->getOrdinal());
    assert(ordinal == STANDARD_TYPE_VARBINARY);
    assert(1 == pInAccessor->getTupleDesc().size());

    inputTupleData.compute(pInAccessor->getTupleDesc());
    outputTupleData.compute(pOutAccessor->getTupleDesc());
}


void UncollectExecStream::open(bool restart)
{
    ConduitExecStream::open(restart);
    bytesWritten = 0;
}

ExecStreamResult UncollectExecStream::execute(ExecStreamQuantum const &quantum)
{
    ExecStreamResult rc = precheckConduitBuffers();
    if (EXECRC_YIELD != rc) {
        return rc;
    }

    if (!pInAccessor->demandData()) {
        return EXECRC_BUF_UNDERFLOW;
    }

    pInAccessor->unmarshalTuple(inputTupleData);

#if 0
    std::cout<<"input tuple descriptor" << pInAccessor->getTupleDesc()<<std::endl;
    std::cout << "input tuple = ";
    TupleDescriptor statusDesc = pInAccessor->getTupleDesc();
    TuplePrinter tuplePrinter;
    tuplePrinter.print(std::cout, statusDesc, inputTupleData);
    std::cout << std::endl;
#endif

    TupleAccessor& outTa = pOutAccessor->getScratchTupleAccessor();
    while (bytesWritten < inputTupleData[0].cbData) {
        // write one item in the input array to the output buffer
        outTa.setCurrentTupleBuf(inputTupleData[0].pData + bytesWritten);
        outTa.unmarshal(outputTupleData);
#if 0
    std::cout << "unmarshalling ouput tuple= ";
    TupleDescriptor statusDesc = pOutAccessor->getTupleDesc();
    TuplePrinter tuplePrinter;
    tuplePrinter.print(std::cout, statusDesc, outputTupleData);
    std::cout << std::endl;
#endif

        if (!pOutAccessor->produceTuple(outputTupleData)) {
            return EXECRC_BUF_OVERFLOW;
        }
        bytesWritten += outTa.getCurrentByteCount();

    }

    assert(pInAccessor->isTupleConsumptionPending());
    assert(bytesWritten == inputTupleData[0].cbData);
    pInAccessor->consumeTuple();

    return EXECRC_QUANTUM_EXPIRED;
}

FENNEL_END_CPPFILE("$Id$");

// End UncollectExecStream.cpp
