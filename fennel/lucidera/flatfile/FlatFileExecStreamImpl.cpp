/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
// Portions Copyright (C) 2004-2005 John V. Sichi
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

#include "fennel/lucidera/flatfile/FlatFileExecStreamImpl.h"

#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/tuple/StoredTypeDescriptor.h"
#include "fennel/tuple/StandardTypeDescriptor.h"

#include "fennel/disruptivetech/calc/CalcCommon.h"
#include "fennel/disruptivetech/xo/CalcExcn.h"

FENNEL_BEGIN_CPPFILE("$Id$");

FlatFileExecStream *FlatFileExecStream::newFlatFileExecStream()
{
    return new FlatFileExecStreamImpl();
}

void FlatFileExecStreamImpl::convert(
    const FlatFileRowParseResult &result,
    TupleData &tuple)
{
    for (uint i=0; i<tuple.size(); i++) {
        char *value = result.current + result.offsets[i];
        uint size = result.sizes[i];
        uint strippedSize = pParser->stripQuoting(value, size, false);

        if (pCalc) {
            textTuple[i].pData = (PConstBuffer) value;
            textTuple[i].cbData = strippedSize;
            // output tuple is already bound
        } else {
            tuple[i].pData = (PConstBuffer) value;
            tuple[i].cbData = strippedSize;
        }
    }
    if (pCalc) {
        try {
            pCalc->exec();
        } catch (FennelExcn e) {
            FENNEL_TRACE(TRACE_SEVERE, "error executing calculator: " << e.getMessage());
            throw e;
        }
    }
}

void FlatFileExecStreamImpl::prepare(
    FlatFileExecStreamParams const &params)
{
    SingleOutputExecStream::prepare(params);

    header = params.header;
    logging = (params.errorFilePath.size() > 0);
    
    dataTuple.compute(pOutAccessor->getTupleDesc());
    
    scratchAccessor = params.scratchAccessor;
    bufferLock.accessSegment(scratchAccessor);

    rowDesc = readTupleDescriptor(pOutAccessor->getTupleDesc());
    pBuffer.reset(new FlatFileBuffer(params.dataFilePath));
    pParser.reset(new FlatFileParser(
                      params.fieldDelim, params.rowDelim,
                      params.quoteChar, params.escapeChar));
    if (params.calcProgram.size() == 0) return;
    
    try {
        // Force instantiation of the calculator's instruction tables.
        (void) CalcInit::instance();

        pCalc.reset(new Calculator(pDynamicParamManager.get()));
        if (isTracing()) {
            pCalc->initTraceSource(getSharedTraceTarget(), "calc");
        }

        pCalc->assemble(params.calcProgram.c_str());

        FENNEL_TRACE(
            TRACE_FINER,
            "calc program = "
            << std::endl << params.calcProgram);

        FENNEL_TRACE(
            TRACE_FINER,
            "calc input TupleDescriptor = "
            << pCalc->getInputRegisterDescriptor());

        textDesc = pCalc->getInputRegisterDescriptor();
        FENNEL_TRACE(
            TRACE_FINER,
            "xo input TupleDescriptor = "
            << textDesc);

        FENNEL_TRACE(
            TRACE_FINER,
            "calc output TupleDescriptor = "
            << pCalc->getOutputRegisterDescriptor());

        FENNEL_TRACE(
            TRACE_FINER,
            "xo output TupleDescriptor = "
            << params.outputTupleDesc);

        assert(textDesc.storageEqual(pCalc->getInputRegisterDescriptor()));

        TupleDescriptor outputDesc = pCalc->getOutputRegisterDescriptor();

        if (!params.outputTupleDesc.empty()) {
            assert(outputDesc.storageEqual(params.outputTupleDesc));

            // if the plan specifies an output descriptor with different
            // nullability, use that instead
            outputDesc = params.outputTupleDesc;
        }
        pOutAccessor->setTupleShape(
            outputDesc,
            pOutAccessor->getTupleFormat());

        textTuple.compute(textDesc);

        dataTuple.compute(outputDesc);

        // bind calculator to tuple data (tuple data may later change)
        pCalc->bind(&textTuple,&dataTuple);

        // Set calculator to return immediately on exception as a
        // workaround.  Prevents indeterminate results from an instruction
        // that throws an exception from causing non-deterministic
        // behavior later in program execution.
        pCalc->continueOnException(false);

    } catch (FennelExcn e) {
        FENNEL_TRACE(TRACE_SEVERE, "error preparing calculator: " << e.getMessage());
        throw e;
    }
}

FlatFileRowDescriptor FlatFileExecStreamImpl::readTupleDescriptor(
    const TupleDescriptor &tupleDesc)
{
    StandardTypeDescriptorFactory typeFactory;
    FlatFileRowDescriptor rowDesc;
    for (uint i=0; i < tupleDesc.size(); i++) {
        TupleAttributeDescriptor attr = tupleDesc[i];
        StandardTypeDescriptorOrdinal ordinal =
            StandardTypeDescriptorOrdinal(
                attr.pTypeDescriptor->getOrdinal());
        if (StandardTypeDescriptor::isTextArray(ordinal)) {
            rowDesc.push_back(FlatFileColumnDescriptor(true, attr.cbStorage));
        } else {
            rowDesc.push_back(FlatFileColumnDescriptor(false,
                                  FLAT_FILE_MAX_COLUMN_NAME_LEN));
        }
    }
    return rowDesc;
}

void FlatFileExecStreamImpl::getResourceRequirements(
    ExecStreamResourceQuantity &minQuantity,
    ExecStreamResourceQuantity &optQuantity)
{
    SingleOutputExecStream::getResourceRequirements(minQuantity,optQuantity);
    minQuantity.nCachePages += 2;
    optQuantity = minQuantity;
}

void FlatFileExecStreamImpl::open(bool restart)
{
    if (restart) {
        releaseResources();
    }
    SingleOutputExecStream::open(restart);

    if (! restart)
    {
        uint cbTupleMax =
            pOutAccessor->getScratchTupleAccessor().getMaxByteCount();
        bufferLock.allocatePage();
        uint cbPageSize = bufferLock.getPage().getCache().getPageSize();
        assert(cbPageSize >= cbTupleMax);
        pBufferStorage = bufferLock.getPage().getWritableData();
        pBuffer->setStorage((char*)pBufferStorage, cbPageSize);
    }
    pBuffer->open();
    pBuffer->fill();
    next = pBuffer->buf();
    isRowPending = false;

    if (header) {
        FlatFileRowDescriptor headerDesc;
        for (uint i=0; i < rowDesc.size(); i++) {
            headerDesc.push_back(
                FlatFileColumnDescriptor(
                    true,
                    FLAT_FILE_MAX_COLUMN_NAME_LEN));
        }
        next = pParser->scanRow(*pBuffer, next, headerDesc, lastResult);
        next = pParser->scanRowEnd(*pBuffer, lastResult);
    }
}


ExecStreamResult FlatFileExecStreamImpl::execute(
    ExecStreamQuantum const &quantum)
{
    if (pOutAccessor->getState() == EXECBUF_OVERFLOW) {
        return EXECRC_BUF_OVERFLOW;
    }

    for (uint nTuples=0; nTuples < quantum.nTuplesMax; nTuples++) {
        while (!isRowPending) {
            if (pBuffer->end() && (next >= pBuffer->buf()+pBuffer->size())) {
                pOutAccessor->markEOS();
                return EXECRC_EOS;
            }
            next = pParser->scanRow(*pBuffer, next, rowDesc, lastResult);
            switch (lastResult.status) {
            case FlatFileRowParseResult::INCOMPLETE_COLUMN:
            case FlatFileRowParseResult::COLUMN_TOO_LARGE:
            case FlatFileRowParseResult::TOO_FEW_COLUMNS:
            case FlatFileRowParseResult::TOO_MANY_COLUMNS:
                if (logging) {
                    // log error;
                }
                next = pParser->scanRowEnd(*pBuffer, lastResult);
                continue;
            case FlatFileRowParseResult::NO_STATUS:
                try {
                    convert(lastResult, dataTuple);
                    isRowPending = true;
                } catch (FennelExcn e) {
                    // log error
                    FENNEL_TRACE(TRACE_SEVERE,
                        "error executing calculator: " << e.getMessage());
                }
                break;
            default:
                permAssert(false);
            }
        }

        if (!pOutAccessor->produceTuple(dataTuple)) {
            return EXECRC_BUF_OVERFLOW;
        }
        next = pParser->scanRowEnd(*pBuffer, lastResult);
        isRowPending = false;
    }
    return EXECRC_QUANTUM_EXPIRED;
}

void FlatFileExecStreamImpl::closeImpl()
{
    releaseResources();
    SingleOutputExecStream::closeImpl();
}

void FlatFileExecStreamImpl::releaseResources()
{
    pBuffer->close();
}

FENNEL_END_CPPFILE("$Id$");

// End FlatFileExecStreamImpl.cpp
