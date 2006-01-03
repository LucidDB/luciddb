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

#include "fennel/common/FennelResource.h"
#include "fennel/common/SysCallExcn.h"
#include "fennel/device/RandomAccessFileDevice.h"
#include "fennel/device/RandomAccessRequest.h"
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
            FENNEL_TRACE(TRACE_SEVERE,
                "error executing calculator: " << e.getMessage());
            throw e;
        }
        if (pCalc->mWarnings.begin() != pCalc->mWarnings.end()) {
            throw CalcExcn(pCalc->warnings(), textDesc, textTuple);
        }    
    }
}

void FlatFileExecStreamImpl::logError(const FlatFileRowParseResult &result)
{
    switch (result.status) {   
    case FlatFileRowParseResult::INCOMPLETE_COLUMN:
        reason = FennelResource::instance().incompleteColumn();
        break;
    case FlatFileRowParseResult::COLUMN_TOO_LARGE:
        reason = FennelResource::instance().rowTextTooLong();
        break;
    case FlatFileRowParseResult::NO_COLUMN_DELIM:
        reason = FennelResource::instance().noColumnDelimiter();
        break;
    case FlatFileRowParseResult::TOO_FEW_COLUMNS:
        reason = FennelResource::instance().tooFewColumns();
        break;
    case FlatFileRowParseResult::TOO_MANY_COLUMNS:
        reason = FennelResource::instance().tooManyColumns();
        break;
    default:
        permAssert(false);
    }
    logError(reason, result);
}

/**
 * Specifies parameters for flat file read requests
 */
class FlatFileBinding : public RandomAccessRequestBinding
{
    std::string path;
    const char *buffer;
    uint bufferSize;
    
public:
    FlatFileBinding(std::string &path, const char *buf, uint size) 
    {
        this->path = path;
        buffer = buf;
        bufferSize = size;
    }
        
    PBuffer getBuffer() const { return (PBuffer) buffer; }
    uint getBufferSize() const { return bufferSize; }
    void notifyTransferCompletion(bool bSuccess) {
        if (!bSuccess) {
            throw FennelExcn(FennelResource::instance().dataTransferFailed(
                                 path, bufferSize));
        }
    }
};

void FlatFileExecStreamImpl::logError(
    const std::string reason,
    const FlatFileRowParseResult &result)
{
    this->reason = reason;
    std::string rowText =
        std::string(result.current, result.next-result.current);
    if (logging) {
        if (! pErrorFile) {
            DeviceMode openMode;
            openMode.create = 1;
            try {
                pErrorFile.reset(
                    new RandomAccessFileDevice(errorFilePath, openMode));
            } catch (SysCallExcn e) {
                FENNEL_TRACE(TRACE_FINE, e.getMessage());
                throw FennelExcn(
                    FennelResource::instance().writeLogFailed(errorFilePath));
            }
            filePosition = pErrorFile->getSizeInBytes();
        }

        std::ostringstream oss;
        oss << reason << ", " << rowText << endl;
        std::string record = oss.str();
        uint targetSize = record.size()*sizeof(char);
            
        pErrorFile->setSizeInBytes(filePosition + targetSize);
        RandomAccessRequest writeRequest;
        writeRequest.pDevice = pErrorFile.get();
        writeRequest.cbOffset = filePosition;
        writeRequest.cbTransfer = targetSize;
        writeRequest.type = RandomAccessRequest::WRITE;
        FlatFileBinding binding(errorFilePath, record.c_str(), targetSize);
        writeRequest.bindingList.push_back(binding);
        pErrorFile->transfer(writeRequest);
        pErrorFile->flush();
        filePosition += targetSize;
    }
}

void FlatFileExecStreamImpl::detectMajorErrors()
{
    if (nRowsOutput > 0 && nRowErrors > 0) {
        // TODO: we probably shouldn't throw an error here, but we should
        // warn user that errors were encountered and were written to log
        //throw FennelExcn(FennelResource::instance().errorsEncountered(
        //                     dataFilePath, errorFilePath));
    }
    if (nRowsOutput > 0 || nRowErrors == 0) return;
    checkRowDelimiter();
    // REVIEW: perhaps we shouldn't throw an error here. If the data being
    // read is not crucial, we may want to swallow this.
    throw FennelExcn(
        FennelResource::instance().noRowsReturned(dataFilePath, reason));
}

void FlatFileExecStreamImpl::checkRowDelimiter()
{
    if (lastResult.nRowDelimsRead == 0) {
        throw FennelExcn(
            FennelResource::instance().noRowDelimiter(dataFilePath));
    }
}

void FlatFileExecStreamImpl::prepare(
    FlatFileExecStreamParams const &params)
{
    SingleOutputExecStream::prepare(params);

    header = params.header;
    logging = (params.errorFilePath.size() > 0);
    dataFilePath = params.dataFilePath;
    errorFilePath = params.errorFilePath;
    
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
        FENNEL_TRACE(TRACE_SEVERE, "error preparing calculator: "
            << e.getMessage());
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
                                  FLAT_FILE_MAX_NON_CHAR_VALUE_LEN));
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
        if (cbPageSize < cbTupleMax) {
            throw FennelExcn(FennelResource::instance().rowTypeTooLong(
                                 cbTupleMax, cbPageSize));
        }
        pBufferStorage = bufferLock.getPage().getWritableData();
        pBuffer->setStorage((char*)pBufferStorage, cbPageSize);
    }
    pBuffer->open();
    pBuffer->fill();
    next = pBuffer->buf();
    isRowPending = false;
    nRowsOutput = nRowErrors = 0;
    lastResult.reset();

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
        checkRowDelimiter();
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
            if (pBuffer->readCompleted() && (next >= pBuffer->contentEnd())) {
                detectMajorErrors();
                pOutAccessor->markEOS();
                return EXECRC_EOS;
            }
            next = pParser->scanRow(*pBuffer, next, rowDesc, lastResult);
            switch (lastResult.status) {
            case FlatFileRowParseResult::INCOMPLETE_COLUMN:
            case FlatFileRowParseResult::COLUMN_TOO_LARGE:
            case FlatFileRowParseResult::NO_COLUMN_DELIM:
            case FlatFileRowParseResult::TOO_FEW_COLUMNS:
            case FlatFileRowParseResult::TOO_MANY_COLUMNS:
                logError(lastResult);
                next = pParser->scanRowEnd(*pBuffer, lastResult);
                nRowErrors++;
                continue;
            case FlatFileRowParseResult::NO_STATUS:
                try {
                    convert(lastResult, dataTuple);
                    isRowPending = true;
                } catch (CalcExcn e) {
                    logError(e.getMessage(), lastResult);
                    next = pParser->scanRowEnd(*pBuffer, lastResult);
                    nRowErrors++;
                    continue;
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
        nRowsOutput++;
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
    pErrorFile.reset();
}

FENNEL_END_CPPFILE("$Id$");

// End FlatFileExecStreamImpl.cpp
