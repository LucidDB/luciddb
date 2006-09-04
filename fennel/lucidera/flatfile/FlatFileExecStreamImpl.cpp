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
#include "fennel/common/FennelResource.h"
#include "fennel/common/SysCallExcn.h"
#include "fennel/device/RandomAccessFileDevice.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/tuple/StoredTypeDescriptor.h"
#include "fennel/tuple/StandardTypeDescriptor.h"

#include "fennel/lucidera/flatfile/FlatFileBinding.h"
#include "fennel/lucidera/flatfile/FlatFileExecStreamImpl.h"

#include "fennel/disruptivetech/calc/CalcCommon.h"
#include "fennel/disruptivetech/xo/CalcExcn.h"

FENNEL_BEGIN_CPPFILE("$Id$");

FlatFileExecStream *FlatFileExecStream::newFlatFileExecStream()
{
    return new FlatFileExecStreamImpl();
}

void FlatFileExecStreamImpl::handleTuple(
    const FlatFileRowParseResult &result,
    TupleData &tuple)
{
    TupleData *pTupleData = NULL;
    if (pCalc) {
        // NOTE: bind calc inputs for real implementation
        // (output tuple is already bound)
        pTupleData = &textTuple;
    } else {
        // NOTE: just return text tuple for fake implementation
        pTupleData = &tuple;
    }

    // Describe array is initialized here, because describe requires
    // an unbounded scan, and the number of fields are not known
    // until the scan is in progress. Note that we use the first row
    // to determine how many field sizes to return, an imperfect guess.
    bool initial = false;
    if (mode == FLATFILE_MODE_DESCRIBE) {
        if (fieldSizes.size() == 0) {
            initial = true;
            fieldSizes.resize(result.offsets.size());
        }
        // Ignore rows with wrong number of columns
        if (fieldSizes.size() != result.offsets.size()) {
            FlatFileRowParseResult detail = result;
            if (detail.offsets.size() > fieldSizes.size()) {
                detail.status = FlatFileRowParseResult::TOO_MANY_COLUMNS;
            } else {
                detail.status = FlatFileRowParseResult::TOO_FEW_COLUMNS;
            }
            logError(detail);
            return;
        }
    }

    // Prepare values for returning
    for (uint i = 0; i < result.offsets.size(); i++) {
        char *value = result.current + result.offsets[i];
        uint size = result.sizes[i];
        uint strippedSize = 0;
        if (value != NULL) {
            strippedSize = pParser->stripQuoting(value, size, false);
        }
        if (mode == FLATFILE_MODE_DESCRIBE) {
            if (initial) {
                fieldSizes[i] = strippedSize;
            } else {
                fieldSizes[i] = max(fieldSizes[i], strippedSize);
            }
            continue;
        }
        if (size == 0) {
            // a value which is empty, (not quoted empty), is null
            (*pTupleData)[i].pData = NULL;
            (*pTupleData)[i].cbData = 0;
        } else {
            (*pTupleData)[i].pData = (PConstBuffer) value;
            // quietly truncate long columns
            strippedSize = std::min(strippedSize, textDesc[i].cbStorage);
            (*pTupleData)[i].cbData = strippedSize;
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

    if (mode != FLATFILE_MODE_DESCRIBE) {
        isRowPending = true;
    } else {
        // in describe mode, use this to count how many rows have been
        // read, though this is an abuse of the variable's intended purpose
        nRowsOutput++;
    }
}

void FlatFileExecStreamImpl::describeStream(TupleData &tupleData)
{
    if (fieldSizes.size() == 0) {
        throw FennelExcn(
            FennelResource::instance().flatfileDescribeFailed(dataFilePath) );
    }
    
    std::ostringstream oss;
    for (int i = 0; i < fieldSizes.size(); i++) {
        oss << fieldSizes[i];
        if (i != fieldSizes.size() - 1) {
            oss << " ";
        }
    }
    // NOTE: this newly created string is saved as part of the stream 
    // to avoid being popped off the stack
    describeResult = oss.str();
    const char *value = describeResult.c_str();
    uint cbValue = describeResult.size() * sizeof(char);

    assert(tupleData.size() == 1);
    tupleData[0].pData = (PConstBuffer) value;
    tupleData[0].cbData = cbValue;
    isRowPending = true;
}

void FlatFileExecStreamImpl::logError(const FlatFileRowParseResult &result)
{
    switch (result.status) {   
    case FlatFileRowParseResult::INCOMPLETE_COLUMN:
        reason = FennelResource::instance().incompleteColumn();
        break;
    case FlatFileRowParseResult::ROW_TOO_LARGE:
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

void FlatFileExecStreamImpl::logError(
    const std::string reason,
    const FlatFileRowParseResult &result)
{
    
    this->reason = reason;
    std::string rowText =
        std::string(result.current, result.next-result.current);

    if (! logging) {
        return;
    }
    if (! pErrorFile) {
        DeviceMode openMode;
        openMode.create = 1;
        try {
            pErrorFile.reset(
                new RandomAccessFileDevice(errorFilePath, openMode));
        } catch (SysCallExcn e) {
            FENNEL_TRACE(TRACE_SEVERE, e.getMessage());
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
    char *data = const_cast<char *>(record.c_str());
    FlatFileBinding binding(errorFilePath, data, targetSize);
    writeRequest.bindingList.push_back(binding);
    pErrorFile->transfer(writeRequest);
    filePosition += targetSize;
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
    if (pBuffer->isDone() && lastResult.nRowDelimsRead == 0) {
        throw FennelExcn(
            FennelResource::instance().noRowDelimiter(dataFilePath));
    }
}

// FIXME: this method should leverage existing CalcStream code
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

    mode = params.mode;
    rowDesc = readTupleDescriptor(pOutAccessor->getTupleDesc());
    pBuffer.reset(new FlatFileBuffer(params.dataFilePath));
    pParser.reset(new FlatFileParser(
                      params.fieldDelim, params.rowDelim,
                      params.quoteChar, params.escapeChar));

    numRowsScan = params.numRowsScan;

    if (params.calcProgram.size() == 0) {
        textDesc = params.outputTupleDesc;
        return;
    }
    
    // Initialize calculator if a program was specified
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
            rowDesc.push_back(FlatFileColumnDescriptor(attr.cbStorage));
        } else {
            rowDesc.push_back(
                FlatFileColumnDescriptor(FLAT_FILE_MAX_NON_CHAR_VALUE_LEN));
        }
    }
    if (mode == FLATFILE_MODE_DESCRIBE) {
        rowDesc.setUnbounded();
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
        bufferLock.allocatePage();
        uint cbPageSize = bufferLock.getPage().getCache().getPageSize();
        pBufferStorage = bufferLock.getPage().getWritableData();
        pBuffer->setStorage((char*)pBufferStorage, cbPageSize);
    }
    pBuffer->open();
    pBuffer->read();
    next = pBuffer->getReadPtr();
    isRowPending = false;
    nRowsOutput = nRowErrors = 0;
    lastResult.reset();

    if (header) {
        FlatFileRowDescriptor headerDesc;
        for (uint i=0; i < rowDesc.size(); i++) {
            headerDesc.push_back(
                FlatFileColumnDescriptor(
                    FLAT_FILE_MAX_COLUMN_NAME_LEN));
        }
        pParser->scanRow(
            pBuffer->getReadPtr(), pBuffer->getSize(), headerDesc, lastResult);
        pBuffer->setReadPtr(lastResult.next);
        if (lastResult.status != FlatFileRowParseResult::NO_STATUS) {
            logError(lastResult);
            try {
                checkRowDelimiter();
            } catch (FennelExcn e) {
                reason = e.getMessage();
            }
            throw FennelExcn(
                FennelResource::instance().flatfileNoHeader(
                    dataFilePath, reason));
        }
    }

    done = false;
}

ExecStreamResult FlatFileExecStreamImpl::execute(
    ExecStreamQuantum const &quantum)
{
    // detect whether scan was previously finished
    if (done && !isRowPending) {
        pOutAccessor->markEOS();
        return EXECRC_EOS;
    }
    // detect whether output buffer is capable of accepting more data
    if (pOutAccessor->getState() == EXECBUF_OVERFLOW
        || pOutAccessor->getState() == EXECBUF_EOS) {
        return EXECRC_BUF_OVERFLOW;
    }

    // read up to the number of (good or bad) tuples specified by quantum
    for (uint nTuples=0; nTuples < quantum.nTuplesMax;) {
        // ready the next row for output
        while (!isRowPending) {
            // check quantum, since this loop doesn't break until a good
            // row is read
            if (nTuples >= quantum.nTuplesMax) {
                break;
            }

            if ((numRowsScan > 0 && numRowsScan == nRowsOutput)
                || pBuffer->isDone())
            {
                done = true;
                break;
            }
            pParser->scanRow(
                pBuffer->getReadPtr(),pBuffer->getSize(),rowDesc,lastResult);
            nTuples++;
            
            switch (lastResult.status) {
            case FlatFileRowParseResult::INCOMPLETE_COLUMN:
                if (pBuffer->isFull()) {
                    lastResult.status = FlatFileRowParseResult::ROW_TOO_LARGE;
                } else if (!pBuffer->isComplete()) {
                    pBuffer->read();
                    continue;
                }
            case FlatFileRowParseResult::NO_COLUMN_DELIM:
            case FlatFileRowParseResult::TOO_FEW_COLUMNS:
            case FlatFileRowParseResult::TOO_MANY_COLUMNS:
                logError(lastResult);
                nRowErrors++;
                pBuffer->setReadPtr(lastResult.next);
                continue;
            case FlatFileRowParseResult::NO_STATUS:
                try {
                    handleTuple(lastResult, dataTuple);
                    pBuffer->setReadPtr(lastResult.next);
                } catch (CalcExcn e) {
                    logError(e.getMessage(), lastResult);
                    nRowErrors++;
                    pBuffer->setReadPtr(lastResult.next);
                    continue;
                }
                break;
            default:
                permAssert(false);
            }
        }

        // describe produces one row after it's done reading input
        if (mode == FLATFILE_MODE_DESCRIBE && done && !isRowPending) {
            describeStream(dataTuple);
        }

        // try to output pending rows
        if (isRowPending) {
            if (!pOutAccessor->produceTuple(dataTuple)) {
                return EXECRC_BUF_OVERFLOW;
            }
            isRowPending = false;
            nRowsOutput++;
        }

        // close stream if no more rows are available
        if (done) {
            detectMajorErrors();
            pOutAccessor->markEOS();
            return EXECRC_EOS;
        }
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
    if (pErrorFile) {
        pErrorFile->flush();
        pErrorFile.reset();
    }
}

FENNEL_END_CPPFILE("$Id$");

// End FlatFileExecStreamImpl.cpp
