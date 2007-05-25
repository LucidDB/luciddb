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

FENNEL_BEGIN_CPPFILE("$Id$");

FlatFileExecStream *FlatFileExecStream::newFlatFileExecStream()
{
    return new FlatFileExecStreamImpl();
}

// NOTE: keep this consistent with the Farrago java file
//   com.lucidera.farrago.namespace.flatfile.FlatFileFennelRel.java 
const uint FlatFileExecStreamImpl::MAX_ROW_ERROR_TEXT_WIDTH = 4000;

// TODO: remove Fennel calc code and linking
void FlatFileExecStreamImpl::prepare(
    FlatFileExecStreamParams const &params)
{
    SingleOutputExecStream::prepare(params);

    header = params.header;
    dataFilePath = params.dataFilePath;
    lenient = params.lenient;
    trim = params.trim;
    mapped = params.mapped;
    columnNames = params.columnNames;
    
    dataTuple.compute(pOutAccessor->getTupleDesc());
    
    scratchAccessor = params.scratchAccessor;
    bufferLock.accessSegment(scratchAccessor);

    mode = params.mode;
    rowDesc = readTupleDescriptor(pOutAccessor->getTupleDesc());
    rowDesc.setLenient(lenient);
    pBuffer.reset(new FlatFileBuffer(params.dataFilePath));
    pParser.reset(new FlatFileParser(
                      params.fieldDelim, params.rowDelim,
                      params.quoteChar, params.escapeChar,
                      params.trim));

    numRowsScan = params.numRowsScan;
    textDesc = params.outputTupleDesc;
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
        headerDesc.setLenient(lenient);
        if (mapped) {
            headerDesc.setUnbounded();
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

        // Generate mapping from text file columns to output columns.
        // Match names in the header with output field names. Names in
        // the header are always trimmed.
        if (mapped) {
            if (! lenient) {
                throw FennelExcn(
                    FennelResource::instance()
                    .flatfileMappedRequiresLenient());
            }

            pParser->stripQuoting(lastResult, true);
            uint nFields = lastResult.getReadCount();
            int found = 0;

            std::vector<uint> columnMap;
            columnMap.resize(nFields);
            for (uint i = 0; i < nFields; i++) {
                std::string name(
                    lastResult.getColumn(i),
                    lastResult.getColumnSize(i));
                columnMap[i] = findField(name);
                if (columnMap[i] != -1) {
                    found++;
                }
            }
            if (found == 0) {
                throw FennelExcn(
                    FennelResource::instance().flatfileNoMappedColumns(
                        std::string(" "),
                        std::string(" ")));
            }            
            rowDesc.setMap(columnMap);
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
                handleTuple(lastResult, dataTuple);
                pBuffer->setReadPtr(lastResult.next);
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
            pOutAccessor->markEOS();
            return EXECRC_EOS;
        }
    }
    return EXECRC_QUANTUM_EXPIRED;
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

int FlatFileExecStreamImpl::findField(const std::string &name)
{
    for (uint i = 0; i < columnNames.size(); i++) {
        if (strcasecmp(name.c_str(), columnNames[i].c_str()) == 0) {
            return i;
        }
    }
    return -1;
}

void FlatFileExecStreamImpl::handleTuple(
    FlatFileRowParseResult &result,
    TupleData &tuple)
{
    TupleData *pTupleData = &tuple;

    // Describe array is initialized here, because describe requires
    // an unbounded scan, and the number of fields are not known
    // until the scan is in progress. Note that we use the first row
    // to determine how many field sizes to return, an imperfect guess.
    if (mode == FLATFILE_MODE_DESCRIBE) {
        if (fieldSizes.size() == 0) {
            fieldSizes.resize(result.getReadCount(), 0);
        }
        // If not lenient, check for rows with wrong number of columns
        if ((!lenient) && fieldSizes.size() != result.getReadCount()) {
            FlatFileRowParseResult detail = result;
            if (detail.getReadCount() > fieldSizes.size()) {
                detail.status = FlatFileRowParseResult::TOO_MANY_COLUMNS;
            } else {
                detail.status = FlatFileRowParseResult::TOO_FEW_COLUMNS;
            }
            logError(detail);
            return;
        }
    }

    // Prepare values for returning
    pParser->stripQuoting(result, trim);
    for (uint i = 0; i < result.getReadCount(); i++) {
        if (mode == FLATFILE_MODE_DESCRIBE) {
            if (i < fieldSizes.size()) {
                fieldSizes[i] = max(fieldSizes[i], result.getColumnSize(i));
            }
            continue;
        }
        (*pTupleData)[i].pData = (PConstBuffer) result.getColumn(i);
        // quietly truncate long columns
        (*pTupleData)[i].cbData =
            std::min(result.getColumnSize(i), textDesc[i].cbStorage);
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

    // initialize logging objects
    if (errorDesc.size() == 0) {
        // TODO: get project specific type factory
        StandardTypeDescriptorFactory typeFactory;
        StoredTypeDescriptor const &typeDesc =
            typeFactory.newDataType(STANDARD_TYPE_VARCHAR);
        bool nullable = true;

        errorDesc.push_back(
            TupleAttributeDescriptor(
                typeDesc,
                nullable,
                MAX_ROW_ERROR_TEXT_WIDTH));

        errorTuple.compute(errorDesc);
    }

    uint length = result.next-result.current;
    length = std::min(length, MAX_ROW_ERROR_TEXT_WIDTH);
    errorTuple[0].pData = (PConstBuffer) result.current;
    errorTuple[0].cbData = length;

    postError(ROW_ERROR, reason, errorDesc, errorTuple, -1);
}

void FlatFileExecStreamImpl::checkRowDelimiter()
{
    if (pBuffer->isDone() && lastResult.nRowDelimsRead == 0) {
        throw FennelExcn(
            FennelResource::instance().noRowDelimiter(dataFilePath));
    }
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
