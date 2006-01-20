/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2006-2006 LucidEra, Inc.
// Copyright (C) 2006-2006 The Eigenbase Project
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
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/lucidera/bitmap/LbmGeneratorExecStream.h"
#include "fennel/lucidera/bitmap/LbmSplicerExecStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void LbmSplicerExecStream::prepare(LbmSplicerExecStreamParams const &params)
{
    BTreeExecStream::prepare(params);
    ConduitExecStream::prepare(params);

    assert(treeDescriptor.tupleDescriptor == pInAccessor->getTupleDesc());
    bitmapTupleDesc = treeDescriptor.tupleDescriptor;
    bTreeTupleData.compute(bitmapTupleDesc);
    inputTuple.compute(bitmapTupleDesc);
    nIdxKeys = treeDescriptor.keyProjection.size() - 1;

    dynParamId = params.dynParamId;

    uint minEntrySize;

    LbmEntry::getSizeBounds(
        bitmapTupleDesc,
        treeDescriptor.segmentAccessor.pSegment->getUsablePageSize(),
        minEntrySize,
        maxEntrySize);

    // setup output tuple and accessor
    outputTuple.compute(pOutAccessor->getTupleDesc());
    outputTuple[0].pData = (PConstBuffer) &numRowsLoaded;
    outputTupleAccessor = &pOutAccessor->getScratchTupleAccessor();
}

void LbmSplicerExecStream::open(bool restart)
{
    BTreeExecStream::open(restart);
    ConduitExecStream::open(restart);

    if (!restart) {

        bitmapBuffer.reset(new FixedBuffer[maxEntrySize]);
        pCurrentEntry = SharedLbmEntry(new LbmEntry());
        pCurrentEntry->init(
            bitmapBuffer.get(), maxEntrySize, bitmapTupleDesc);

        bTreeWriter = SharedBTreeWriter(
            new BTreeWriter(treeDescriptor, scratchAccessor, false));

        outputTupleBuffer.reset(
            new(FixedBuffer[outputTupleAccessor->getMaxByteCount()]));
    }
    isDone = false;
    currEntry = false;
    currExistingEntry = false;
    if (bTreeWriter->searchFirst() == false) {
        bTreeWriter->endSearch();
        emptyTable = true;
        // switch writer to monotonic now that we know the table
        // is empty
        bTreeWriter.reset();
        bTreeWriter = SharedBTreeWriter(
            new BTreeWriter(treeDescriptor, scratchAccessor, true));
    } else {
        emptyTable = false;
    }
}

void LbmSplicerExecStream::getResourceRequirements(
    ExecStreamResourceQuantity &minQuantity,
    ExecStreamResourceQuantity &optQuantity)
{
    ConduitExecStream::getResourceRequirements(minQuantity, optQuantity);

    // btree pages
    minQuantity.nCachePages += 5;

    optQuantity = minQuantity;
}

ExecStreamResult LbmSplicerExecStream::execute(ExecStreamQuantum const &quantum)
{
    if (isDone) {
        pOutAccessor->markEOS();
        return EXECRC_EOS;
    }

    // no more input; write out last bitmap entry and produce final row count,
    // which is stored in a dynamic parameter set upstream
    
    if (pInAccessor->getState() == EXECBUF_EOS) {
        if (currEntry) {
            insertBitmapEntry();
        }
        numRowsLoaded = *reinterpret_cast<RecordNum const *>(
            pDynamicParamManager->getParam(dynParamId).getDatum().pData);
        outputTupleAccessor->marshal(outputTuple, outputTupleBuffer.get());
        pOutAccessor->provideBufferForConsumption(
            outputTupleBuffer.get(),
            outputTupleBuffer.get() +
                outputTupleAccessor->getCurrentByteCount());
        isDone = true;
        return EXECRC_BUF_OVERFLOW;
    }

    for (uint i = 0; i < quantum.nTuplesMax; i++) {
        if (!pInAccessor->demandData()) {
            return EXECRC_BUF_UNDERFLOW;
        }

        pInAccessor->unmarshalTuple(inputTuple);

        FENNEL_TRACE(TRACE_FINE, "input Tuple from sorter");
        FENNEL_TRACE(TRACE_FINE, LbmEntry::toString(inputTuple));

        if (!currEntry) {

            // if the key already exists in the index, splice the
            // entry just read to the existing btree entry
            if (existingEntry(inputTuple)) {
                spliceEntry(inputTuple);
            }
        } else {

            // Compare the key values of the currentEntry with the
            // input tuple.  If they're the same, try splicing with
            // currentEntry.  Otherwise, write out currentEntry and
            // set currentEntry to the new input
            int keyComp = pCurrentEntry->compareEntry(
                inputTuple, bitmapTupleDesc);
            assert(keyComp <= 0);
            if (keyComp == 0) {
                spliceEntry(inputTuple);
            } else {
                insertBitmapEntry();
                if (existingEntry(inputTuple)) {
                    spliceEntry(inputTuple);
                }
            }
        }
        pInAccessor->consumeTuple();
    }

    return EXECRC_QUANTUM_EXPIRED;
}

void LbmSplicerExecStream::closeImpl()
{
    BTreeExecStream::closeImpl();
    ConduitExecStream::closeImpl();
    bitmapBuffer.reset();
    pCurrentEntry.reset();
    bTreeWriter.reset();
    outputTupleBuffer.reset();
}

ExecStreamBufProvision LbmSplicerExecStream::getOutputBufProvision() const
{
    return BUFPROV_PRODUCER;
}

bool LbmSplicerExecStream::existingEntry(TupleData const &bitmapEntry)
{
    if (!emptyTable) {
        // if an exact match isn't found, make sure we at least match the
        // first part of the key up till the rid
        bool match = bTreeWriter->searchForKey(
            bitmapEntry, DUP_SEEK_BEGIN, false);
        bTreeWriter->getTupleAccessorForRead().unmarshal(bTreeTupleData);
        if (match == false) {
            int keyComp = bitmapTupleDesc.compareTuplesKey(
                bTreeTupleData, bitmapEntry, nIdxKeys);
            assert(keyComp <= 0);
            if (keyComp == 0) {
                match = true;
            }
        }

        // current bitmap entry becomes the existing btree entry
        if (match == true) {
            currExistingEntry = true;
            createNewBitmapEntry(bTreeTupleData);
            return true;
        }
    }

    // set current bitmap entry to new entry
    currExistingEntry = false;
    createNewBitmapEntry(bitmapEntry);
    return false;
}

void LbmSplicerExecStream::spliceEntry(TupleData &bitmapEntry)
{
     FENNEL_TRACE(TRACE_FINE, "splice two entries");
     FENNEL_TRACE(TRACE_FINE, pCurrentEntry->toString());
     FENNEL_TRACE(TRACE_FINE, LbmEntry::toString(bitmapEntry));

    if (!pCurrentEntry->mergeEntry(bitmapEntry)) {
        insertBitmapEntry();
        createNewBitmapEntry(bitmapEntry);
    }
}

void LbmSplicerExecStream::insertBitmapEntry()
{
    // implement btree updates as deletes followed by inserts
    if (currExistingEntry) {
        bTreeWriter->deleteCurrent();
        currExistingEntry = false;
    }

    TupleData const &indexTuple = pCurrentEntry->produceEntryTuple();

     FENNEL_TRACE(TRACE_FINE, "insert Tuple into BTree");
     FENNEL_TRACE(TRACE_FINE, LbmEntry::toString(indexTuple));

    bTreeWriter->insertTupleData(indexTuple, DUP_FAIL);
}

void LbmSplicerExecStream::createNewBitmapEntry(TupleData const &bitmapEntry)
{
    pCurrentEntry->setEntryTuple(bitmapEntry);
    currEntry = true;
}

FENNEL_END_CPPFILE("$Id$");

// End LbmSplicerExecStream.cpp
