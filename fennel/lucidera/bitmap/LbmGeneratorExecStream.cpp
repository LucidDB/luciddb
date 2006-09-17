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

FENNEL_BEGIN_CPPFILE("$Id$");

LbmGeneratorExecStream::LbmGeneratorExecStream()
{
    dynParamsCreated = false;
}

void LbmGeneratorExecStream::prepare(LbmGeneratorExecStreamParams const &params)
{
    BTreeExecStream::prepare(params);
    LcsRowScanBaseExecStream::prepare(params);

    dynParamId = params.dynParamId;
    assert(opaqueToInt(dynParamId) > 0);

    createIndex = params.createIndex;

    scratchLock.accessSegment(scratchAccessor);
    scratchPageSize = scratchAccessor.pSegment->getUsablePageSize();

    // setup input tuple
    assert(inAccessors[0]->getTupleDesc().size() == 2);
    inputTuple.compute(inAccessors[0]->getTupleDesc());

    // setup tuple used to store key values read from clusters
    bitmapTuple.computeAndAllocate(pOutAccessor->getTupleDesc());

    // setup output tuple
    assert(treeDescriptor.tupleDescriptor == pOutAccessor->getTupleDesc());
    bitmapTupleDesc = treeDescriptor.tupleDescriptor;

    nIdxKeys = treeDescriptor.keyProjection.size() - 1;

    // determine min and max size of bitmap entries

    LbmEntry::getSizeBounds(
        bitmapTupleDesc, 
        treeDescriptor.segmentAccessor.pSegment->getUsablePageSize(),
        minBitmapSize,
        maxBitmapSize);
}

void LbmGeneratorExecStream::open(bool restart)
{
    BTreeExecStream::open(restart);
    LcsRowScanBaseExecStream::open(restart);
    nBitmapEntries = 0;
    flushIdx = 0;
    nBitmapBuffers = 0;
    nScratchPagesAllocated = 0;
    producePending = LBM_NOFLUSH_PENDING;
    skipRead = false;
    rowCount = 0;
    batchRead = false;
    if (!restart) {
        pDynamicParamManager->createParam(
            dynParamId, inAccessors[0]->getTupleDesc()[0]);
        dynParamsCreated = true;
    }
}

void LbmGeneratorExecStream::getResourceRequirements(
    ExecStreamResourceQuantity &minQuantity,
    ExecStreamResourceQuantity &optQuantity,
    ExecStreamResourceSettingType &optType)
{
    BTreeExecStream::getResourceRequirements(minQuantity, optQuantity);
    LcsRowScanBaseExecStream::getResourceRequirements(minQuantity, optQuantity);
    numMiscScratchPages = minQuantity.nCachePages;

    // need a minimum of one scratch pages for constructing LbmEntry's
    minQuantity.nCachePages += 1;

    // If this is a multi-key index, then we're only creating singleton
    // LbmEntry's and therefore, don't need multiple scratch pages.
    // Otherwise, we ideally want to set the number of scratch pages
    // based on the max number of distinct values in compressed batches.
    // Since we don't have that information, we'll use an "estimate" of
    // 10 pages.
    if (nIdxKeys > 1) {
        optQuantity.nCachePages += 1;
        optType = EXEC_RESOURCE_ACCURATE;
    } else {
        optQuantity.nCachePages += 11;
        optType = EXEC_RESOURCE_ESTIMATE;
    }
}

void LbmGeneratorExecStream::setResourceAllocation(
    ExecStreamResourceQuantity &quantity)
{
    BTreeExecStream::setResourceAllocation(quantity);
    LcsRowScanBaseExecStream::setResourceAllocation(quantity);

    maxNumScratchPages = quantity.nCachePages - numMiscScratchPages;
}

ExecStreamResult LbmGeneratorExecStream::execute(
    ExecStreamQuantum const &quantum)
{
    // read the start rid and num of rows to load
    
    if (inAccessors[0]->getState() != EXECBUF_EOS) {

        if (!inAccessors[0]->demandData()) {
            return EXECRC_BUF_UNDERFLOW;
        }

        inAccessors[0]->unmarshalTuple(inputTuple);

        // in the case of create index, the number of rows affected
        // is returned as 0, since the statement is a DDL
        if (createIndex) {
            numRowsToLoad = 0;
            startRid = LcsRid(0);
        } else {
            numRowsToLoad =
                *reinterpret_cast<RecordNum const *> (inputTuple[0].pData);
            startRid = 
                *reinterpret_cast<LcsRid const *> (inputTuple[1].pData);
        }
        currRid = startRid;

        // set number of rows to load in a dynamic parameter that
        // splicer will later read
        pDynamicParamManager->writeParam(dynParamId, inputTuple[0]);
        inAccessors[0]->consumeTuple();

        // position to the starting rid
        for (uint iClu = 0; iClu < nClusters; iClu++) {

            SharedLcsClusterReader &pScan = pClusters[iClu];
            if (!pScan->position(startRid)) {
                // empty table
                pOutAccessor->markEOS();
                return EXECRC_EOS;
            }
            syncColumns(pScan);
        }

        // initialize bitmap table to a single entry, assuming we're
        // starting with singleton bitmaps
        initBitmapTable(1);
    }

    if (pOutAccessor->getState() == EXECBUF_EOS) {
        return EXECRC_EOS;
    }

    // take care of any pending flushes first
    
    switch (producePending) {
    case LBM_ENTRYFLUSH_PENDING:
        // outputTuple already contains the pending tuple to be flushed
        if (!pOutAccessor->produceTuple(outputTuple)) {
            return EXECRC_BUF_OVERFLOW;
        }
        bitmapTable[flushStart].inuse = false;
        break;
    case LBM_TABLEFLUSH_PENDING:
        if (!flushTable(flushStart)) {
            return EXECRC_BUF_OVERFLOW;
        }
        break;
    default:
        break;
    }
    producePending = LBM_NOFLUSH_PENDING;

    ExecStreamResult rc;

    if (nIdxKeys == 1) {
        rc = generateSingleKeyBitmaps(quantum);
    } else {
        rc = generateMultiKeyBitmaps(quantum);
    }

    switch (rc) {
    case EXECRC_BUF_OVERFLOW:
    case EXECRC_QUANTUM_EXPIRED:
        return rc;
    case EXECRC_EOS:
        // no more rows to process
        if (!createIndex) {
            assert(rowCount == numRowsToLoad);
        }
        pOutAccessor->markEOS();
        return EXECRC_EOS;
    default:
        permAssert(false);
    }
}

ExecStreamResult LbmGeneratorExecStream::generateSingleKeyBitmaps(
    ExecStreamQuantum const &quantum)
{
    // read from the current batch until either the end of the batch
    // is reached, or there is an overflow in a write to the output stream
    for (uint i = 0; i < quantum.nTuplesMax; i++) {
        if (pClusters[0]->clusterCols[0].batchIsCompressed()) {
            if (!generateBitmaps()) {
                return EXECRC_BUF_OVERFLOW;
            }
        } else if (!batchRead) {
            if (!generateSingletons()) {
                return EXECRC_BUF_OVERFLOW;
            }
        }

        // move to the next batch
        batchRead = false;
        SharedLcsClusterReader &pScan = pClusters[0];
        if (!pScan->nextRange()) {
            return EXECRC_EOS;
        }
        pScan->clusterCols[0].sync();
    }
    return EXECRC_QUANTUM_EXPIRED;
}

ExecStreamResult LbmGeneratorExecStream::generateMultiKeyBitmaps(
    ExecStreamQuantum const &quantum)
{
    // read through all rows until the end of the table has been reached,
    // or there is an overflow in a write to the output stream
    for (uint i = 0; i < quantum.nTuplesMax; i++) {
        uint prevClusterEnd = 0;
        if (!skipRead) {
            // reset buffer before loading new values, in case previous
            // row contained nulls
            bitmapTuple.resetBuffer();
            for (uint iClu = 0; iClu < nClusters; iClu++) {

                SharedLcsClusterReader &pScan = pClusters[iClu];

                if (currRid >= pScan->getRangeEndRid()) {

                    // move to the next batch if this particular cluster
                    // reader has reached the end of its batch
                    if (!pScan->nextRange()) {
                        assert(
                            iClu == 0 &&
                                (nClusters == 1 || !pClusters[1]->nextRange()));
                        return EXECRC_EOS;
                    }
                    assert(
                        currRid >= pScan->getRangeStartRid() &&
                        currRid < pScan->getRangeEndRid());
                    syncColumns(pScan);
                } else {
                    assert(currRid >= pScan->getRangeStartRid());
                    pScan->advanceWithinBatch(
                        opaqueToInt(currRid - pScan->getCurrentRid()));
                }
                readColVals(pScan, bitmapTuple, prevClusterEnd);
                prevClusterEnd += pScan->nColsToRead;
            }
        }

        createSingletonBitmapEntry();
        if (!flushEntry(0)) {
            return EXECRC_BUF_OVERFLOW;
        }
    }
    return EXECRC_QUANTUM_EXPIRED;
}

void LbmGeneratorExecStream::createSingletonBitmapEntry()
{
    // create the singleton bitmap entry and then flush it out
    // right away; should never fail trying to create a singleton
    // entry
    initRidAndBitmap(bitmapTuple, &currRid);
    bool rc = addRidToBitmap(0, bitmapTuple, currRid);
    assert(rc);
    skipRead = false;
    ++currRid;
    ++rowCount;
}

void LbmGeneratorExecStream::closeImpl()
{
    BTreeExecStream::closeImpl();
    LcsRowScanBaseExecStream::closeImpl();
    if (dynParamsCreated) {
        pDynamicParamManager->deleteParam(dynParamId);
    }
    keyCodes.clear();
    bitmapTable.clear();
    scratchPages.clear();

    if (scratchAccessor.pSegment) {
        scratchAccessor.pSegment->deallocatePageRange(
            NULL_PAGE_ID, NULL_PAGE_ID);
    }
}

bool LbmGeneratorExecStream::generateBitmaps()
{
    // in the single key case, the column reader is always the first
    // one, in the first cluster reader
    LcsColumnReader &colReader = pClusters[0]->clusterCols[0];
    uint nDistinctVals = colReader.getBatchValCount();

    // only read rows beginning at startRid
    uint nRows = pClusters[0]->getRangeRowsLeft();

    // if first time through, setup the keycode array and read the batch
    if (!batchRead) {
        uint nRead;

        initBitmapTable(nDistinctVals);
        keyCodes.resize(nRows);
        colReader.readCompressedBatch(nRows, &keyCodes[0], &nRead);
        assert(nRows == nRead);

        batchRead = true;
        currBatch = 0;
    }

    // resume reading batch values based on where we last left off;
    // if the value has been read but not yet processed, skip the read
    for (uint i = currBatch; i < nRows; i++) {
        if (!skipRead) {
            PBuffer curValue = colReader.getBatchValue(keyCodes[i]);
            // reset buffer before loading new value, in case previous
            // row had nulls
            bitmapTuple.resetBuffer();
            bitmapTuple[0].loadLcsDatum(curValue);
            initRidAndBitmap(bitmapTuple, &currRid);
        }
        if (!addRidToBitmap(keyCodes[i], bitmapTuple, currRid)) {
            currBatch = i;
            skipRead = true;
            return false;
        }
        ++currRid;
        ++rowCount;
        skipRead = false;
    }

    // flush out table since the next batch will have a different set
    // of keycodes
    if (!flushTable(0)) {
        // set currBatch to avoid re-reading column values above
        // when return back into this method
        currBatch = nRows;
        return false;
    }

    return true;
}

bool LbmGeneratorExecStream::generateSingletons()
{
    SharedLcsClusterReader &pScan = pClusters[0];

    do {
        // if we've already read the row but haven't processed it yet,
        // skip the read
        if (!skipRead) {
            uint prevClusterEnd = 0;

            // reset buffer before loading new values, in case previous
            // row contained nulls
            bitmapTuple.resetBuffer();

            for (uint iCluCol = 0; iCluCol < pScan->nColsToRead; iCluCol++) {
                PBuffer curValue =
                    pScan->clusterCols[iCluCol].getCurrentValue();
                bitmapTuple[projMap[prevClusterEnd + iCluCol]].
                    loadLcsDatum(curValue);
            }
            prevClusterEnd += pScan->nColsToRead;
        }

        createSingletonBitmapEntry();
        if (!flushEntry(0)) {
            // advance now so the next time we come in here, we'll
            // be correctly positioned on the next rid
            if (!advanceReader(pScan)) {
                // if we're at the end of the batch, avoid coming
                // back in here until the new batch is read
                batchRead = true;
            }
            return false;
        }

        // advance to the next rid; if at the end of the batch,
        // return to caller; else, continue reading from current
        // batch
        if (!advanceReader(pScan))
            return true;
    } while (true);
}

bool LbmGeneratorExecStream::advanceReader(SharedLcsClusterReader &pScan)
{
    if (!pScan->advance(1)) {
        return false;
    } 
    syncColumns(pScan);
    return true;
}

void LbmGeneratorExecStream::initBitmapTable(uint nEntries)
{
    if (nEntries > nBitmapEntries) {

        // resize bitmap table to accomodate new batch,
        // which has more distinct values
        bitmapTable.resize(nEntries);
        for (uint i = nBitmapEntries; i < nEntries; i++) {
            bitmapTable[i].pBitmap = SharedLbmEntry(new LbmEntry());
        }

        // compute the size of the bitmap buffers, based on the number
        // of scratch pages available and the number of distinct values
        // in the batch
        nBufsPerPage = (uint) ceil((double) nEntries / maxNumScratchPages);
        entrySize = scratchPageSize / nBufsPerPage;

        if (entrySize < minBitmapSize) {
            entrySize = minBitmapSize;
            nBufsPerPage = scratchPageSize / entrySize;
        } else if (entrySize > maxBitmapSize) {
            entrySize = maxBitmapSize;
            nBufsPerPage = scratchPageSize / entrySize;
        }
    }

    if (nEntries != nBitmapEntries) {
        // divide up the buffers across the bitmap table; if there are
        // not enough buffers, the bitmap entries at the end of the table
        // won't have assigned buffers yet; no need to re-divide the buffers
        // if the previous bitmap table had the same number of entries

        uint nPages = (uint) ceil((double) nEntries / nBufsPerPage);
        if (nPages > maxNumScratchPages) {
            nPages = maxNumScratchPages;
        }
        while (nPages > nScratchPagesAllocated) {
            scratchLock.allocatePage();
            PBuffer newPage = scratchLock.getPage().getWritableData();
            scratchPages.push_back(newPage);
            ++nScratchPagesAllocated;
        }
        uint idx = 0;
        for (uint i = 0; i < nPages; i++) {
            uint offset = 0;
            for (uint j = 0; j < nBufsPerPage; j++) {
                idx = i * nBufsPerPage + j;
                if (idx == nEntries) {
                    break;
                }
                bitmapTable[idx].bufferPtr = scratchPages[i] + offset;
                offset += entrySize;
            }
            if (idx == nEntries) {
                break;
            }
        }
        // entries without assigned buffers
        for (uint i = idx + 1; i < nEntries; i++) {
            bitmapTable[i].bufferPtr = NULL;
        }
    }

    for (uint i = 0; i < nEntries; i++) {
        bitmapTable[i].inuse = false;
    }
    flushIdx = 0;
    nBitmapEntries = nEntries;
}

void LbmGeneratorExecStream::initRidAndBitmap(
    TupleData &bitmapTuple, LcsRid* pCurrRid)
{
    bitmapTuple[nIdxKeys].pData = (PConstBuffer) pCurrRid;
    bitmapTuple[nIdxKeys+1].pData = NULL;
    bitmapTuple[nIdxKeys+2].pData = NULL;
}

bool LbmGeneratorExecStream::addRidToBitmap(
    uint keycode, TupleData &initBitmap, LcsRid rid)
{
    assert(keycode <= nBitmapEntries);

    if (bitmapTable[keycode].inuse) {
        assert(bitmapTable[keycode].bufferPtr);

        bool maxedOut = !bitmapTable[keycode].pBitmap->setRID(rid);
        if (maxedOut) {
            if (!flushEntry(keycode)) {
                return false;
            }
            assert(!bitmapTable[keycode].inuse);
            bitmapTable[keycode].inuse = true;
            // buffer should now have enough space; create a singleton
            // entry
            bitmapTable[keycode].pBitmap->setEntryTuple(initBitmap);
        }
    } else {

        if (!bitmapTable[keycode].bufferPtr) {
            // no assigned buffer yet; get a buffer by flushing
            // out an existing entry
            PBuffer bufPtr = flushBuffer();
            if (!bufPtr) {
                return false;
            }
            bitmapTable[keycode].bufferPtr = bufPtr;
        }
        // now that we have a buffer, initialize the entry
        bitmapTable[keycode].pBitmap->init(
            bitmapTable[keycode].bufferPtr, NULL, entrySize, bitmapTupleDesc);
        bitmapTable[keycode].pBitmap->setEntryTuple(initBitmap);
        bitmapTable[keycode].inuse = true;
    }

    return true;
}

PBuffer LbmGeneratorExecStream::flushBuffer()
{
    // need to flush a buffer out and return that buffer for use; for now,
    // cycle through the entries in round robin order in determining which
    // to flush
    //
    // NOTE zfong 6-Jan-2006: We may want to change this to a more 
    // sophisticated scheme where we flush on a LRU basis
    PBuffer retPtr;
    do {
        if (bitmapTable[flushIdx].bufferPtr) {
            retPtr = bitmapTable[flushIdx].bufferPtr;
            if (bitmapTable[flushIdx].inuse) {
                if (!flushEntry(flushIdx)) {
                    return NULL;
                }
            }
            bitmapTable[flushIdx].bufferPtr = NULL;
            break;
        }
        flushIdx = ++flushIdx % nBitmapEntries;
    } while (true);

    flushIdx = ++flushIdx % nBitmapEntries;
    return retPtr;
}

bool LbmGeneratorExecStream::flushTable(uint start)
{
    assert(start <= nBitmapEntries);
    for (uint i = start; i < nBitmapEntries; i++) {
        if (bitmapTable[i].inuse) {
            if (!flushEntry(i)) {
                producePending = LBM_TABLEFLUSH_PENDING;
                return false;
            }
        }
    }

    return true;
}

bool LbmGeneratorExecStream::flushEntry(uint keycode)
{
    assert(bitmapTable[keycode].inuse && bitmapTable[keycode].bufferPtr);

    // retrieve the generated bitmap entry and write it to the output
    // stream

    outputTuple = bitmapTable[keycode].pBitmap->produceEntryTuple();

    FENNEL_TRACE(TRACE_FINE, LbmEntry::toString(outputTuple));

    if (!pOutAccessor->produceTuple(outputTuple)) {
        flushStart = keycode;
        producePending = LBM_ENTRYFLUSH_PENDING;
        return false;
    }

    // entry no longer is associated with an entry tuple
    bitmapTable[keycode].inuse = false;

    return true;
}

FENNEL_END_CPPFILE("$Id$");

// End LbmGeneratorExecStream.cpp
