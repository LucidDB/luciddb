/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2006 The Eigenbase Project
// Copyright (C) 2010 SQLstream, Inc.
// Copyright (C) 2006 Dynamo BI Corporation
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
#include "fennel/lbm/LbmGeneratorExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/tuple/UnalignedAttributeAccessor.h"

#include <numeric>

FENNEL_BEGIN_CPPFILE("$Id$");

void LbmGeneratorExecStream::prepare(LbmGeneratorExecStreamParams const &params)
{
    BTreeExecStream::prepare(params);
    LcsRowScanBaseExecStream::prepare(params);

    insertRowCountParamId = params.insertRowCountParamId;
    assert(opaqueToInt(insertRowCountParamId) > 0);

    createIndex = params.createIndex;
    parameterIds.resize(nClusters);
    for (uint i = 0; i < nClusters; i++) {
        parameterIds[i] = params.lcsClusterScanDefs[i].rootPageIdParamId;
    }

    scratchLock.accessSegment(scratchAccessor);
    scratchPageSize = scratchAccessor.pSegment->getUsablePageSize();

    // setup input tuple -- numRowsToLoad, startRid
    assert(inAccessors[0]->getTupleDesc().size() == 2);
    inputTuple.compute(inAccessors[0]->getTupleDesc());

    // setup tuple used to store key values read from clusters
    bitmapTuple.computeAndAllocate(pOutAccessor->getTupleDesc());

    // setup output tuple
    assert(treeDescriptor.tupleDescriptor == pOutAccessor->getTupleDesc());
    bitmapTupleDesc = treeDescriptor.tupleDescriptor;

    attrAccessors.resize(bitmapTupleDesc.size());
    for (int i = 0; i < bitmapTupleDesc.size(); ++i) {
        attrAccessors[i].compute(bitmapTupleDesc[i]);
    }

    nIdxKeys = treeDescriptor.keyProjection.size() - 1;

    // determine min and max size of bitmap entries

    LbmEntry::getSizeBounds(
        bitmapTupleDesc,
        treeDescriptor.segmentAccessor.pSegment->getUsablePageSize(),
        minBitmapSize,
        maxBitmapSize);

    ridRuns.resize(1);
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
    doneReading = false;
    revertToSingletons = false;
    ridRuns.clear();
    if (!restart) {
        pDynamicParamManager->createParam(
            insertRowCountParamId, inAccessors[0]->getTupleDesc()[0]);

        // set the rootPageIds of the clusters, if there are dynamic parameters
        // corresponding to them
        if (parameterIds.size() > 0) {
            for (uint i = 0; i < nClusters; i++) {
                if (opaqueToInt(parameterIds[i]) > 0) {
                    pClusters[i]->setRootPageId(
                        *reinterpret_cast<PageId const *>(
                            pDynamicParamManager->getParam(parameterIds[i])
                                .getDatum().pData));
                }
            }
        }
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

    if (inAccessors[0]->getState() == EXECBUF_EOS) {
        if (doneReading) {
            pOutAccessor->markEOS();
            return EXECRC_EOS;
        }

    } else {
        if (!inAccessors[0]->demandData()) {
            return EXECRC_BUF_UNDERFLOW;
        }

        inAccessors[0]->unmarshalTuple(inputTuple);

        // in the case of create index, the number of rows affected
        // is returned as 0, since the statement is a DDL
        LcsRidRun ridRun;
        if (createIndex) {
            numRowsToLoad = 0;
            ridRun.nRids = RecordNum(MAXU);
            startRid = LcsRid(0);
        } else {
            numRowsToLoad =
                *reinterpret_cast<RecordNum const *> (inputTuple[0].pData);
            ridRun.nRids = numRowsToLoad;
            startRid =
                *reinterpret_cast<LcsRid const *> (inputTuple[1].pData);
        }
        currRid = startRid;

        // Setup the prefetch rid run to a single run spanning the range
        // of rows to be inserted into the index
        ridRun.startRid = startRid;
        ridRuns.push_back(ridRun);

        // set number of rows to load in a dynamic parameter that
        // splicer will later read
        pDynamicParamManager->writeParam(insertRowCountParamId, inputTuple[0]);

        inAccessors[0]->consumeTuple();

        // special case where there are no rows -- don't bother reading
        // from the table because we may end up reading deleted rows
        if (!createIndex && numRowsToLoad == 0) {
            doneReading = true;
            return EXECRC_BUF_UNDERFLOW;
        }

        // position to the starting rid
        for (uint iClu = 0; iClu < nClusters; iClu++) {
            SharedLcsClusterReader &pScan = pClusters[iClu];
            if (!pScan->position(startRid)) {
                // empty table
                doneReading = true;
                return EXECRC_BUF_UNDERFLOW;
            }
            syncColumns(pScan);
        }

        // initialize bitmap table to a single entry, assuming we're
        // starting with singleton bitmaps
        bool rc = initBitmapTable(1);
        assert(rc);
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
            // It's possible for the row count to be larger if we're building
            // an index on a replaced column.
            assert(rowCount >= numRowsToLoad);
        }
        doneReading = true;
        return EXECRC_BUF_UNDERFLOW;
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
        if (!revertToSingletons
            && pClusters[0]->clusterCols[0].batchIsCompressed())
        {
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
        revertToSingletons = false;
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
                            iClu == 0
                            && (nClusters == 1 || !pClusters[1]->nextRange()));
                        return EXECRC_EOS;
                    }
                    assert(
                        currRid >= pScan->getRangeStartRid()
                        && currRid < pScan->getRangeEndRid());
                    syncColumns(pScan);
                } else {
                    assert(currRid >= pScan->getRangeStartRid());
                    pScan->advanceWithinBatch(
                        opaqueToInt(currRid - pScan->getCurrentRid()));
                }
                readColVals(
                    pScan,
                    bitmapTuple,
                    prevClusterEnd);
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
    keyCodes.clear();
    keyReductionMap.clear();
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

        // if there's insufficient buffer space, revert to generating
        // singletons for this batch
        if (!initBitmapTable(nDistinctVals)) {
            revertToSingletons = true;
            return generateSingletons();
        }

        keyCodes.resize(nRows);
        colReader.readCompressedBatch(nRows, &keyCodes[0], &nRead);
        assert(nRows == nRead);

        // By default, use an identity map for key reduction
        keyReductionMap.resize(nDistinctVals);
        std::iota(keyReductionMap.begin(), keyReductionMap.end(), 0);

        // Then make any adjustments needed.  Note that the scope of
        // this mapping relies on the fact that we call flushTable
        // at the end of each batch.
        remapTrailingBlanks();

        batchRead = true;
        currBatch = 0;
    }

    // resume reading batch values based on where we last left off;
    // if the value has been read but not yet processed, skip the read
    for (uint i = currBatch; i < nRows; i++) {
        uint16_t keyCode = keyCodes[i];
        keyCode = keyReductionMap[keyCode];
        if (!skipRead) {
            PBuffer curValue = colReader.getBatchValue(keyCode);
            // reset buffer before loading new value, in case previous
            // row had nulls
            bitmapTuple.resetBuffer();

            attrAccessors[0].loadValue(bitmapTuple[0], curValue);
            initRidAndBitmap(bitmapTuple, &currRid);
        }
        if (!addRidToBitmap(keyCode, bitmapTuple, currRid)) {
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

void LbmGeneratorExecStream::remapTrailingBlanks()
{
    // For an explanation of the problem this solves, see
    // http://jira.eigenbase.org/browse/LDB-198
    StoredTypeDescriptor const &typeDesc =
        *(bitmapTupleDesc[0].pTypeDescriptor);

    // Only needed for VARCHAR
    bool unicode;
    if (typeDesc.getOrdinal() == STANDARD_TYPE_VARCHAR) {
        unicode = false;
    } else if (typeDesc.getOrdinal() == STANDARD_TYPE_UNICODE_VARCHAR) {
        unicode = true;
    } else {
        return;
    }

    // For each distinct value with trailing blanks, compare it against
    // all other distinct values; if it is equivalent to one with
    // fewer blanks, then map the longer one to the shorter one.
    TupleDatum datum1, datum2;
    LcsColumnReader &colReader = pClusters[0]->clusterCols[0];
    uint nDistinctVals = colReader.getBatchValCount();
    for (uint i1 = 0; i1 < nDistinctVals; ++i1) {
        PBuffer pVal = colReader.getBatchValue(i1);
        attrAccessors[0].referenceValue(datum1, pVal);
        if (!datum1.pData) {
            continue;
        }
        if (!datum1.cbData) {
            continue;
        }
        if (unicode) {
            // TODO jvs 15-Sept-2009:  this won't work on architectures
            // which require aligned memory access.
            Ucs2ConstBuffer pLast = reinterpret_cast<Ucs2ConstBuffer>
                (datum1.pData + datum1.cbData - 2);
            if (*pLast != ' ') {
                continue;
            }
        } else {
            if (datum1.pData[datum1.cbData - 1] != ' ') {
                continue;
            }
        }
        uint cbMin = datum1.cbData;
        for (uint i2 = 0; i2 < nDistinctVals; ++i2) {
            if (i1 == i2) {
                continue;
            }
            PBuffer pVal2 = colReader.getBatchValue(i2);
            attrAccessors[0].referenceValue(datum2, pVal2);
            if (!datum2.pData) {
                continue;
            }
            if (datum2.cbData >= cbMin) {
                continue;
            }
            int c = typeDesc.compareValues(
                datum1.pData,
                datum1.cbData,
                datum2.pData,
                datum2.cbData);
            if (c == 0) {
                cbMin = datum2.cbData;
                keyReductionMap[i1] = i2;
            }
        }
    }
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
                uint idx = projMap[prevClusterEnd + iCluCol];

                attrAccessors[idx].loadValue(bitmapTuple[idx], curValue);
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
        if (!advanceReader(pScan)) {
            return true;
        }
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

bool LbmGeneratorExecStream::initBitmapTable(uint nEntries)
{
    // compute the size of the bitmap buffers, based on the number
    // of scratch pages available and the number of distinct values
    // in the batch
    uint nBufsPerPage = (uint) ceil((double) nEntries / maxNumScratchPages);
    uint currSize = scratchPageSize / nBufsPerPage;

    if (currSize < minBitmapSize) {
        currSize = minBitmapSize;
        nBufsPerPage = scratchPageSize / currSize;
    } else if (currSize > maxBitmapSize) {
        currSize = maxBitmapSize;
        nBufsPerPage = scratchPageSize / currSize;
    }

    // If there are less than 8 buffers, then there cannot be more keys than
    // buffers.  That's because we need to avoid flushing those buffers that
    // potentially overlap in the last byte with the upcoming rids being
    // processed.
    uint nBuffers = nBufsPerPage * maxNumScratchPages;
    if (nBuffers < 8 && nEntries > nBuffers) {
        return false;
    }

    if (nEntries > nBitmapEntries) {
        // resize bitmap table to accomodate new batch, which has more
        // distinct values
        bitmapTable.resize(nEntries);
        for (uint i = nBitmapEntries; i < nEntries; i++) {
            bitmapTable[i].pBitmap = SharedLbmEntry(new LbmEntry());
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
                offset += currSize;
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
    entrySize = currSize;

    return true;
}

void LbmGeneratorExecStream::initRidAndBitmap(
    TupleData &bitmapTuple, LcsRid* pCurrRid)
{
    bitmapTuple[nIdxKeys].pData = (PConstBuffer) pCurrRid;
    bitmapTuple[nIdxKeys + 1].pData = NULL;
    bitmapTuple[nIdxKeys + 2].pData = NULL;
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
            PBuffer bufPtr = flushBuffer(rid);
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

PBuffer LbmGeneratorExecStream::flushBuffer(LcsRid addRid)
{
    // need to flush a buffer out and return that buffer for use; for now,
    // cycle through the entries in round robin order in determining which
    // to flush
    //
    // NOTE zfong 6-Jan-2006: We may want to change this to a more
    // sophisticated scheme where we flush on a LRU basis
    PBuffer retPtr;
    uint nAttempts = 0;
    do {
        ++nAttempts;
        if (nAttempts > nBitmapEntries) {
            // we should always have enough buffers so we can flush at least
            // one existing entry
            permAssert(false);
        }
        if (bitmapTable[flushIdx].bufferPtr) {
            retPtr = bitmapTable[flushIdx].bufferPtr;
            if (bitmapTable[flushIdx].inuse) {
                // skip over entries whose rid range overlaps with the rid
                // that will be added next, since we potentially may need
                // to add that rid (or one that follows and is within the
                // same rid range) into that entry
                if (bitmapTable[flushIdx].pBitmap->inRange(addRid)) {
                    flushIdx = ++flushIdx % nBitmapEntries;
                    continue;
                }
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
