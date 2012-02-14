/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/

#include "fennel/common/CommonPreamble.h"
#include "fennel/sorter/ExternalSortRunLoader.h"
#include "fennel/sorter/ExternalSortInfo.h"
#include "fennel/exec/ExecStreamBufAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

ExternalSortRunLoader::ExternalSortRunLoader(ExternalSortInfo &sortInfoIn)
    : sortInfo(sortInfoIn)
{
    nMemPagesMax = 0;
    pDataBuffer = NULL;
    pIndexBuffer = NULL;

    nTuplesLoaded = nTuplesFetched = 0;

    runningParallelTask = false;

    bufferLock.accessSegment(sortInfo.memSegmentAccessor);

    tupleAccessor.compute(sortInfo.tupleDesc);
    tupleAccessor2.compute(sortInfo.tupleDesc);

    keyData.compute(sortInfo.keyDesc);
    keyData2 = keyData;

    keyAccessor.bind(tupleAccessor, sortInfo.keyProj);
    keyAccessor2.bind(tupleAccessor2, sortInfo.keyProj);

    partitionKeyData.computeAndAllocate(sortInfo.keyDesc);
    partitionKeyInitialized = false;

    // TODO:  utility methods for calculations below, and assert block
    // size is power of 2
    uint nKeysPerPage = sortInfo.cbPage / sizeof(PBuffer);
    assert(nKeysPerPage > 1);
    nKeysPerPage >>= 1;
    indexToPageShift = 1;
    indexPageMask = 1;
    while ((nKeysPerPage & indexPageMask) == 0) {
        ++indexToPageShift;
        indexPageMask <<= 1;
        ++indexPageMask;
    }
}

ExternalSortRunLoader::~ExternalSortRunLoader()
{
    releaseResources();
}

inline PBuffer &ExternalSortRunLoader::getPointerArrayEntry(uint iTuple)
{
    // REVIEW jvs 12-June-2004:  This is the price we pay for not using a big
    // linear array.  Is it too expensive?
    uint iPage = iTuple >> indexToPageShift;
    uint iSubKey = iTuple & indexPageMask;
    PBuffer *pPage = reinterpret_cast<PBuffer *>(indexBuffers[iPage]);
    return pPage[iSubKey];
}

void ExternalSortRunLoader::startRun()
{
    if (!nMemPagesMax) {
        nMemPagesMax = sortInfo.nSortMemPagesPerRun;
    }

    freeBuffers.insert(
        freeBuffers.end(), indexBuffers.begin(), indexBuffers.end());
    freeBuffers.insert(
        freeBuffers.end(), dataBuffers.begin(), dataBuffers.end());
    indexBuffers.clear();
    dataBuffers.clear();
    pDataBuffer = NULL;
    pIndexBuffer = NULL;
    partitionKeyInitialized = false;

    if (!allocateDataBuffer()) {
        permAssert(false);
    }

    if (!allocateIndexBuffer()) {
        permAssert(false);
    }

    nTuplesLoaded = nTuplesFetched = 0;

    fetchArray.nTuples = 0;
}

PBuffer ExternalSortRunLoader::allocateBuffer()
{
    PBuffer pBuffer;

    if (!freeBuffers.empty()) {
        pBuffer = freeBuffers.back();
        freeBuffers.pop_back();
        return pBuffer;
    }

    if ((indexBuffers.size() + dataBuffers.size()) >= nMemPagesMax) {
        return NULL;
    }

    bufferLock.allocatePage();
    pBuffer = bufferLock.getPage().getWritableData();

    // REVIEW jvs 12-June-2004:  we rely on the fact that the underlying
    // ScratchSegment keeps the page pinned for us; need to make this
    // official.
    bufferLock.unlock();

    return pBuffer;
}

bool ExternalSortRunLoader::allocateDataBuffer()
{
    pDataBuffer = allocateBuffer();
    if (pDataBuffer) {
        dataBuffers.push_back(pDataBuffer);
        pDataBufferEnd = pDataBuffer + sortInfo.cbPage;
        return true;
    } else {
        return false;
    }
}

bool ExternalSortRunLoader::allocateIndexBuffer()
{
    if (indexBuffers.size() > sortInfo.nIndexMemPages) {
        pIndexBuffer = NULL;
        return false;
    }
    pIndexBuffer = allocateBuffer();
    if (pIndexBuffer) {
        indexBuffers.push_back(pIndexBuffer);
        pIndexBufferEnd = pIndexBuffer + sortInfo.cbPage;
        return true;
    } else {
        return false;
    }
}

void ExternalSortRunLoader::releaseResources()
{
    freeBuffers.clear();
    indexBuffers.clear();
    dataBuffers.clear();
    nMemPagesMax = 0;

    // REVIEW jvs 12-June-2004:  see corresponding comment above in
    // allocateBuffer()

    sortInfo.memSegmentAccessor.pSegment->deallocatePageRange(
        NULL_PAGE_ID, NULL_PAGE_ID);
}

ExternalSortRC ExternalSortRunLoader::loadRun(
    ExecStreamBufAccessor &bufAccessor)
{
    for (;;) {
        if (!bufAccessor.demandData()) {
            break;
        }
        uint cbAvailable = bufAccessor.getConsumptionAvailable();
        assert(cbAvailable);
        PConstBuffer pSrc = bufAccessor.getConsumptionStart();
        bool overflow = false;
        bool yield = false;
        bool skippedRow = false;
        uint cbCopy = 0;
        uint cbTuple = 0;
        while (cbCopy < cbAvailable) {
            PConstBuffer pSrcTuple = pSrc + cbCopy;
            cbTuple = tupleAccessor.getBufferByteCount(pSrcTuple);
            assert(cbTuple);
            assert(cbTuple <= tupleAccessor.getMaxByteCount());

            // partition sort
            if (sortInfo.partitionKeyCount > 0) {
                if (skipRow(bufAccessor, pSrcTuple)) {
                    // current row is to be skipped from sort operation.
                    skippedRow = true;
                    break;
                }

                if (checkEndOfPartition(bufAccessor, pSrcTuple)) {
                    yield = true;
                    break;
                }
            }

            // first make sure we have room for the key pointer
            if (pIndexBuffer >= pIndexBufferEnd) {
                if (!allocateIndexBuffer()) {
                    FENNEL_TRACE(
                        TRACE_FINEST,
                        " No space for new index Buffer. Overflow.... ");
                    overflow = true;
                    break;
                }
            }

            // now make sure we have enough room for the data
            if (pDataBuffer + cbCopy + cbTuple > pDataBufferEnd) {
                if (!cbCopy) {
                    // first tuple:  try to allocate a new buffer
                    if (!allocateDataBuffer()) {
                        // since cbCopy is zero, we can return right now
                        FENNEL_TRACE(
                            TRACE_FINEST,
                            " No space for new data Buffer. Overflow.... ");
                        return EXTSORT_OVERFLOW;
                    }
                }
                // copy whatever we've calculated so far;
                // next time through the for loop, we'll allocate a fresh buffer
                break;
            }

            *((PBuffer *) pIndexBuffer) = pDataBuffer + cbCopy;
            pIndexBuffer += sizeof(PBuffer);
            nTuplesLoaded++;
            cbCopy += cbTuple;
        }
        if (skippedRow || cbCopy) {
            memcpy(pDataBuffer, pSrc, cbCopy);
            pDataBuffer += cbCopy;
            if (skippedRow) {
                // consume skipped row from the input as well.
                cbCopy += cbTuple;
            }
            bufAccessor.consumeData(pSrc + cbCopy);
        }

        if (yield) {
            return EXTSORT_YIELD;
        }

        if (overflow) {
            return EXTSORT_OVERFLOW;
        }
    }

    return EXTSORT_SUCCESS;
}

bool ExternalSortRunLoader::skipRow(
    ExecStreamBufAccessor &bufAccessor, PConstBuffer pSrcTuple)
{
    // always return false.
    return false;
}

bool ExternalSortRunLoader::checkEndOfPartition(
    ExecStreamBufAccessor &bufAccessor, PConstBuffer pSrcTuple)
{
    if (!partitionKeyInitialized) {
        // Need to save current Partition key
        tupleAccessor.setCurrentTupleBuf(pSrcTuple);
        keyAccessor.unmarshal(keyData);
        for (int i = 0; i < sortInfo.partitionKeyCount; i++) {
            partitionKeyData[i].memCopyFrom(keyData[i]);
        }
        partitionKeyInitialized = true;
    } else {
        // check for change in partition column(s).
        tupleAccessor.setCurrentTupleBuf(pSrcTuple);
        keyAccessor.unmarshal(keyData);
        if (sortInfo.keyDesc.compareTuplesKey
                (keyData, partitionKeyData,
                 sortInfo.partitionKeyCount) != 0)
        {
            // 'end of partition'. sort this partition and produce
            // result for this partition.
            return true;
        }
    }
    return false;
}

void ExternalSortRunLoader::sort()
{
    assert(nTuplesLoaded);

    quickSort(0, nTuplesLoaded - 1);
}

ExternalSortFetchArray &ExternalSortRunLoader::bindFetchArray()
{
    return fetchArray;
}

ExternalSortRC ExternalSortRunLoader::fetch(uint nTuplesRequested)
{
    if (nTuplesFetched >= nTuplesLoaded) {
        return EXTSORT_ENDOFDATA;
    }

    fetchArray.ppTupleBuffers = (PBuffer *)
        &(getPointerArrayEntry(nTuplesFetched));
    uint pageEnd = (nTuplesFetched | indexPageMask) + 1;
    pageEnd = std::min(pageEnd, nTuplesLoaded);
    fetchArray.nTuples = std::min(nTuplesRequested, pageEnd - nTuplesFetched);
    nTuplesFetched += fetchArray.nTuples;

    return EXTSORT_SUCCESS;
}

inline void ExternalSortRunLoader::quickSortSwap(uint l, uint r)
{
    std::swap(getPointerArrayEntry(l), getPointerArrayEntry(r));
}

// TODO:  move this
const uint step_factor = 7;

PBuffer ExternalSortRunLoader::quickSortFindPivot(uint l, uint r)
{
    uint i, j, cnt, step;
    PBuffer vals[step_factor];

    if (r <= l) {
        return NULL;
    }

    cnt = 0;
    step = ((r - l) / step_factor) + 1;
    for (i = l; i <= r; i += step) {
        vals[cnt] = getPointerArrayEntry(i);
        j = cnt++;
        while (j > 0) {
            tupleAccessor.setCurrentTupleBuf(vals[j]);
            tupleAccessor2.setCurrentTupleBuf(vals[j - 1]);
            keyAccessor.unmarshal(keyData);
            keyAccessor2.unmarshal(keyData2);
            if (sortInfo.compareKeys(keyData, keyData2) >= 0) {
                break;
            }
            std::swap(vals[j], vals[j - 1]);
            j--;
        }
    }
    if (step == 1) {
        for (i = 0; i < cnt; ++i) {
            getPointerArrayEntry(l + i) = vals[i];
        }
        return NULL;
    }
    return vals[(cnt >> 1)];
}

uint ExternalSortRunLoader::quickSortPartition(uint l, uint r, PBuffer pivot)
{
    l--;
    r++;

    tupleAccessor.setCurrentTupleBuf(pivot);
    keyAccessor.unmarshal(keyData);
    for (;;) {
        for (;;) {
            ++l;
            tupleAccessor2.setCurrentTupleBuf(getPointerArrayEntry(l));
            keyAccessor2.unmarshal(keyData2);
            if (sortInfo.compareKeys(keyData2, keyData) >= 0) {
                break;
            }
        }
        for (;;) {
            --r;
            tupleAccessor2.setCurrentTupleBuf(getPointerArrayEntry(r));
            keyAccessor2.unmarshal(keyData2);
            if (sortInfo.compareKeys(keyData2, keyData) <= 0) {
                break;
            }
        }
        if (l < r) {
            quickSortSwap(l, r);
        } else {
            return l;
        }
    }
}

void ExternalSortRunLoader::quickSort(uint l, uint r)
{
    PBuffer pPivotTuple;
    uint x;

    pPivotTuple = quickSortFindPivot(l, r);
    if (pPivotTuple) {
        x = quickSortPartition(l, r, pPivotTuple);
        if (x == l) {
            // pPivotTuple was lowest value in partition and was at position l
            // - move off of it and keep going
            x++;
        }
        quickSort(l, x - 1);
        quickSort(x, r);
    }
}

uint ExternalSortRunLoader::getLoadedTupleCount()
{
    return nTuplesLoaded;
}

bool ExternalSortRunLoader::isStarted()
{
    return nTuplesLoaded > nTuplesFetched;
}

FENNEL_END_CPPFILE("$Id$");

// End ExternalSortRunLoader.cpp
