/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Copyright (C) 2005-2009 The Eigenbase Project
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
#include "fennel/lucidera/colstore/LcsClusterNodeWriter.h"
#include "fennel/tuple/TupleAccessor.h"
#include <boost/scoped_array.hpp>

FENNEL_BEGIN_CPPFILE("$Id$");

LcsClusterNodeWriter::LcsClusterNodeWriter(
    BTreeDescriptor const &treeDescriptorInit,
    SegmentAccessor const &accessorInit,
    TupleDescriptor const &colTupleDescInit,
    SharedTraceTarget pTraceTargetInit,
    std::string nameInit) :
        LcsClusterAccessBase(treeDescriptorInit),
        TraceSource(pTraceTargetInit, nameInit)
{
    scratchAccessor = accessorInit;
    bufferLock.accessSegment(scratchAccessor);
    bTreeWriter = SharedBTreeWriter(
        new BTreeWriter(treeDescriptorInit, scratchAccessor, true));
    colTupleDesc = colTupleDescInit;
    clusterDump =
        SharedLcsClusterDump(
            new LcsClusterDump(
                treeDescriptorInit,
                colTupleDesc,
                TRACE_FINE,
                pTraceTargetInit,
                nameInit));
    nClusterCols = 0;
    pHdr = 0;
    hdrSize = 0;
    pIndexBlock = 0;
    pBlock = 0;
    szBlock = 0;
    minSzLeft = 0;
    batchDirs.reset();
    pValBank.reset();
    oValBank.reset();
    batchOffset.reset();
    batchCount.reset();
    szLeft = 0;
    nBits.reset();
    nextWidthChange.reset();
    arraysAllocated = false;
    valBankStart.reset();
    bForceMode.reset();
    forceModeCount.reset();
    maxValueSize.reset();
}

LcsClusterNodeWriter::~LcsClusterNodeWriter()
{
    close();
}

void LcsClusterNodeWriter::close()
{
    // flush and unlock last page written
    if (clusterLock.isLocked()) {
        clusterLock.flushPage(true);
    }
    clusterLock.unlock();

    bTreeWriter.reset();
    batchDirs.reset();
    pValBank.reset();
    valBankStart.reset();
    forceModeCount.reset();
    bForceMode.reset();
    oValBank.reset();
    batchOffset.reset();
    batchCount.reset();
    nBits.reset();
    nextWidthChange.reset();
    maxValueSize.reset();
    attrAccessors.reset();
}

bool LcsClusterNodeWriter::getLastClusterPageForWrite(
    PLcsClusterNode &pBlock, LcsRid &firstRid)
{
    // get the last key in the btree (if it exists) and read the cluster
    // page based on the pageid stored in that btree record

    if (bTreeWriter->searchLast() == false) {
        bTreeWriter->endSearch();
        return false;
    }

    bTreeWriter->getTupleAccessorForRead().unmarshal(bTreeTupleData);
    clusterPageId = readClusterPageId();
    clusterLock.lockExclusive(clusterPageId);
    pBlock = &(clusterLock.getNodeForWrite());
    firstRid = pBlock->firstRID;

    // End the search so the BTreeWriter doesn't think it's positioned within
    // the btree.  We'll position properly on the first monotonic insert.
    bTreeWriter->endSearch();

    if (isTracingLevel(TRACE_FINE)) {
        FENNEL_TRACE(TRACE_FINE,
                     "Calling ClusterDump from getLastClusterPageForWrite");
        clusterDump->dump(opaqueToInt(clusterPageId), pBlock, szBlock);
    }

    return true;
}

PLcsClusterNode LcsClusterNodeWriter::allocateClusterPage(LcsRid firstRid)
{
    // allocate a new cluster page and insert the corresponding rid, pageid
    // record into the btree

    PageId prevPageId = NULL_PAGE_ID;

    if (clusterLock.isLocked()) {
        // Remember the predecessor so that we can chain it below.
        prevPageId = clusterLock.getPageId();

        // Kick off an asynchronous write on the page we've just finished
        // so that when it comes time to checkpoint or victimize,
        // maybe it will be on disk already.
        clusterLock.flushPage(true);
    }

    clusterPageId = clusterLock.allocatePage();
    if (prevPageId != NULL_PAGE_ID) {
        segmentAccessor.pSegment->setPageSuccessor(prevPageId, clusterPageId);
    }
    bTreeRid = firstRid;
    bTreeTupleData[0].pData = reinterpret_cast<uint8_t *> (&firstRid);
    bTreeTupleData[1].pData = reinterpret_cast<uint8_t *> (&clusterPageId);
    bTreeWriter->insertTupleData(bTreeTupleData, DUP_FAIL);
    return &(clusterLock.getNodeForWrite());
}

void LcsClusterNodeWriter::init(
    uint nColumn, PBuffer iBlock, PBuffer *pB, uint szB)
{
    nClusterCols = nColumn;
    pIndexBlock = iBlock;
    pBlock = pB;
    szBlock = szB;
    pHdr = (PLcsClusterNode) pIndexBlock;

    hdrSize = getClusterSubHeaderSize(nClusterCols);

    // initialize lastVal, firstVal, and nVal fields in the header
    // to point to the appropriate positions in the indexBlock

    setHdrOffsets(pHdr);

    minSzLeft = nClusterCols * (LcsMaxLeftOver * sizeof(uint16_t) +
                     sizeof(LcsBatchDir));

    allocArrays();
}

void LcsClusterNodeWriter::openNew(LcsRid startRID)
{
    int i;

    // Inialize block header and batches
    pHdr->firstRID = startRID;
    pHdr->nColumn = nClusterCols;
    pHdr->nBatch = 0;
    pHdr->oBatch = hdrSize;

    for (i = 0; i < nClusterCols; i++) {
        lastVal[i] = szBlock;
        firstVal[i] = (uint16_t) szBlock;
        nVal[i] = 0;
        delta[i] = 0;
        batchDirs[i].mode = LCS_COMPRESSED;
        batchDirs[i].nVal = 0;
        batchDirs[i].nRow = 0;
        batchDirs[i].oVal = 0;
        batchDirs[i].oLastValHighMark = lastVal[i];
        batchDirs[i].nValHighMark = nVal[i];
        batchOffset[i] = hdrSize;
        // # of bits it takes to represent 0 values
        nBits[i] = 0;
        nextWidthChange[i] = 1;
        batchCount[i] = 0;
    }

    // account for the header size, account for at least 1 batch for each column
    // and leave space for one addtional batch for a "left-over" batch

    szLeft = szBlock - hdrSize -
                (2 * sizeof(LcsBatchDir)) * nClusterCols;
    szLeft = std::max(szLeft, 0);
    assert(szLeft >= 0);
}

bool LcsClusterNodeWriter::openAppend(
    uint *nValOffsets, uint16_t *lastValOffsets, RecordNum &nrows)
{
    int i;

    // leave space for one batch for each column entry
    szLeft = lastVal[nClusterCols - 1] - pHdr->oBatch -
                (pHdr->nBatch + 2* nClusterCols) * sizeof(LcsBatchDir);
    szLeft = std::max(szLeft, 0);
    assert(szLeft >= 0);

    // Let's move the values, batch directories, and batches to
    // temporary blocks from index block
    nrows = moveFromIndexToTemp();

    for (i = 0; i < nClusterCols; i++) {
        nValOffsets[i] = nVal[i];
        lastValOffsets[i] = lastVal[i];
        memset(&batchDirs[i], 0, sizeof(LcsBatchDir));

        batchDirs[i].oLastValHighMark = lastVal[i];
        batchDirs[i].nValHighMark = nVal[i];
        batchDirs[i].mode = LCS_COMPRESSED;

        // # of bits it takes to represent 0 values
        nBits[i] = 0;
        nextWidthChange[i] = 1;

        oValBank[i] = 0;
        batchCount[i] = pHdr->nBatch / nClusterCols;
    }

    return (szLeft == 0);
}

void LcsClusterNodeWriter::describeLastBatch(
    uint column, uint &dRow, uint &recSize)
{
    PLcsBatchDir pBatch;

    pBatch = (PLcsBatchDir) (pBlock[column] + batchOffset[column]);
    dRow = pBatch[batchCount[column] -1].nRow % 8;
    recSize = pBatch[batchCount[column] -1].recSize;
}

uint16_t LcsClusterNodeWriter::getNextVal(uint column, uint16_t thisVal)
{
    if (thisVal && thisVal != szBlock) {
        return
            (uint16_t) (thisVal +
                attrAccessors[column].getStoredByteCount(
                    pBlock[column] + thisVal));
    } else {
        return 0;
    }
}

void LcsClusterNodeWriter::rollBackLastBatch(uint column, PBuffer pBuf)
{
    uint i;
    PLcsBatchDir pBatch;
    uint16_t *pValOffsets;

    uint8_t *pBit;                      // bitVecs start address
    WidthVec w;                         // bitVec width vector
    PtrVec p;                           // bitVec offsets
    uint iV;                            // # of bit vectors

    uint16_t rows[LcsMaxRollBack];      // row index storage
    int origSzLeft;
    uint len;

    // load last batch, nBatch must be at least 1
    pBatch = (PLcsBatchDir)(pBlock[column] + batchOffset[column]);
    batchDirs[column]  = pBatch[batchCount[column] -1];

    // compute size left in temporary block before roll back
    origSzLeft = lastVal[column] - batchOffset[column] -
                    (batchCount[column]+2)*sizeof(LcsBatchDir);

    if ((batchDirs[column].nRow > 8) || (batchDirs[column].nRow % 8) == 0) {
        return;
    }

    if (batchDirs[column].mode == LCS_COMPRESSED) {
        // calculate the bit vector widthes
        iV = bitVecWidth(calcWidth(batchDirs[column].nVal), w);

        // this is where the bit vectors start
        pBit = pBlock[column] + batchDirs[column].oVal +
                batchDirs[column].nVal * sizeof(uint16_t);

        // nByte are taken by the bit vectors
        bitVecPtr(batchDirs[column].nRow, iV, w, p, pBit);

        // there are at most 8 rows in this batch
        readBitVecs(rows, iV, w, p, 0, batchDirs[column].nRow);

        // get the address of the batches value offsets
        pValOffsets = (uint16_t *)(pBlock[column] + batchDirs[column].oVal);

        // fill up buffer with batches values
        for (i = 0; i < batchDirs[column].nRow;
            i++, pBuf += batchDirs[column].recSize)
        {
            len =
                attrAccessors[column].getStoredByteCount(
                    pBlock[column] + pValOffsets[rows[i]]);
            memcpy(pBuf, pBlock[column] + pValOffsets[rows[i]], len);
        }

    } else if (batchDirs[column].mode == LCS_FIXED) {
        // fixed size record batch
        // copy the values into the given buffer
        memcpy(pBuf, pBlock[column] + batchDirs[column].oVal,
                batchDirs[column].nRow * batchDirs[column].recSize);
    } else {
        // variable sized records (batch.mode == LCS_VARIABLE)
        // get the address of the batches value offsets
        pValOffsets = (uint16_t *)(pBlock[column] + batchDirs[column].oVal);

        // fill up buffer with batches values
        for (i = 0; i < batchDirs[column].nRow;
            i++, pBuf += batchDirs[column].recSize)
        {
            len =
                attrAccessors[column].getStoredByteCount(
                    pBlock[column] + pValOffsets[i]);
            memcpy(pBuf, pBlock[column] + pValOffsets[i], len);
        }
    }

    // Reset the last batch
    batchCount[column]--;
    // batch dir offset points to the beginning of the rolled back batch
    batchOffset[column] = batchDirs[column].oVal;

    // copy the batch dir back to the end of the prev batch.
    memmove(pBlock[column] + batchOffset[column], pBatch,
            batchCount[column] * sizeof(LcsBatchDir));

    // recalc size left
    // leave place for one new batch(the rolled back one will be rewriten)
    // and possibley one to follow.  Subtract the difference of the new size
    // and the original size and add this to szLeft in index variable
    int newSz;
    newSz = lastVal[column] - batchOffset[column] -
            (batchCount[column] + 2) * sizeof(LcsBatchDir);
    szLeft += (newSz - origSzLeft);
    szLeft = std::max(szLeft, 0);
    assert(szLeft >= 0);

    // # of bits it takes to represent 0 values
    nBits[column] = 0;
    nextWidthChange[column] = 1;

    // set batch parameters
    batchDirs[column].mode = LCS_COMPRESSED;
    batchDirs[column].nVal = 0;
    batchDirs[column].nRow = 0;
    batchDirs[column].oVal = 0;
    batchDirs[column].recSize = 0;
}

// addValue() where the current value already exists

bool LcsClusterNodeWriter::addValue(uint column, bool bFirstTimeInBatch)
{
    // Calculate szleft assuming the value gets added.
    szLeft -= sizeof(uint16_t);

    // if there is not enough space left, reject value
    if (szLeft < ((int) nClusterCols * LcsMaxSzLeftError)) {
        // set szLeft to its previous value
        szLeft += sizeof(uint16_t);
        assert(szLeft >= 0);
        return false;
    }

    if (bFirstTimeInBatch) {
        // there is enough space to house the value, increment batch
        // value count
        batchDirs[column].nVal++;

        // check if nBits needs to change by comparing the value count
        // the change point count
        if (batchDirs[column].nVal == nextWidthChange[column]) {
            // calculate the next nBits value, and the count of values
            // for the next chane
            nBits[column] = calcWidth(batchDirs[column].nVal);
            nextWidthChange[column] = (1 << nBits[column]) + 1;
        }
    }

    return true;
}

// addValue() where the value must be added to the bottom of the page

bool LcsClusterNodeWriter::addValue(uint column, PBuffer pVal, uint16_t *oVal)
{
    uint16_t lastValOffset;
    int oldSzLeft = szLeft;
    uint szVal = attrAccessors[column].getStoredByteCount(pVal);

    // if we are in forced fixed compression mode,
    // see if the maximum record size in this batch has increased.
    // if so, adjust the szLeft based on the idea that each previous element
    // will now be taking more space
    if (bForceMode[column] == fixed) {
        if (szVal > maxValueSize[column]) {
            szLeft -= batchDirs[column].nVal *
                (szVal - maxValueSize[column]);
            maxValueSize[column] = szVal;
        }
    }

    // adjust szleft (upper bound on amount of space left in the block).
    // If we are in a forced fixed compression mode then we can calculate this
    // exactly.  If we are in "none" mode, then we calculate szLeft according to
    // variable mode compression (which should be an upper bound), and adjust it
    // later in pickCompressionMode
    if (bForceMode[column] == fixed) {
        szLeft -= maxValueSize[column];
    } else {
        // assume value is being added in variable mode
        // (note: this assumes that whenever we convert from
        // variable mode to compressed mode, the compressed mode will
        // take less space, for only in this case is szLeft an upper bound)
        szLeft -= (sizeof(uint16_t) + szVal) ;
    }

    // if there is not enough space left reject value
    if (szLeft < ((int) nClusterCols * LcsMaxSzLeftError)) {
        // set szLeft to its previous value
        szLeft = oldSzLeft;
        assert(szLeft >= 0);
        return false;
    }

    // otherwise, go ahead and add the value...

    lastValOffset = lastVal[column] - szVal;

    // there is enough space to house the value, increment batch value count
    batchDirs[column].nVal++;

    // check if nBits needs to change by comparing the value count
    // the change point count
    if (batchDirs[column].nVal == nextWidthChange[column]) {
        // calculate the next nBits value, and the count of values
        // for the next chane
        nBits[column] = calcWidth(batchDirs[column].nVal);
        nextWidthChange[column] = (1 << nBits[column]) + 1;
    }

    lastVal[column] = lastValOffset;

    // Save the value being inserted.  If we are building the
    // block in fixed mode then save the value into pValBank
    // rather than saving it into the block
    if (fixed == bForceMode[column]) {
        memcpy(pValBank[column] + lastValOffset, pVal, szVal);
    } else {
        memcpy(pBlock[column] + lastValOffset, pVal, szVal);
    }

    // return the block offset of the new value;
    *oVal = lastValOffset;

    nVal[column]++;

    return true;
}

void LcsClusterNodeWriter::undoValue(
    uint column, PBuffer pVal, bool bFirstInBatch)
{
    // pVal may be null if the value already exists, in which case, it wasn't
    // added to the value list.  However, if it was the first such value for
    // the batch, addValue was called to bump-up the batch value count
    // so we still need to call undoValue
    uint szVal =
        (pVal) ? attrAccessors[column].getStoredByteCount(pVal) : 0;

    // add back size subtracted for offset
    szLeft += (sizeof(uint16_t) + szVal) ;
    assert(szLeft >= 0);

    // If value was new to the batch, then adjust counters
    if (bFirstInBatch) {
        // decrement batch count
        batchDirs[column].nVal--;

        //reset nextWidthChange
        if (batchDirs[column].nVal == 0) {
            nextWidthChange[column] = 1;
        } else {
            // calculate the next nBits value, and the count of values
            // for the next chane
            nBits[column] = calcWidth(batchDirs[column].nVal);
            nextWidthChange[column] = (1 << nBits[column]) + 1;
        }
    }

    if (pVal) {
        // upgrage header
        lastVal[column] += szVal;
        nVal[column]--;
    }
}

void LcsClusterNodeWriter::putCompressedBatch(
    uint column, PBuffer pRows, PBuffer pBuf)
{
    uint        i, j, b;
    uint        iRow;
    uint        nByte;
    uint8_t     *pBit;
    uint16_t    *pOffs;
    PLcsBatchDir pBatch;

    WidthVec    w;      // bitVec width vector
    PtrVec      p;      // bitVec offsets
    uint        iV;     // number of bit vectors

    // pickCompressionMode() was called prior to putCompressedBatch,
    // and the following has been already done:
    // -- the batch descriptors were moved to the back of the batch
    // -- a batch descriptor for this batch has been placed in the batch
    //    directory
    // -- this->batch contains up to date info
    // -- the caller has copied nVal value offsets to the head of this batch

    // write to buffer values for rows over the 8 boundary if nrow is
    // greater then 8

    if (batchDirs[column].nRow > 8) {
        uint len;
        pOffs = (uint16_t *)(pBlock[column] + batchDirs[column].oVal);
        for (i = round8Boundary((uint32_t) batchDirs[column].nRow);
            i < batchDirs[column].nRow; i++, pBuf += batchDirs[column].recSize)
        {
            iRow = ((uint16_t *) pRows)[i];
            len =
                attrAccessors[column].getStoredByteCount(
                    pBlock[column] + pOffs[iRow]);
            memcpy(pBuf, pBlock[column] + pOffs[iRow], len);
        }
        batchDirs[column].nRow =
            round8Boundary((uint32_t) batchDirs[column].nRow);
    }

    // calculate the bit vector widthes, sum(w[i]) is nBits
    iV = bitVecWidth(nBits[column], w);

    // this is where the bit vectors start
    pBit = pBlock[column] + batchDirs[column].oVal +
            batchDirs[column].nVal*sizeof(uint16_t);

    // nByte are taken by the bit vectors, clear them befor OR-ing
    nByte = bitVecPtr(batchDirs[column].nRow, iV, w, p, pBit);
    memset(pBit, 0, nByte);

    for (j = 0, b = 0; j < iV ; j++) {
        switch (w[j]) {
        case 16:
            memcpy(p[j], pRows, batchDirs[column].nRow * sizeof(uint16_t));
            break;

        case 8:
            for (i = 0; i < batchDirs[column].nRow ; i++) {
                (p[j])[i] = (uint8_t)((uint16_t *) pRows)[i];
            }
            break;

        case 4:
            for (i = 0; i < batchDirs[column].nRow ; i++) {
                setBits(p[j] + i / 2 , 4, (i % 2) * 4,
                    (uint16_t)(((uint16_t *) pRows)[i] >> b));
            }
            break;

        case 2:
            for (i = 0; i < batchDirs[column].nRow ; i++) {
                setBits(p[j] + i / 4 , 2, (i % 4) * 2,
                    (uint16_t)(((uint16_t *) pRows)[i] >> b));
            }
            break;

        case 1:
            for (i = 0; i < batchDirs[column].nRow ; i++) {
                setBits(p[j] + i / 8 , 1, (i % 8),
                    (uint16_t)(((uint16_t *)pRows)[i] >> b));
            }
            break;

        default:
                ;
        }
        b += w[j];
    }

    // put the batch in the batch directory
    pBatch = (PLcsBatchDir)(pBlock[column] + batchOffset[column]);
    pBatch[batchCount[column]] = batchDirs[column];
    batchCount[column]++;

    // reset the batch state
    batchDirs[column].mode = LCS_COMPRESSED;
    batchDirs[column].oLastValHighMark = lastVal[column];
    batchDirs[column].nValHighMark = nVal[column];
    batchDirs[column].nVal = 0;
    batchDirs[column].oVal = batchOffset[column];
    batchDirs[column].nRow = 0;

    // # of bits it takes to represent 0 values
    nBits[column] = 0;
    nextWidthChange[column] = 1 ;
}

void LcsClusterNodeWriter::putFixedVarBatch(
    uint column, uint16_t *pRows, PBuffer pBuf)
{
    uint        i;
    uint        batchRows;
    PBuffer     pVal;
    PLcsBatchDir pBatch;
    PBuffer     src;
    uint        batchRecSize;
    uint16_t    localLastVal;
    uint16_t    localoValBank;
    PBuffer     localpValBank, localpBlock;


    // The # of rows in a batch will be smaller then 8 or a multiple
    // of 8.  All but the last batch in a load will be greater then 8.
    // Round to nearest 8 boundary, unless this is the last batch (which
    // we determine by the nRows in the batch being less than 8).
    // If it turns it isn't the latch batch, then our caller (WriteBatch)
    // will roll back the whole batch and add it to a new block instead.
    batchRows = (batchDirs[column].nRow > 8)
        ? batchDirs[column].nRow & 0xfffffff8 : batchDirs[column].nRow;

    // the value destination
    pVal = pBlock[column] + batchDirs[column].oVal;
    if (batchDirs[column].mode == LCS_VARIABLE) {
        // For a variable mode, copy in the value offsets into the RI block.
        // The left over i.e. the rows over the highest 8 boundary will be
        // copied to pBuf
        memcpy(pVal, pRows, batchRows * sizeof(uint16_t));
    } else {
        // it's a fixed record batch
        assert(batchDirs[column].mode == LCS_FIXED);

        batchRecSize  = batchDirs[column].recSize;
        localLastVal  = lastVal[column];
        localpValBank = pValBank[column] + valBankStart[column];
        localoValBank = oValBank[column];
        localpBlock   = pBlock[column];

        // Copy the values themselves into the block.
        // The values are currently stored in pValBank
        for (i = 0; i < batchRows; i++) {
            // valueSource determines by the offset whether the value comes from
            // the bank of from the block
            src = valueSource(localLastVal, localpValBank, localoValBank,
                                localpBlock, pRows[i]);
            uint len = attrAccessors[column].getStoredByteCount(src);
            memcpy(pVal, src, len);
            pVal += batchRecSize;
        }
    }

    // if forced fixed mode is true we need to periodically set it false, so
    // we can at least check if the data can be compressed
    if (bForceMode[column] != none) {
        if (forceModeCount[column] > 20) {
            bForceMode[column] = none;
            forceModeCount[column] = 0;
        }
    }

    batchRecSize  = batchDirs[column].recSize;
    localLastVal  = lastVal[column];
    localpValBank = pValBank[column] + valBankStart[column];
    localoValBank = oValBank[column];
    localpBlock   = pBlock[column];

    // copy the tail of the batch (the last nRow % 8 values) to pBuf
    pVal = pBuf;
    for (i = batchRows; i < batchDirs[column].nRow; i++) {
        // valueSource determines by the offset whether the value comes from
        // the bank or from the block. if the value bank is not used
        // valueSource will get all the values fron the block
        src = valueSource(localLastVal, localpValBank, localoValBank,
                            localpBlock, pRows[i]);
        uint len = attrAccessors[column].getStoredByteCount(src);
        memcpy(pVal, src, len);
        pVal += batchRecSize;
    }

    if (pValBank[column]) {
        oValBank[column] = 0;
    }

    // Put batch descriptor in batch directory
    batchDirs[column].nRow = batchRows;
    pBatch = (PLcsBatchDir)(pBlock[column] + batchOffset[column]);
    pBatch[batchCount[column]] = batchDirs[column];

    // inc. batch count
    batchCount[column]++;

    // reset the batch state.  set batch back to compressed mode
    // unless the flag has been set that next
    switch (bForceMode[column]) {
    case none:
        batchDirs[column].mode = LCS_COMPRESSED;
        break;
    case fixed:
        batchDirs[column].mode = LCS_FIXED;
        break;
    case variable:
        batchDirs[column].mode = LCS_VARIABLE;
        break;
    default:
        assert(false);
    }
    batchDirs[column].oLastValHighMark = lastVal[column];
    batchDirs[column].nValHighMark = nVal[column];
    batchDirs[column].nVal = 0;
    batchDirs[column].oVal = batchOffset[column];
    batchDirs[column].nRow = 0;

    // # of bits it takes to represent 0 values
    nBits[column] = 0;
    nextWidthChange[column] = 1 ;

    maxValueSize[column] = 0;
}

void LcsClusterNodeWriter::pickCompressionMode(
    uint column, uint recSize, uint nRow, uint16_t **pValOffset,
    LcsBatchMode &compressionMode)
{
    uint        nByte;
    PLcsBatchDir pBatch;
    WidthVec    w;      // bitVec m_width vector
    uint        iV;     // number of bit vectors


    uint        szCompressed;   // size of the compressed batch
    uint        szVariable;     // size of the variable sized batch
    uint        szFixed;        // size of the fixed batch
    uint        szNonCompressed;
    uint        deltaVal;
    uint        batchRows;      // # of rows in the batch that is nRows rounded
                                // down to the nearest 8 boundary

    // update batch fields
    batchDirs[column].nRow = nRow;
    batchDirs[column].recSize = recSize;

    // calculate the size required for a compressed and sorted batch
    // by summing the spcae required for the value offsets, the bit vectors
    // and the values that were put in since the batch started

    // the # of rows in a batch will be smaller then 8 or a multiple
    // of 8. all but the last batch in a load will be greater then 8.
    batchRows = (nRow > 8) ? nRow & 0xfffffff8 : nRow;

    szCompressed = batchDirs[column].nVal*sizeof(uint16_t) +
                        (nBits[column]*nRow + LcsMaxSzLeftError * 8) / 8
                   + (batchDirs[column].oLastValHighMark - lastVal[column]);

    // the variable size batch does not have the bit vectors
    szVariable = batchDirs[column].nRow * sizeof(uint16_t)
                   + (batchDirs[column].oLastValHighMark - lastVal[column]);

    // calculate the size required for the non compressed fixed mode
    // add max left overs to allow left overs to be added back
    uint    leftOverSize;
    leftOverSize = LcsMaxLeftOver * sizeof(uint16_t) +
                    (3 * LcsMaxLeftOver + LcsMaxSzLeftError * 8) / 8
                       + LcsMaxLeftOver * recSize;
    szFixed = nRow * recSize + leftOverSize;

    szNonCompressed = std::min(szFixed, szVariable);

    // Should we store this block in one of the non-compressed modes (fixed or
    // variable)?  We do this if either
    //    1) the non-compressed size is smaller than the compressed size
    // or 2) we built the block in forced uncompress mode
    //
    // test if the compressed size is bigger then the non compressed size

    if ((fixed == bForceMode[column] || variable == bForceMode[column])
        || szCompressed > szNonCompressed) {
        // switch to one of the noncompressed modes
        *pValOffset = NULL;
        batchDirs[column].nVal = 0;

        forceModeCount[column]++;

        // If we are storing it in fixed mode...
        if (fixed == bForceMode[column] || szNonCompressed == szFixed) {
            // batch will be stored in fixed mode
            // change mode
            batchDirs[column].mode = LCS_FIXED;

            // Are we switching from variable mode to fixed mode?
            if (bForceMode[column] != fixed) {
                // We are going to store the batch in fixed mode.  But currently
                // it is saved in variable mode.  Save the value pointers into
                // pValBank and we will use them later to help in conversion

                // number of bytes taken by value over the batch high mark point
                deltaVal = batchDirs[column].oLastValHighMark - lastVal[column];

                // save the values obtained while this batch was in
                // variable mode
                if (deltaVal) {
                    memcpy(pValBank[column],
                           pBlock[column] + lastVal[column], deltaVal);
                }

                valBankStart[column] = 0;

                // mark that for the next few times, automatically go to
                // fixed mode without checking if it is the best
                bForceMode[column] = fixed;

                // Adjust szLeft since we have freed up some space in
                // the block
                assert(szVariable >= szFixed);
                szLeft += (szVariable - szFixed);
                assert(szLeft >= 0);
            } else {
                valBankStart[column] = lastVal[column];
            }

            // Reclaim the space at the bottom of the block that
            // used to hold the values

            // first offset included in the bank
            oValBank[column] = lastVal[column];
            lastVal[column] = batchDirs[column].oLastValHighMark;
            nVal[column] = batchDirs[column].nValHighMark;

            // the number of bytes taken by the batch
            nByte = batchRows * batchDirs[column].recSize;

        } else {
            // batch will be stored in variable mode

            batchDirs[column].mode = LCS_VARIABLE;

            // the number of bytes taken by the batch
            nByte = batchRows*sizeof(uint16_t);

            // mark that for the next few times, automatically go to
            // fixed mode without checking if it is the best
            bForceMode[column] = variable;
        }
    } else {
        // batch will be stored in compressed mode
        // values will be put at the start of the new batch

        *pValOffset = (uint16_t *)(pBlock[column] + batchOffset[column]);

        // calculate the bit vector widthes
        iV = bitVecWidth(nBits[column], w);

        // nByte is the # bytes taken by the batch, it is the sum of
        // the bit vectors size and the value offsets
        nByte = sizeofBitVec(batchRows, iV, w) +
                batchDirs[column].nVal * sizeof(uint16_t);

        // Adjust szLeft since we have freed up some space in the block
        assert(szVariable >= szCompressed);
        szLeft += (szVariable - szCompressed);
        assert(szLeft >= 0);
    }

    compressionMode = batchDirs[column].mode;

    // Slide down the batch directories to make room for new batch data (batch
    // directories occur after the batches).
    pBatch = (PLcsBatchDir)(pBlock[column] + batchOffset[column] + nByte);
    memmove(pBatch, pBlock[column] + batchOffset[column],
            batchCount[column]*sizeof(LcsBatchDir));

    // adjust szLeft for the space used by the next batch to reflect only
    // its batch directory
    szLeft -= sizeof(LcsBatchDir);
    szLeft = std::max(szLeft, 0);
    assert(szLeft >= 0);

    // batchDirs[column].oVal points to where the batch dir used to be,
    // and this is where the batch records will start
    batchDirs[column].oVal = batchOffset[column];

    // set batchOffset[column] to point to the start of the batch
    // directores (if we have another batch then this will become the
    // offset of the new batch)
    batchOffset[column] = (batchOffset[column] + nByte);
}

// myCopy: like memcpy(), but optimized for case where source
// and destination are the same (ie, when we have a single column
// cluster and we are copying from the index block to the temporary
// blocks, this will do nothing because the temp block just points
// back to the index block)
void myCopy(void* pDest, void* pSrc, uint sz)
{
    if (pDest == pSrc) {
        return;
    } else {
        memcpy(pDest, pSrc, sz);
    }
}

RecordNum LcsClusterNodeWriter::moveFromIndexToTemp()
{
    PLcsBatchDir pBatch;
    boost::scoped_array<uint16_t> batchDirOffset;
    uint16_t loc;
    uint column;
    uint batchCount = pHdr->nBatch / nClusterCols;
    uint b;

    batchDirOffset.reset(new uint16_t[pHdr->nBatch]);

    // First move the values
    //
    // copy values from index for all columns starting with the
    // 1st column in cluster.
    for (column = 0; column < nClusterCols; column++) {
        uint sz = firstVal[column] - lastVal[column];
        loc = (uint16_t) (szBlock - sz);
        myCopy(pBlock[column] + loc, pIndexBlock + lastVal[column], sz);

        // adjust lastVal and firstVal to offset in temporary block
        lastVal[column]  = loc;
        firstVal[column] = (uint16_t) szBlock;
    }

    // Next move the batches

    pBatch = (PLcsBatchDir)(pIndexBlock + pHdr->oBatch);
    for (column = 0; column < nClusterCols; column++) {
        uint i;
        loc = hdrSize;

        // move every batch for this column
        for (b = column, i = 0; i < batchCount; i++, b = b + nClusterCols) {
            uint16_t    batchStart = loc;

            if (pBatch[b].mode == LCS_COMPRESSED) {
                uint8_t     *pBit;
                WidthVec    w;      // bitVec m_width vector
                PtrVec      p;      // bitVec offsets
                uint        iV;     // # of bit vectors
                uint        sizeOffsets, nBytes;

                //copy offsets
                sizeOffsets =  pBatch[b].nVal * sizeof(uint16_t);
                myCopy(
                    pBlock[column] + loc, pIndexBlock + pBatch[b].oVal,
                    sizeOffsets);

                // step past offsets
                loc = (uint16_t) (loc + sizeOffsets);

                // calculate the bit vector widthes
                iV = bitVecWidth(calcWidth(pBatch[b].nVal), w);

                // this is where the bit vectors start
                pBit = pIndexBlock + pBatch[b].oVal + sizeOffsets;

                // nByte are taken by the bit vectors
                nBytes = bitVecPtr(pBatch[b].nRow, iV, w, p, pBit);

                myCopy(pBlock[column] + loc, pBit, nBytes);

                // step past bit vectors
                loc = (uint16_t) (loc + nBytes);
            } else if (pBatch[b].mode == LCS_VARIABLE) {
                uint        sizeOffsets;

                sizeOffsets = pBatch[b].nRow * sizeof(uint16_t);

                // variable size record batch
                myCopy(
                    pBlock[column] + loc, pIndexBlock + pBatch[b].oVal,
                    sizeOffsets);

                // step past offsets
                loc = (uint16_t) (loc + sizeOffsets);
            } else {
                // fixed mode batch
                uint sizeFixed;

                sizeFixed =  pBatch[b].nRow * pBatch[b].recSize;
                // fixed size record batch
                myCopy(
                    pBlock[column] + loc, pIndexBlock + pBatch[b].oVal,
                    sizeFixed);

                //step past fixed records
                loc = (uint16_t) (loc + sizeFixed);
            }

            // set offset where values start in temp block
            batchDirOffset[b] = batchStart;
        }

        // move batch directories for this column

        uint16_t  dirLoc;
        b = column;
        dirLoc = loc;
        batchOffset[column] = dirLoc;

        // move every batch for this column
        for (i = 0; i < batchCount; i++) {
            PLcsBatchDir pTempBatch = (PLcsBatchDir)(pBlock[column] + dirLoc);
            myCopy(pTempBatch, &pBatch[b], sizeof(LcsBatchDir));

            pTempBatch->oVal = batchDirOffset[b];
            // increment to next batch and next location in temp block
            b = b + nClusterCols;
            dirLoc += sizeof(LcsBatchDir);
        }
    }

    // compute the number of rows on the page
    pBatch = (PLcsBatchDir)(pIndexBlock + pHdr->oBatch);
    RecordNum nrows = 0;
    for (b = 0; b < pHdr->nBatch; b = b + nClusterCols) {
        nrows += pBatch[b].nRow;
    }

    batchDirOffset.reset();
    return nrows;
}

void LcsClusterNodeWriter::moveFromTempToIndex()
{
    PLcsBatchDir pBatch;
    uint        sz, numBatches = batchCount[0];
    uint16_t    offset, loc;
    uint        column, b;

    // Copy values from temporary blocks for all columns starting with the
    // 1st column in cluster.

    for (offset = (uint16_t) szBlock, column = 0; column < nClusterCols;
        column++)
    {
        sz = szBlock - lastVal[column];
        myCopy(
            pIndexBlock + (offset - sz), pBlock[column] + lastVal[column], sz);

        //  set delta value to subtract from offsets to get relative offset
        delta[column] = (uint16_t)(szBlock - offset);

        // adjust firstVal and lastVal in the leaf block header to appropriate
        // offsets in index block (currently base on offsets in temporary block)
        firstVal[column] = offset;
        offset = (uint16_t) (offset - sz);
        lastVal[column] = offset;
    }

    // copy batch descriptors (which point to the batches)

    for (loc =  hdrSize, b = 0; b < numBatches; b++) {
        for (column = 0; column < nClusterCols; column++) {
            uint16_t    batchStart = loc;

            pBatch = (PLcsBatchDir)(pBlock[column] + batchOffset[column]);

            if (pBatch[b].mode == LCS_COMPRESSED) {
                uint8_t     *pBit;
                WidthVec    w;      // bitVec m_width vector
                PtrVec      p;      // bitVec offsets
                uint        iV;     // # of bit vectors
                uint        sizeOffsets, nBytes;

                sizeOffsets =  pBatch[b].nVal * sizeof(uint16_t);

                // first copy offsets then bit vectors
                myCopy(
                    pIndexBlock + loc, pBlock[column] + pBatch[b].oVal,
                    sizeOffsets);

                // step past offsets
                loc = (uint16_t) (loc + sizeOffsets);

                // calculate the bit vector widthes
                iV = bitVecWidth(calcWidth(pBatch[b].nVal), w);

                // this is where the bit vectors start in temporary block
                pBit = pBlock[column] + pBatch[b].oVal + sizeOffsets;

                // nByte are taken by the bit vectors
                nBytes = bitVecPtr(pBatch[b].nRow, iV, w, p, pBit);

                myCopy(pIndexBlock + loc, pBit, nBytes);

                // step past bit vectors
                loc = (uint16_t)(loc + nBytes);

            } else if (pBatch[b].mode == LCS_VARIABLE) {
                uint        sizeOffsets;

                sizeOffsets =  pBatch[b].nRow * sizeof(uint16_t);

                // variable size record batch
                myCopy(
                    pIndexBlock + loc, pBlock[column] + pBatch[b].oVal,
                    sizeOffsets);

                // step past offsets
                loc = (uint16_t) (loc + sizeOffsets);
            } else {
                // Fixed mode
                uint sizeFixed;

                sizeFixed =  pBatch[b].nRow * pBatch[b].recSize;
                // fixed size record batch
                myCopy(
                    pIndexBlock + loc, pBlock[column] + pBatch[b].oVal,
                    sizeFixed);

                //step past fixed records
                loc = (uint16_t) (loc + sizeFixed);
            }

            // set offset where values start in indexBlock
            pBatch[b].oVal = batchStart;
        }
    }

    //adjust batch count in leaf block header
    pHdr->nBatch = nClusterCols * numBatches;

    // start batch directory at end of last batch
    pHdr->oBatch = loc;

    // copy batch directories
    for (b = 0; b < numBatches; b++) {
        for (column = 0; column < nClusterCols; column++) {
            pBatch = (PLcsBatchDir)(pBlock[column] + batchOffset[column]);
            myCopy(pIndexBlock + loc, &pBatch[b], sizeof(LcsBatchDir));
            loc += sizeof(LcsBatchDir);
        }
    }

    if (isTracingLevel(TRACE_FINE)) {
        FENNEL_TRACE(
            TRACE_FINE, "Calling ClusterDump from moveFromTempToIndex");
        clusterDump->dump(opaqueToInt(clusterPageId), pHdr, szBlock);
    }
}

void LcsClusterNodeWriter::allocArrays()
{
    // allocate arrays only if they have not been allocated already
    if (!arraysAllocated) {
        arraysAllocated = true;

        batchDirs.reset(new LcsBatchDir[nClusterCols]);

        pValBank.reset(new PBuffer[nClusterCols]);

        // allocate larger buffers for the individual pages in the value bank

        attrAccessors.reset(new UnalignedAttributeAccessor[nClusterCols]);

        for (uint col = 0; col < nClusterCols; col++) {
            bufferLock.allocatePage();
            pValBank[col] = bufferLock.getPage().getWritableData();
            // Similar to what's done in external sorter, we rely on the fact
            // that the underlying ScratchSegment keeps the page pinned for us.
            // The pages will be released when all other pages associated with
            // the ScratchSegment are released.
            bufferLock.unlock();

            attrAccessors[col].compute(colTupleDesc[col]);
        }

        valBankStart.reset(new uint16_t[nClusterCols]);

        forceModeCount.reset(new uint[nClusterCols]);

        bForceMode.reset(new ForceMode[nClusterCols]);

        oValBank.reset(new uint16_t[nClusterCols]);

        batchOffset.reset(new uint16_t[nClusterCols]);

        batchCount.reset(new uint[nClusterCols]);

        nBits.reset(new uint[nClusterCols]);

        nextWidthChange.reset(new uint[nClusterCols]);

        maxValueSize.reset(new uint[nClusterCols]);
    }

    memset(valBankStart.get(), 0, nClusterCols * sizeof(uint16_t));
    memset(forceModeCount.get(), 0, nClusterCols * sizeof(uint));
    memset(bForceMode.get(), 0, nClusterCols * sizeof(ForceMode));
    memset(oValBank.get(), 0, nClusterCols * sizeof(uint16_t));
    memset(batchOffset.get(), 0, nClusterCols * sizeof(uint16_t));
    memset(batchCount.get(), 0, nClusterCols * sizeof(uint));
    memset(nBits.get(), 0, nClusterCols * sizeof(uint));
    memset(nextWidthChange.get(), 0, nClusterCols * sizeof(uint));
    memset(maxValueSize.get(), 0, nClusterCols * sizeof(uint));
}


FENNEL_END_CPPFILE("$Id$");

// End LcsClusterNodeWriter.cpp
