/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
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
#include <boost/scoped_array.hpp>

FENNEL_BEGIN_CPPFILE("$Id$");

LcsClusterNodeWriter::LcsClusterNodeWriter()
{
    m_numColumns = 0;
    m_pHdr = 0;
    m_pHdrSize = 0;
    m_indexBlock = 0;
    m_pBlock = 0;
    m_szBlock = 0;
    m_rIMinSzLeft = 0;
    m_batch.reset();
    m_pValBank.reset();
    m_oValBank.reset();
    m_batchOffset.reset();
    m_batchCount.reset();
    m_szLeft = 0;
    m_nBits.reset();
    m_nextWidthChange.reset();
    m_allocArrays = false;
    m_pValBankStart.reset();
    m_bForceMode.reset();
    m_forceModeCount.reset();
    m_maxValueSize.reset();
}

LcsClusterNodeWriter::~LcsClusterNodeWriter()
{
    Close();
}

void LcsClusterNodeWriter::Close()
{
    m_batch.reset();
    m_pValBank.reset();
    m_pValBankStart.reset();
    m_forceModeCount.reset();
    m_bForceMode.reset();
    m_oValBank.reset();
    m_batchOffset.reset();
    m_batchCount.reset();
    m_nBits.reset();
    m_nextWidthChange.reset();
    m_maxValueSize.reset();
}


void LcsClusterNodeWriter::Init(SegmentAccessor const &accessor,
                                uint16_t nColumn, PBuffer iBlock, PBuffer *pB,
                                uint16_t szB)
{
    scratchAccessor = accessor;
    bufferLock.accessSegment(scratchAccessor);
    m_numColumns = nColumn;
    m_indexBlock = iBlock;
    m_pBlock = pB;
    m_szBlock = szB;
    m_pHdr = (PLcsClusterNode) m_indexBlock;

    m_pHdrSize = (uint16_t) GetClusterSubHeaderSize(m_numColumns);

    // initialize lastVal, firstVal, and nVal fields in the header
    // to point to the appropriate positions in the indexBlock
    
    m_pHdr->lastVal = (uint16_t *)((PBuffer) m_pHdr + sizeof(LcsClusterNode));
    m_pHdr->firstVal = (uint16_t *)((PBuffer) m_pHdr->lastVal +
                                    sizeof(uint16_t) * m_numColumns);
    m_pHdr->nVal = (uint16_t *)((PBuffer) m_pHdr->firstVal +
                                    sizeof(uint16_t) * m_numColumns);
    m_pHdr->delta = (uint16_t *) ((PBuffer) m_pHdr->nVal +
                                    sizeof(uint16_t) * m_numColumns);

    m_rIMinSzLeft = m_numColumns * (LcsMaxLeftOver * sizeof(uint16_t) +
                     sizeof(LcsBatchDir));

    AllocArrays();
}

void LcsClusterNodeWriter::OpenNew(Rid startRID)
{
    int i;

    // Inialize block header and batches
    m_pHdr->firstRID = startRID;
    m_pHdr->nColumn = m_numColumns;
    m_pHdr->nBatch = 0;
    m_pHdr->oBatch = m_pHdrSize;

    for (i = 0; i < m_numColumns; i++) {
        m_pHdr->lastVal[i] = m_szBlock;
        m_pHdr->firstVal[i] = (uint16_t) m_szBlock;
        m_pHdr->nVal[i] = 0;
        m_pHdr->delta[i] = 0;
        m_batch[i].mode = LCS_COMPRESSED;
        m_batch[i].nVal = 0;
        m_batch[i].nRow = 0;
        m_batch[i].oVal = 0;
        m_batch[i].oLastValHighMark = m_pHdr->lastVal[i];
        m_batch[i].nValHighMark = m_pHdr->nVal[i];
        m_batchOffset[i] = m_pHdrSize;
        // # of bits it takes to represent 0 values
        m_nBits[i] = 0;
        m_nextWidthChange[i] = 1;
        m_batchCount[i] = 0;
    }

    // account for the header size, account for at least 1 batch for each column
    // and leave space for one addtional batch for a "left-over" batch

    m_szLeft = m_szBlock - m_pHdrSize - (2*sizeof(LcsBatchDir)) * m_numColumns;
    m_szLeft = std::max(m_szLeft, 0);
    assert(m_szLeft >= 0);
}

void LcsClusterNodeWriter::OpenAppend(uint16_t *nVal, uint16_t *lastVal,
                                      RecordNum &nrows)
{
    int i;

    // leave space for one batch for each column entry
    m_szLeft = m_pHdr->lastVal[m_numColumns-1] - m_pHdr->oBatch -
                (m_pHdr->nBatch + 2*m_numColumns) * sizeof(LcsBatchDir);
    m_szLeft = std::max(m_szLeft, 0);
    assert(m_szLeft >= 0);

    // Let's move the values, batch directories, and batches to
    // temporary blocks from index block
    nrows = MoveFromIndexToTemp();

    for (i = 0; i < m_numColumns; i++) {
        nVal[i] = m_pHdr->nVal[i];
        lastVal[i] = m_pHdr->lastVal[i];
        memset(&m_batch[i], 0, sizeof(LcsBatchDir));

        m_batch[i].oLastValHighMark = m_pHdr->lastVal[i];
        m_batch[i].nValHighMark = m_pHdr->nVal[i];
        m_batch[i].mode = LCS_COMPRESSED;

        // # of bits it takes to represent 0 values
        m_nBits[i] = 0;
        m_nextWidthChange[i] = 1;

        m_oValBank[i] = 0;
        m_batchCount[i] = (uint16_t) (m_pHdr->nBatch/m_numColumns);
    }
}

void LcsClusterNodeWriter::DescribeLastBatch(uint16_t column, uint &dRow,
                                                uint16_t &recSize)
{
    PLcsBatchDir pBatch;

    pBatch = (PLcsBatchDir) (m_pBlock[column] + m_batchOffset[column]);
    dRow = pBatch[m_batchCount[column] -1].nRow % 8;
    recSize = pBatch[m_batchCount[column] -1].recSize;
}

uint16_t LcsClusterNodeWriter::GetNextVal(uint16_t column, uint16_t thisVal)
{
    if (thisVal && thisVal != m_szBlock)
        return (uint16_t) (thisVal +
            TupleDatum(m_pBlock[column] + thisVal).getStorageLength());
    else
        return 0;
}

void LcsClusterNodeWriter::RollBackLastBatch(uint16_t column, PBuffer pBuf)
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

    // load last batch, nBatch must be at least 1
    pBatch = (PLcsBatchDir)(m_pBlock[column] + m_batchOffset[column]);
    m_batch[column]  = pBatch[m_batchCount[column] -1];

    // compute size left in temporary block before roll back
    origSzLeft = m_pHdr->lastVal[column] - m_batchOffset[column] -
                    (m_batchCount[column]+2)*sizeof(LcsBatchDir);

    if ((m_batch[column].nRow > 8) || (m_batch[column].nRow % 8) == 0)
        return;

    if (m_batch[column].mode == LCS_COMPRESSED) {
        // calculate the bit vector widthes
        iV = BitVecWidth(CalcWidth(m_batch[column].nVal), w);

        // this is where the bit vectors start
        pBit = m_pBlock[column] + m_batch[column].oVal +
                m_batch[column].nVal * sizeof(uint16_t);

        // nByte are taken by the bit vectors
        BitVecPtr(m_batch[column].nRow, iV, w, p, pBit);

        // there are at most 8 rows in this batch
        ReadBitVecs(rows, iV, w, p, 0, m_batch[column].nRow);

        // get the address of the batches value offsets
        pValOffsets = (uint16_t *)(m_pBlock[column] + m_batch[column].oVal);

        // fill up buffer with batches values
        for (i = 0; i < m_batch[column].nRow;
                i++, pBuf += m_batch[column].recSize)
            memcpy(pBuf, m_pBlock[column] + pValOffsets[rows[i]],
                    m_batch[column].recSize);

    } else if (m_batch[column].mode == LCS_FIXED) {
        // fixed size record batch
        // copy the values into the given buffer
        memcpy(pBuf, m_pBlock[column] + m_batch[column].oVal,
                m_batch[column].nRow*m_batch[column].recSize);
    } else {
        // variable sized records (batch.mode == LCS_VARIABLE)
        // get the address of the batches value offsets
        pValOffsets = (uint16_t *)(m_pBlock[column] + m_batch[column].oVal);

        // fill up buffer with batches values
        for (i = 0; i < m_batch[column].nRow;
                i++, pBuf += m_batch[column].recSize)
            memcpy(pBuf, m_pBlock[column] + pValOffsets[i],
                    m_batch[column].recSize);
    }

    // Reset the last batch
    m_batchCount[column]--;
    // batch dir offset points to the beginning of the rolled back batch
    m_batchOffset[column] = m_batch[column].oVal;

    // copy the batch dir back to the end of the prev batch.
    memmove(m_pBlock[column] + m_batchOffset[column], pBatch,
            m_batchCount[column] * sizeof(LcsBatchDir));

    // recalc size left
    // leave place for one new batch(the rolled back one will be rewriten)
    // and possibley one to follow.  Subtract the difference of the new size
    // and the original size and add this to szLeft in index variable
    int newSz;
    newSz = m_pHdr->lastVal[column] - m_batchOffset[column] -
            (m_batchCount[column] + 2) * sizeof(LcsBatchDir);
    m_szLeft += (newSz - origSzLeft);
    m_szLeft = std::max(m_szLeft, 0);
    assert(m_szLeft >= 0);

    // # of bits it takes to represent 0 values
    m_nBits[column] = 0;
    m_nextWidthChange[column] = 1;

    // set batch parameters
    m_batch[column].mode = LCS_COMPRESSED;
    m_batch[column].nVal = 0;
    m_batch[column].nRow = 0;
    m_batch[column].oVal = 0;
    m_batch[column].recSize = 0;
}

// AddValue() where the current value already exists

bool LcsClusterNodeWriter::AddValue(uint16_t column, bool bFirstTimeInBatch)
{
    // Calculate szleft assuming the value gets added.
    m_szLeft -= sizeof(uint16_t);

    // if there is not enough space left, reject value
    if (m_szLeft < m_numColumns * LcsMaxSzLeftError) {
        // set szLeft to its previous value
        m_szLeft += sizeof(uint16_t);
        assert(m_szLeft >= 0);
        return false;
    }

    if (bFirstTimeInBatch) {
        // there is enough space to house the value, increment batch
        // value count
        m_batch[column].nVal++;

        // check if nBits needs to change by comparing the value count
        // the change point count
        if (m_batch[column].nVal == m_nextWidthChange[column]) {
            // calculate the next nBits value, and the count of values
            // for the next chane
            m_nBits[column] = CalcWidth(m_batch[column].nVal);
            m_nextWidthChange[column] = (1 << m_nBits[column]) + 1;
        }
    }

    return true;
}

// AddValue() where the value must be added to the bottom of the page

bool LcsClusterNodeWriter::AddValue(uint16_t column, PBuffer pVal, uint16_t *oVal)
{
    uint16_t lastVal;
    int oldSzLeft = m_szLeft;
    uint szVal = TupleDatum(pVal).getStorageLength();
    
    // if we are in forced fixed compression mode,
    // see if the maximum record size in this batch has increased.
    // if so, adjust the szLeft based on the idea that each previous element
    // will now be taking more space
    if (m_bForceMode[column] == fixed) {
        if (szVal > m_maxValueSize[column]) {
            m_szLeft -= m_batch[column].nVal * (szVal - m_maxValueSize[column]);
            m_maxValueSize[column] = szVal;
        }
    }

    // adjust szleft (upper bound on amount of space left in the block).
    // If we are in a forced fixed compression mode then we can calculate this
    // exactly.  If we are in "none" mode, then we calculate szLeft according to
    // variable mode compression (which should be an upper bound), and adjust it
    // later in PickCompressionMode
    if (m_bForceMode[column] == fixed) {
        m_szLeft -= m_maxValueSize[column];
    } else {
        // assume value is being added in variable mode
        // (note: this assumes that whenever we convert from
        // variable mode to compressed mode, the compressed mode will
        // take less space, for only in this case is szLeft an upper bound)
        m_szLeft -= (sizeof(uint16_t) + szVal) ;
    }

    // if there is not enough space left reject value
    if (m_szLeft < m_numColumns * LcsMaxSzLeftError) {
        // set szLeft to its previous value
        m_szLeft = oldSzLeft;
        assert(m_szLeft >= 0);
        return false;
    }

    // otherwise, go ahead and add the value...

    lastVal = m_pHdr->lastVal[column] - szVal;

    // there is enough space to house the value, increment batch value count
    m_batch[column].nVal++;

    // check if nBits needs to change by comparing the value count
    // the change point count
    if (m_batch[column].nVal == m_nextWidthChange[column]) {
        // calculate the next nBits value, and the count of values
        // for the next chane
        m_nBits[column] = CalcWidth(m_batch[column].nVal);
        m_nextWidthChange[column] = (1 << m_nBits[column]) + 1;
    }

    m_pHdr->lastVal[column] = lastVal;

    // Save the value being inserted.  If we are building the
    // block in fixed mode then save the value into m_pValBank
    // rather than saving it into the block
    if (fixed == m_bForceMode[column])
        memcpy(m_pValBank[column] + lastVal, pVal, szVal);
    else
        memcpy(m_pBlock[column] + lastVal, pVal, szVal);

    // return the block offset of the new value;
    *oVal = lastVal;

    m_pHdr->nVal[column]++;

    return true;
}

void LcsClusterNodeWriter::UndoValue(uint16_t column, PBuffer pVal,
                                     bool bFirstInBatch)
{
    // pVal may be null if the value already exists, in which case, it wasn't
    // added to the value list.  However, if it was the first such value for
    // the batch, AddValue was called to bump-up the batch value count
    // so we still need to call UndoValue
    uint szVal = (pVal) ? TupleDatum(pVal).getStorageLength() : 0;
  
    // add back size subtracted for offset
    m_szLeft += (sizeof(uint16_t) + szVal) ;
    assert(m_szLeft >= 0);

    // If value was new to the batch, then adjust counters
    if (bFirstInBatch) {
        // decrement batch count
        m_batch[column].nVal--;

        //reset nextWidthChange
        if (0 == m_batch[column].nVal)
            m_nextWidthChange[column] = 1;
        else {
            // calculate the next nBits value, and the count of values
            // for the next chane
            m_nBits[column] = CalcWidth(m_batch[column].nVal);
            m_nextWidthChange[column] = (1 << m_nBits[column]) + 1;
        }
    }

    if (pVal) {
        // upgrage header
        m_pHdr->lastVal[column] = (m_pHdr->lastVal[column] + szVal);
        m_pHdr->nVal[column]--;
    }
}

void LcsClusterNodeWriter::PutCompressedBatch(uint16_t column, PBuffer pRows,
                                              PBuffer pBuf)
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

    // PickCompressionMode() was called prior to PutCompressedBatch,
    // and the following has been already done:
    // -- the batch descriptors were moved to the back of the batch
    // -- a batch descriptor for this batch has been placed in the batch
    //    directory
    // -- this->batch contains up to date info
    // -- the caller has copied nVal value offsets to the head of this batch

    // write to buffer values for rows over the 8 boundary if nrow is
    // greater then 8

    if (m_batch[column].nRow > 8) {
        pOffs = (uint16_t *)(m_pBlock[column] + m_batch[column].oVal);
        for (i = m_batch[column].nRow & 0xfffffff8; i < m_batch[column].nRow;
                i++, pBuf += m_batch[column].recSize) {
            iRow = ((uint16_t *) pRows)[i];
            memcpy(pBuf, m_pBlock[column] + pOffs[iRow],
                    m_batch[column].recSize);
        }
        m_batch[column].nRow = m_batch[column].nRow &0xfffffff8;
    }

    // calculate the bit vector widthes, sum(w[i]) is m_nBits
    iV = BitVecWidth(m_nBits[column], w);

    // this is where the bit vectors start
    pBit = m_pBlock[column] + m_batch[column].oVal +
            m_batch[column].nVal*sizeof(uint16_t);

    // nByte are taken by the bit vectors, clear them befor OR-ing
    nByte = BitVecPtr(m_batch[column].nRow, iV, w, p, pBit);
    memset(pBit, 0, nByte);

    for (j = 0, b = 0; j < iV ; j++) {
        switch(w[j])
        {
        case 16:
            memcpy(p[j], pRows, m_batch[column].nRow * sizeof(uint16_t));
            break;

        case 8:
            for (i = 0; i < m_batch[column].nRow ; i++)
                (p[j])[i] = (uint8_t)((uint16_t *) pRows)[i];
            break;

        case 4:
            for (i = 0; i < m_batch[column].nRow ; i++)
                SetBits(p[j] + i/2 , 4, (i % 2) * 4,
                        (uint16_t)(((uint16_t *) pRows)[i] >> b));
            break;

        case 2:
            for (i = 0; i < m_batch[column].nRow ; i++)
                SetBits(p[j] + i/4 , 2, (i % 4) * 2,
                        (uint16_t)(((uint16_t *) pRows)[i] >> b));
            break;

        case 1:
            for (i = 0; i < m_batch[column].nRow ; i++)
                SetBits(p[j] + i/8 , 1, (i % 8),
                        (uint16_t)(((uint16_t *)pRows)[i] >> b));
            break;

        default:
                ;
        }
        b += w[j];
    }

    // put the batch in the batch directory
    pBatch = (PLcsBatchDir)(m_pBlock[column] + m_batchOffset[column]);
    pBatch[m_batchCount[column]] = m_batch[column];
    m_batchCount[column]++;

    // reset the batch state
    m_batch[column].mode = LCS_COMPRESSED;
    m_batch[column].oLastValHighMark = m_pHdr->lastVal[column];
    m_batch[column].nValHighMark = m_pHdr->nVal[column];
    m_batch[column].nVal = 0;
    m_batch[column].oVal = m_batchOffset[column];
    m_batch[column].nRow = 0;

    // # of bits it takes to represent 0 values
    m_nBits[column] = 0;
    m_nextWidthChange[column] = 1 ;
}

void LcsClusterNodeWriter::PutFixedVarBatch(uint16_t column, uint16_t *pRows,
                                            PBuffer pBuf)
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
    batchRows = (m_batch[column].nRow > 8)
                    ? m_batch[column].nRow & 0xfffffff8 : m_batch[column].nRow;

    // the value destination
    pVal = m_pBlock[column] + m_batch[column].oVal;
    if (m_batch[column].mode == LCS_VARIABLE) {
        // For a variable mode, copy in the value offsets into the RI block.
        // The left over i.e. the rows over the highest 8 boundary will be
        // copied to pBuf
        memcpy(pVal, pRows, batchRows * sizeof(uint16_t));
    } else {

        // it's a fixed record batch
        assert(m_batch[column].mode == LCS_FIXED);

        batchRecSize  = m_batch[column].recSize;
        localLastVal  = m_pHdr->lastVal[column];
        localpValBank = m_pValBank[column] + m_pValBankStart[column];
        localoValBank = m_oValBank[column];
        localpBlock   = m_pBlock[column];

        // Copy the values themselves into the block.
        // The values are currently stored in m_pValBank
        for (i = 0; i < batchRows; i++) {
            // ValueSource determines by the offset whether the value comes from
            // the bank of from the block
            src = ValueSource(localLastVal, localpValBank, localoValBank,
                                localpBlock, pRows[i]);
            memcpy(pVal, src, batchRecSize);
            pVal += batchRecSize;
        }
    }

    // if forced fixed mode is true we need to periodically set it false, so
    // we can at least check if the data can be compressed
    if (m_bForceMode[column] != none) {
        if (m_forceModeCount[column] > 20) {
            m_bForceMode[column] = none;
            m_forceModeCount[column] = 0;
        }
    }

    batchRecSize  = m_batch[column].recSize;
    localLastVal  = m_pHdr->lastVal[column];
    localpValBank = m_pValBank[column] + m_pValBankStart[column];
    localoValBank = m_oValBank[column];
    localpBlock   = m_pBlock[column];

    // copy the tail of the batch (the last nRow % 8 values) to pBuf
    pVal = pBuf;
    for (i = batchRows; i < m_batch[column].nRow; i++) {
        // ValueSource determines by the offset whether the value comes from
        // the bank or from the block. if the value bank is not used
        // ValueSource will get all the values fron the block
        src = ValueSource(localLastVal, localpValBank, localoValBank,
                            localpBlock, pRows[i]);
        memcpy(pVal, src, m_batch[column].recSize);
        pVal += m_batch[column].recSize;
    }

    if (m_pValBank[column]) {
        m_oValBank[column] = 0;
    }

    // Put batch descriptor in batch directory
    m_batch[column].nRow = batchRows;
    pBatch = (PLcsBatchDir)(m_pBlock[column] + m_batchOffset[column]);
    pBatch[m_batchCount[column]] = m_batch[column];

    // inc. batch count
    m_batchCount[column]++;

    // reset the batch state.  set batch back to compressed mode
    // unless the flag has been set that next
    switch(m_bForceMode[column]) {
    case none:
        m_batch[column].mode = LCS_COMPRESSED;
        break;
    case fixed:
        m_batch[column].mode = LCS_FIXED;
        break;
    case variable:
        m_batch[column].mode = LCS_VARIABLE;
        break;
    default:
        assert(false);
    }
    m_batch[column].oLastValHighMark = m_pHdr->lastVal[column];
    m_batch[column].nValHighMark = m_pHdr->nVal[column];
    m_batch[column].nVal = 0;
    m_batch[column].oVal = m_batchOffset[column];
    m_batch[column].nRow = 0;

    // # of bits it takes to represent 0 values
    m_nBits[column] = 0;
    m_nextWidthChange[column] = 1 ;

    m_maxValueSize[column] = 0;
}

void LcsClusterNodeWriter::PickCompressionMode(uint16_t column, uint recSize,
                            uint nRow, uint16_t **pValOffset,
                            uint16_t &compressionMode)
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
    m_batch[column].nRow = nRow;
    m_batch[column].recSize = (uint16_t) recSize;

    // calculate the size required for a compressed and sorted batch
    // by summing the spcae required for the value offsets, the bit vectors
    // and the values that were put in since the batch started

    // the # of rows in a batch will be smaller then 8 or a multiple
    // of 8. all but the last batch in a load will be greater then 8.
    batchRows = (nRow > 8) ? nRow & 0xfffffff8 : nRow;

    szCompressed = m_batch[column].nVal*sizeof(uint16_t) +
                        (m_nBits[column]*nRow + LcsMaxSzLeftError * 8) / 8
                   + (m_batch[column].oLastValHighMark -
                           m_pHdr->lastVal[column]);

    // the variable size batch does not have the bit vectors
    szVariable = m_batch[column].nRow * sizeof(uint16_t)
                   + (m_batch[column].oLastValHighMark -
                           m_pHdr->lastVal[column]);

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

    if ((fixed == m_bForceMode[column] || variable == m_bForceMode[column])
        || szCompressed > szNonCompressed) {
        // switch to one of the noncompressed modes
        *pValOffset = NULL;
        m_batch[column].nVal = 0;

        m_forceModeCount[column]++;

        // If we are storing it in fixed mode...
        if (fixed == m_bForceMode[column] || szNonCompressed == szFixed) {
            // batch will be stored in fixed mode
            // change mode
            m_batch[column].mode = LCS_FIXED;

            // Are we switching from variable mode to fixed mode?
            if (m_bForceMode[column] != fixed) {
                // We are going to store the batch in fixed mode.  But currently
                // it is saved in variable mode.  Save the value pointers into
                // m_pValBank and we will use them later to help in conversion

                // number of bytes taken by value over the batch high mark point
                deltaVal = m_batch[column].oLastValHighMark -
                            m_pHdr->lastVal[column];

                // save the values obtained while this batch was in
                // variable mode
                if (deltaVal) {
                    memcpy(m_pValBank[column],
                            m_pBlock[column] + m_pHdr->lastVal[column],
                            deltaVal);
                }

                m_pValBankStart[column] = 0;

                // mark that for the next few times, automatically go to
                // fixed mode without checking if it is the best
                m_bForceMode[column] = fixed;

                // Adjust m_szLeft since we have freed up some space in
                // the block
                assert(szVariable >= szFixed);
                m_szLeft += (szVariable - szFixed);
                assert(m_szLeft >= 0);
            } else
                m_pValBankStart[column] = m_pHdr->lastVal[column];

            // Reclaim the space at the bottom of the block that
            // used to hold the values

            // first offset included in the bank;
            m_oValBank[column] = m_pHdr->lastVal[column];
            m_pHdr->lastVal[column] = m_batch[column].oLastValHighMark;
            m_pHdr->nVal[column] = m_batch[column].nValHighMark;

            // the number of bytes taken by the batch
            nByte = batchRows * m_batch[column].recSize;

        } else {

            // batch will be stored in variable mode

            m_batch[column].mode = LCS_VARIABLE;

            // the number of bytes taken by the batch
            nByte = batchRows*sizeof(uint16_t);

            // mark that for the next few times, automatically go to
            // fixed mode without checking if it is the best
            m_bForceMode[column] = variable;
        }
    } else {

        // batch will be stored in compressed mode
        // values will be put at the start of the new batch

        *pValOffset = (uint16_t *)(m_pBlock[column] + m_batchOffset[column]);

        // calculate the bit vector widthes
        iV = BitVecWidth(m_nBits[column], w);

        // nByte is the # bytes taken by the batch, it is the sum of
        // the bit vectors size and the value offsets
        nByte = SizeofBitVec(batchRows, iV, w) +
                m_batch[column].nVal * sizeof(uint16_t);

        // Adjust m_szLeft since we have freed up some space in the block
        assert(szVariable >= szCompressed);
        m_szLeft += (szVariable - szCompressed);
        assert(m_szLeft >= 0);
    }

    compressionMode = m_batch[column].mode;

    // Slide down the batch directories to make room for new batch data (batch
    // directories occur after the batches).
    pBatch = (PLcsBatchDir)(m_pBlock[column] + m_batchOffset[column] + nByte);
    memmove(pBatch, m_pBlock[column] + m_batchOffset[column],
            m_batchCount[column]*sizeof(LcsBatchDir));

    // adjust m_szLeft for the space used by the next batch to reflect only
    // its batch directory
    m_szLeft -= sizeof(LcsBatchDir);
    m_szLeft = std::max(m_szLeft, 0);
    assert(m_szLeft >= 0);

    // m_batch[column].oVal points to where the batch dir used to be,
    // and this is where the batch records will start
    m_batch[column].oVal = m_batchOffset[column];

    // set m_batchOffset[column] to point to the start of the batch
    // directores (if we have another batch then this will become the
    // offset of the new batch)
    m_batchOffset[column] = (uint16_t)(m_batchOffset[column] + nByte);
}

// myCopy: like memcpy(), but optimized for case where source
// and destination are the same (ie, when we have a single column
// cluster and we are copying from the index block to the temporary
// blocks, this will do nothing because the temp block just points
// back to the index block)
void myCopy(void* pDest, void* pSrc, uint sz)
{
    if (pDest == pSrc)
        return;
    else
        memcpy(pDest, pSrc, sz);
}

RecordNum LcsClusterNodeWriter::MoveFromIndexToTemp()
{
    PLcsBatchDir pBatch;
    boost::scoped_array<uint16_t> batchDirOffset;
    uint16_t loc;
    uint16_t column;
    uint16_t m_batchCount = (uint16_t)(m_pHdr->nBatch / m_numColumns);
    RecordNum nrows;

    batchDirOffset.reset(new uint16_t[m_pHdr->nBatch]);
    
    // First move the values
    //
    // copy values from index for all columns starting with the
    // 1st column in cluster.
    for (column = 0; column < m_numColumns; column++) {
        uint16_t sz = (uint16_t)(m_pHdr->firstVal[column] -
                                     m_pHdr->lastVal[column]);
        loc = (uint16_t) (m_szBlock - sz);
        myCopy(m_pBlock[column] + loc, m_indexBlock + m_pHdr->lastVal[column],
                sz);

        // adjust lastVal and firstVal to offset in temporary block
        m_pHdr->lastVal[column]  = loc;
        m_pHdr->firstVal[column] = (uint16_t) m_szBlock;
    }

    // Next move the batches

    pBatch = (PLcsBatchDir)(m_indexBlock + m_pHdr->oBatch);
    for (column = 0; column < m_numColumns; column++) {
        uint16_t b;
        uint i;
        loc = m_pHdrSize;
        nrows = 0;

        // move every batch for this column
        for (b = column, i = 0; i < m_batchCount;
                i++, b = (uint16_t) (b + m_numColumns)) {
            uint16_t    batchStart = loc;

            nrows += pBatch[b].nRow;
            if (pBatch[b].mode == LCS_COMPRESSED) {
                uint8_t     *pBit;
                WidthVec    w;      // bitVec m_width vector
                PtrVec      p;      // bitVec offsets
                uint        iV;     // # of bit vectors
                uint        sizeOffsets, nBytes;

                //copy offsets
                sizeOffsets =  pBatch[b].nVal * sizeof(uint16_t);
                myCopy(m_pBlock[column] + loc, m_indexBlock + pBatch[b].oVal,
                        sizeOffsets);

                // step past offsets
                loc = (uint16_t) (loc + sizeOffsets);

                // calculate the bit vector widthes
                iV = BitVecWidth(CalcWidth(pBatch[b].nVal), w);

                // this is where the bit vectors start
                pBit = m_indexBlock + pBatch[b].oVal + sizeOffsets;

                // nByte are taken by the bit vectors
                nBytes = BitVecPtr(pBatch[b].nRow, iV, w, p, pBit);

                myCopy(m_pBlock[column] + loc, pBit, nBytes);

                // step past bit vectors
                loc = (uint16_t) (loc + nBytes);
            } else if (pBatch[b].mode == LCS_VARIABLE) {
                uint        sizeOffsets;

                sizeOffsets =  pBatch[b].nRow * sizeof(uint16_t);

                // variable size record batch
                myCopy(m_pBlock[column] + loc, m_indexBlock + pBatch[b].oVal,
                        sizeOffsets);

                // step past offsets
                loc = (uint16_t) (loc + sizeOffsets);
            } else  {

                // fixed mode batch
                uint sizeFixed;

                sizeFixed =  pBatch[b].nRow * pBatch[b].recSize;
                // fixed size record batch
                myCopy(m_pBlock[column] + loc, m_indexBlock + pBatch[b].oVal,
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
        m_batchOffset[column] = dirLoc;

        // move every batch for this column
        for (i = 0; i < m_batchCount; i++) {
            PLcsBatchDir pTempBatch = (PLcsBatchDir)(m_pBlock[column] + dirLoc);
            myCopy(pTempBatch, &pBatch[b], sizeof(LcsBatchDir));

            pTempBatch->oVal = batchDirOffset[b];
            // increment to next batch and next location in temp block
            b= (uint16_t)(b+m_numColumns);
            dirLoc += sizeof(LcsBatchDir);
        }
    }

    batchDirOffset.reset();
    return nrows;
}

void LcsClusterNodeWriter::MoveFromTempToIndex()
{
    PLcsBatchDir pBatch;
    uint16_t    sz, numBatches = m_batchCount[0];
    uint16_t    offset, loc;
    uint16_t    column, b;

    // Copy values from temporary blocks for all columns starting with the
    // 1st column in cluster.
    
    for (offset = (uint16_t) m_szBlock, column = 0; column < m_numColumns;
            column++) {
        sz = (uint16_t) (m_szBlock - m_pHdr->lastVal[column]);
        myCopy(m_indexBlock +(offset-sz),
                m_pBlock[column] + m_pHdr->lastVal[column], sz);

        //  set delta value to subtract from offsets to get relative offset
        m_pHdr->delta[column] = (uint16_t)(m_szBlock - offset);

        // adjust firstVal and lastVal in the leaf block header to appropriate
        // offsets in index block (currently base on offsets in temporary block)
        m_pHdr->firstVal[column] = offset;
        offset = (uint16_t) (offset-sz);
        m_pHdr->lastVal[column] = offset;
    }

    // copy batch descriptors (which point to the batches)

    for (loc =  m_pHdrSize, b = 0; b < numBatches; b++) {
        for (column = 0; column < m_numColumns; column++) {
            uint16_t    batchStart = loc;

            pBatch = (PLcsBatchDir)(m_pBlock[column] + m_batchOffset[column]);

            if(pBatch[b].mode == LCS_COMPRESSED) {
                uint8_t     *pBit;
                WidthVec    w;      // bitVec m_width vector
                PtrVec      p;      // bitVec offsets
                uint        iV;     // # of bit vectors
                uint        sizeOffsets, nBytes;

                sizeOffsets =  pBatch[b].nVal * sizeof(uint16_t);

                // first copy offsets then bit vectors
                myCopy(m_indexBlock + loc, m_pBlock[column] + pBatch[b].oVal,
                        sizeOffsets);

                // step past offsets
                loc = (uint16_t) (loc + sizeOffsets);

                // calculate the bit vector widthes
                iV = BitVecWidth(CalcWidth(pBatch[b].nVal), w);

                // this is where the bit vectors start in temporary block
                pBit = m_pBlock[column] + pBatch[b].oVal + sizeOffsets;

                // nByte are taken by the bit vectors
                nBytes = BitVecPtr(pBatch[b].nRow, iV, w, p, pBit);

                myCopy(m_indexBlock+ loc, pBit, nBytes);

                // step past bit vectors
                loc = (uint16_t)(loc + nBytes);

            } else if (pBatch[b].mode == LCS_VARIABLE) {
                uint        sizeOffsets;

                sizeOffsets =  pBatch[b].nRow * sizeof(uint16_t);

                // variable size record batch
                myCopy(m_indexBlock + loc, m_pBlock[column] + pBatch[b].oVal,
                        sizeOffsets);

                // step past offsets
                loc = (uint16_t) (loc + sizeOffsets);
            } else {

                // Fixed mode
                uint sizeFixed;

                sizeFixed =  pBatch[b].nRow * pBatch[b].recSize;
                // fixed size record batch
                myCopy(m_indexBlock +loc, m_pBlock[column] + pBatch[b].oVal,
                        sizeFixed);

                //step past fixed records
                loc = (uint16_t) (loc + sizeFixed);
            }

            // set offset where values start in m_indexBlock
            pBatch[b].oVal = batchStart;
        }
    }

    //adjust batch count in leaf block header
    m_pHdr->nBatch = uint16_t (m_numColumns * numBatches);

    // start batch directory at end of last batch
    m_pHdr->oBatch = loc;

    // copy batch directories
    for (b = 0; b < numBatches; b++) {
        for (column = 0; column < m_numColumns; column++) {
            pBatch = (PLcsBatchDir)(m_pBlock[column] + m_batchOffset[column]);
            myCopy(m_indexBlock + loc, &pBatch[b], sizeof(LcsBatchDir));
            loc += sizeof(LcsBatchDir);
        }
    }
}

void LcsClusterNodeWriter::AllocArrays()
{
    // allocate arrays only if they have not been allocated already
    if (m_allocArrays)
        return;
    m_allocArrays = true;

    m_batch.reset(new LcsBatchDir[m_numColumns]);

    m_pValBank.reset(new PBuffer[m_numColumns]);

    // allocate larger buffers for the individual pages in the value bank

    for (uint col = 0; col < uint(m_numColumns); col++) {
        bufferLock.allocatePage();
        m_pValBank[col] = bufferLock.getPage().getWritableData();
        // Similar to what's done in external sorter, we rely on the fact
        // that the underlying ScratchSegment keeps the page pinned for us.
        // The pages will be released when all other pages associated with
        // the ScratchSegment are released.
        bufferLock.unlock();
    }

    m_pValBankStart.reset(new uint16_t[m_numColumns]);
    memset(m_pValBankStart.get(), 0, m_numColumns * sizeof(uint16_t));

    m_forceModeCount.reset(new uint[m_numColumns]);
    memset(m_forceModeCount.get(), 0, m_numColumns * sizeof(uint16_t));

    m_bForceMode.reset(new ForceMode[m_numColumns]);
    memset(m_bForceMode.get(), 0, m_numColumns * sizeof(ForceMode));

    m_oValBank.reset(new uint16_t[m_numColumns]);
    memset(m_bForceMode.get(), 0, m_numColumns * sizeof(ForceMode));
    
    m_batchOffset.reset(new uint16_t[m_numColumns]);
    memset(m_batchOffset.get(), 0, m_numColumns * sizeof(uint16_t));

    m_batchCount.reset(new int16_t[m_numColumns]);
    memset(m_batchCount.get(), 0, m_numColumns * sizeof(int16_t));

    m_nBits.reset(new uint[m_numColumns]);
    memset(m_nBits.get(), 0, m_numColumns * sizeof(uint));

    m_nextWidthChange.reset(new uint[m_numColumns]);
    memset(m_nextWidthChange.get(), 0, m_numColumns * sizeof(uint));

    m_maxValueSize.reset(new uint[m_numColumns]);
    memset(m_maxValueSize.get(), 0, m_numColumns * sizeof(uint));
}


FENNEL_END_CPPFILE("$Id$");

// End LcsClusterNodeWriter.cpp
