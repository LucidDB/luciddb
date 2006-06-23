/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
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
#include "fennel/lucidera/colstore/LcsClusterReader.h"
#include "fennel/lucidera/colstore/LcsColumnReader.h"
#include "fennel/lucidera/colstore/LcsClusterReader.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void LcsColumnReader::sync()
{
    // Get batch using column's offset within cluster
    pBatch = &pScan->pRangeBatches[colOrd];
    pValues = pScan->pLeaf + pBatch->oVal;
    pBase = pScan->pLeaf - pScan->delta[colOrd];
    
    if (batchIsCompressed()) {
        // where the bit vectors start
        const PBuffer pBit = pValues + (sizeof(uint16_t) * pBatch->nVal);
        // # bits per value
        uint nBits = calcWidth(pBatch->nVal);
        // calculate bit vector widths
        iV = bitVecWidth(nBits, width);
        bitVecPtr(pBatch->nRow, iV, width, origin, (PBuffer) pBit);
    
        uint totWidth;
        if (iV == 1)
            totWidth = width[0];
        else if (iV == 2)
            totWidth = width[0] + width[1];
        else 
            totWidth = 0;

        // hack to do one switch statement based on both width arguments
        // The switch value is unique for any of the following combos of
        // width 1 and 2:
        //      (8,-), (8,4), (8,2), (8,1), (4,-), (4,2), (4,1), (2,-), (2,1),
        //      (1,-)
        //

        switch (totWidth)
        {
        // single vector

        case 16:    // width 1 = 16
            pFuncReadBitVec = readBitVec16;
            break;

        case 8: // width 1 = 8
            pFuncReadBitVec = readBitVec8;
            break;

        case 4: // width 1 = 4
            pFuncReadBitVec = readBitVec4;
            break;

        case 2: // width 1 = 2
            pFuncReadBitVec = readBitVec2;
            break;

        case 1: // width 1 = 1
            pFuncReadBitVec = readBitVec1;
            break;

        // dual vector, first vector 8

        case 12:    // width 1 = 8, width 2 = 4
            pFuncReadBitVec = readBitVec12;
            break;

        case 10:    // width 1 = 8, width 2 = 2
            pFuncReadBitVec = readBitVec10;
            break;

        case 9: // width 1 = 8, width 2 = 1
            pFuncReadBitVec = readBitVec9;
            break;

        // dual vector, first vector 4

        case 6: // width 1 = 4, width 2 = 2
            pFuncReadBitVec = readBitVec6;
            break;

        case 5: // width 1 = 4, width 2 = 1
            pFuncReadBitVec = readBitVec5;
            break;

        // dual vector, first vector is 2

        case 3: // width 1 = 2, width 2 = 1
            pFuncReadBitVec = readBitVec3;
            break;

        // no bit vector stored
        case 0:
            pFuncReadBitVec = readBitVec0;
            break;

        default:
            assert(false);
            break;
        }

        // Set function pointer to get data
        pGetCurrentValueFunc = &LcsColumnReader::getCompressedValue;

    } else if (batchIsFixed()) {
        // Set function pointer to get data in fixed case
        pGetCurrentValueFunc = &LcsColumnReader::getFixedValue;
    } else {
        // Set function pointer to get data in variable case
        pGetCurrentValueFunc = &LcsColumnReader::getVariableValue;
    }
}

const PBuffer LcsColumnReader::getCompressedValue()
{
    return getBatchValue(getCurrentValueCode());
}

const PBuffer LcsColumnReader::getFixedValue()
{
    return (const PBuffer)(pValues + (pScan->getRangePos() * pBatch->recSize));
}

const PBuffer LcsColumnReader::getVariableValue()
{
    return (const PBuffer) (getBatchBase() +
        getBatchOffsets()[pScan->getRangePos()]);
}

void LcsColumnReader::readCompressedBatch(
    uint count, uint16_t *pValCodes, uint *pActCount)
{
    *pActCount = std::min(count, pScan->getRangeRowsLeft());
    readBitVecs(pValCodes, iV, width, origin, pScan->getRangePos(),
                *pActCount);
}

uint16_t LcsColumnReader::getCurrentValueCode() const
{
    assert(batchIsCompressed());
    uint16_t nValCode;
    pFuncReadBitVec(&nValCode, origin, pScan->getRangePos());
    return nValCode;
}

FENNEL_END_CPPFILE("$Id$");

// End LcsColumnReader.cpp
