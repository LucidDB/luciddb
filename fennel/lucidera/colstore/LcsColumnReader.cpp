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
#include "fennel/lucidera/colstore/LcsColumnReader.h"
#include "fennel/lucidera/colstore/LcsClusterReader.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void LcsColumnReader::sync()
{
    // Get batch using column's offset within cluster
    pBatch = &pScan->pRangeBatches[colOrd];
    pValues = pScan->pLeaf + pBatch->oVal;
    pBase = pScan->pLeaf - pScan->pLHdr->delta[colOrd];
    
    if (batchIsCompressed()) {
        // where the bit vectors start
        const PBuffer pBit = pValues + (sizeof(uint16_t) * pBatch->nVal);
        // # bits per value
        uint nBits = CalcWidth(pBatch->nVal);
        // calculate bit vector widths
        iV = BitVecWidth(nBits, width);
        BitVecPtr(pBatch->nRow, iV, width, origin, (PBuffer) pBit);
    
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
            pFuncReadBitVec = ReadBitVec16;
            break;

        case 8: // width 1 = 8
            pFuncReadBitVec = ReadBitVec8;
            break;

        case 4: // width 1 = 4
            pFuncReadBitVec = ReadBitVec4;
            break;

        case 2: // width 1 = 2
            pFuncReadBitVec = ReadBitVec2;
            break;

        case 1: // width 1 = 1
            pFuncReadBitVec = ReadBitVec1;
            break;

        // dual vector, first vector 8

        case 12:    // width 1 = 8, width 2 = 4
            pFuncReadBitVec = ReadBitVec12;
            break;

        case 10:    // width 1 = 8, width 2 = 2
            pFuncReadBitVec = ReadBitVec10;
            break;

        case 9: // width 1 = 8, width 2 = 1
            pFuncReadBitVec = ReadBitVec9;
            break;

        // dual vector, first vector 4

        case 6: // width 1 = 4, width 2 = 2
            pFuncReadBitVec = ReadBitVec6;
            break;

        case 5: // width 1 = 4, width 2 = 1
            pFuncReadBitVec = ReadBitVec5;
            break;

        // dual vector, first vector is 2

        case 3: // width 1 = 2, width 2 = 1
            pFuncReadBitVec = ReadBitVec3;
            break;

        // no bit vector stored
        case 0:
            pFuncReadBitVec = ReadBitVec0;
            break;

        default:
            assert(false);
            break;
        }

        // Set function pointer to get data
        pGetCurrentValueFunc = getCompressedValue;

    } else if (batchIsFixed()) {
        // Set function pointer to get data in fixed case
        pGetCurrentValueFunc = getFixedValue;
    } else {
        // Set function pointer to get data in variable case
        pGetCurrentValueFunc = getVariableValue;
    }
}

const PBuffer LcsColumnReader::getCompressedValue(const LcsColumnReader *me)
{
    return me->getBatchValue(me->getCurrentValueCode());
}

const PBuffer LcsColumnReader::getFixedValue(const LcsColumnReader *me)
{
    return (const PBuffer)(me->pValues + (me->pScan->getRangePos()
                                            * me->pBatch->recSize));
}

const PBuffer LcsColumnReader::getVariableValue(const LcsColumnReader *me)
{
    return (const PBuffer) (me->getBatchBase()
        + me->getBatchOffsets()[me->pScan->getRangePos()]);
}

void LcsColumnReader::readCompressedBatch(uint count, uint16_t *pValCodes,
                                          uint *pActCount)
{
    *pActCount = std::min(count, pScan->getRangeRowsLeft());
    ReadBitVecs(pValCodes, iV, width, origin, pScan->getRangePos(),
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
