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
#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/TupleDescriptor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void TupleDatum::memCopyFrom(TupleDatum const &other)
{
    cbData = other.cbData;
    
    /*
     * Performs memcpy from "other".
     * Sets pData to NULL if it is NULL in "other".
     */
    if (other.pData) {
        memcpy(const_cast<PBuffer>(pData),
            other.pData,
            other.cbData);
    } else {
        pData = other.pData;
    }
}

void TupleDatum::storeLcsDatum(
    PBuffer pDataWithLen,
    TupleAttributeDescriptor const &attrDesc)
{
    PBuffer tmpDataPtr = pDataWithLen;
      
    /*
     * Note:
     * This storage format can only encode values shorter than 0x7fff bytes.
     */
    assert(cbData <= TWO_BYTE_MAX_LENGTH);
      
    if (!pData) {
        /*
         * Handle NULL.
         * NULL is stored as a special two byte length: 0x8000
         */
        *tmpDataPtr = TWO_BYTE_LENGTH_BIT;
        tmpDataPtr ++;
        *tmpDataPtr = 0x00;
        tmpDataPtr ++;
    } else {
        StoredTypeDescriptor::Ordinal typeOrdinal =
            attrDesc.pTypeDescriptor->getOrdinal();

        // strip off leading zeros from 8-byte ints
        if (typeOrdinal == STANDARD_TYPE_INT_64 ||
            typeOrdinal == STANDARD_TYPE_UINT_64)
        {
            compress8ByteInt(tmpDataPtr);
        } else {

            // for varying-length data and data that is nullable, store
            // a length byte in either 1 or 2 bytes, depending on the length
            if (!attrDesc.isLcsFixedWidth) {
                if (cbData <= ONE_BYTE_MAX_LENGTH) {
                    *tmpDataPtr = static_cast<uint8_t>(cbData);
                    tmpDataPtr++;
                } else {
                    uint8_t higherByte =
                        (cbData & TWO_BYTE_LENGTH_MASK1) >> 8 |
                            TWO_BYTE_LENGTH_BIT;
                    uint8_t lowerByte  = cbData & TWO_BYTE_LENGTH_MASK2;
                    *tmpDataPtr = higherByte;
                    tmpDataPtr++;
                    *tmpDataPtr = lowerByte;
                    tmpDataPtr++;
                }
            }

            // store the value
            memcpy(tmpDataPtr, pData, cbData);
        }
    }
}

void TupleDatum::compress8ByteInt(PBuffer pDest)
{
    assert(cbData == 8);
    int64_t intVal = *reinterpret_cast<int64_t const *> (pData);
    uint len;

    if (intVal >= 0) {
        FixedBuffer tmpBuf[8];
        PBuffer pTmpBuf = tmpBuf + 8;
        len = 0;
        do {
            *(--pTmpBuf) = intVal & 0xff;
            len++;
            intVal >>= 8;
        } while (intVal);

        // if the high bit is set, add an extra zero byte to distinguish this
        // value from a negative one
        if (*pTmpBuf & 0x80) {
            *(--pTmpBuf) = 0;
            len++;
        }
        *pDest = static_cast<uint8_t>(len);
        memcpy(pDest + 1, pTmpBuf, len);

    } else {
        // negative case -- calculate the number of bytes based on the value
        if (intVal >= -(0x80)) {
            len = 1;
        } else if (intVal >= -(0x8000)) {
            len = 2;
        } else if (intVal >= -(0x800000)) {
            len = 3;
        } else if (intVal >= -(0x80000000LL)) {
            len = 4;
        } else if (intVal >= -(0x8000000000LL)) {
            len = 5;
        } else if (intVal >= -(0x800000000000LL)) {
            len = 6;
        } else if (intVal >= -(0x80000000000000LL)) {
            len = 7;
        } else {
            len = 8;
        }
        *pDest = static_cast<uint8_t>(len);
        PBuffer pTmpBuf = pDest + 1 + len;
        while (len--) {
            *(--pTmpBuf) = intVal & 0xff;
            intVal >>= 8;
        }
    }
}

void TupleDatum::loadLcsDatum(
    PConstBuffer pDataWithLen,
    TupleAttributeDescriptor const &attrDesc)
{
    assert(pData);
    StoredTypeDescriptor::Ordinal typeOrdinal =
        attrDesc.pTypeDescriptor->getOrdinal();

    // fixed width, non-nullable data is stored without leading length byte(s)
    if (attrDesc.isLcsFixedWidth) {
        cbData = attrDesc.cbStorage;
        memcpy(const_cast<PBuffer>(pData), pDataWithLen, cbData);

    } else {
        // first, check if we have null data
        if (*pDataWithLen & TWO_BYTE_LENGTH_BIT) {
            cbData =
                ((*pDataWithLen & ONE_BYTE_LENGTH_MASK) << 8)
                | *(pDataWithLen + 1);
            if (cbData == 0) {
                 // 0x8000 is used to indicate NULL value.
                pData = NULL;
            } else {        
                // not null, so must have a length that requires 2 bytes to
                // store
                memcpy(const_cast<PBuffer>(pData), pDataWithLen + 2, cbData);
            }
        } else {
            // 8-byte integers are stored with leading zeros stripped off
            if (typeOrdinal == STANDARD_TYPE_INT_64 ||
                typeOrdinal == STANDARD_TYPE_UINT_64)
            {
                uncompress8ByteInt(pDataWithLen);

            // data that requires 1 byte to store the length
            } else {
                cbData = *pDataWithLen;
                memcpy(const_cast<PBuffer>(pData), pDataWithLen + 1, cbData);
            }
        }
    }
}

void TupleDatum::uncompress8ByteInt(PConstBuffer pDataWithLen)
{
    uint len = *pDataWithLen;
    assert(len != 0);
    PConstBuffer pSrcBuf = pDataWithLen + 1;
    uint signByte = *(pSrcBuf++);
    // sign extend the high order byte if it's a negative number
    int64_t intVal =
        int64_t(signByte) | ((signByte & 0x80) ? 0xffffffffffffff00LL : 0);
    while (--len > 0) {
        intVal <<= 8;
        intVal |= *(pSrcBuf++);
    }
    cbData = 8;
    memcpy(const_cast<PBuffer>(pData), &intVal, 8);
}

TupleStorageByteLength TupleDatum::getLcsLength(
    PConstBuffer pDataWithLen,
    TupleAttributeDescriptor const &attrDesc)
{
    assert(pDataWithLen);

    if (attrDesc.isLcsFixedWidth) {
        return attrDesc.cbStorage;
    }

    if (*pDataWithLen & TWO_BYTE_LENGTH_BIT) {
        return
            (((*pDataWithLen & ONE_BYTE_LENGTH_MASK) << 8)
                | *(pDataWithLen + 1))
                + 2;
    } else {
        return (*pDataWithLen + 1);
    }
}    

void TupleData::compute(TupleDescriptor const &tupleDesc)
{
    clear();
    for (uint i = 0; i < tupleDesc.size(); ++i) {
        TupleDatum datum;
        datum.cbData = tupleDesc[i].cbStorage;
        push_back(datum);
    }
}

bool TupleData::containsNull() const
{
    for (uint i = 0; i < size(); ++i) {
        if (!(*this)[i].pData) {
            return true;
        }
    }
    return false;
}

bool TupleData::containsNull(TupleProjection const & tupleProj) const
{
    for (uint i = 0; i < tupleProj.size(); ++i) {
        if (!(*this)[tupleProj[i]].pData) {
            return true;
        }
    }
    return false;
}

void TupleData::projectFrom(
    TupleData const& src,
    TupleProjection const& projection)
{
    clear();
    for (uint i = 0; i < projection.size(); ++i) {
        push_back(src[projection[i]]);
    }
}

FENNEL_END_CPPFILE("$Id$");

// End TupleData.cpp
