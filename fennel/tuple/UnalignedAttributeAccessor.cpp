/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2006-2007 The Eigenbase Project
// Copyright (C) 2006-2007 SQLstream, Inc.
// Copyright (C) 2006-2007 LucidEra, Inc.
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
#include "fennel/tuple/UnalignedAttributeAccessor.h"
#include "fennel/tuple/TupleData.h"

FENNEL_BEGIN_CPPFILE("$Id$");

UnalignedAttributeAccessor::UnalignedAttributeAccessor()
{
    cbStorage = MAXU;
}

UnalignedAttributeAccessor::UnalignedAttributeAccessor(
    TupleAttributeDescriptor const &attrDescriptor)
{
    compute(attrDescriptor);
}

void UnalignedAttributeAccessor::compute(
    TupleAttributeDescriptor const &attrDescriptor)
{
    cbStorage = attrDescriptor.cbStorage;
    StoredTypeDescriptor::Ordinal typeOrdinal =
        attrDescriptor.pTypeDescriptor->getOrdinal();
    isCompressedInt64 =
        (typeOrdinal == STANDARD_TYPE_INT_64) ||
        (typeOrdinal == STANDARD_TYPE_UINT_64);
    omitLengthIndicator =
        !attrDescriptor.isNullable
        && !isCompressedInt64
        && (typeOrdinal != STANDARD_TYPE_VARCHAR)
        && (typeOrdinal != STANDARD_TYPE_VARBINARY)
        && (typeOrdinal != STANDARD_TYPE_UNICODE_VARCHAR);
}

bool UnalignedAttributeAccessor::isInitialized() const
{
    return !isMAXU(cbStorage);
}

inline void UnalignedAttributeAccessor::compressInt64(
    TupleDatum const &datum,
    PBuffer pDest) const
{
    // NOTE jvs 22-Oct-2006:  Although it may not be obvious,
    // this correctly handles both STANDARD_TYPE_INT_64
    // and STANDARD_TYPE_UINT_64 (very large unsigned values
    // are handled as if they were negative here, but
    // the consumer of the TupleDatum won't be aware of that,
    // and the sign-extension in uncompress will be a no-op).

    assert(datum.cbData == 8);
    int64_t intVal = *reinterpret_cast<int64_t const *> (datum.pData);
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

inline void UnalignedAttributeAccessor::uncompressInt64(
    TupleDatum &datum,
    PConstBuffer pDataWithLen) const
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
    datum.cbData = 8;

    // REVIEW jvs 25-Oct-2006:  Do we really need memcpy here?  I
    // think datum.pDatum is guaranteed to be aligned.
    memcpy(const_cast<PBuffer>(datum.pData), &intVal, 8);
}

void UnalignedAttributeAccessor::storeValue(
    TupleDatum const &datum,
    PBuffer pDataWithLen) const
{
    assert(isInitialized());

    PBuffer tmpDataPtr = pDataWithLen;

    if (!datum.pData) {
        /*
         * NULL is stored as a special one-byte length: 0x00
         */
        *tmpDataPtr = 0x00;
    } else {
        /*
         * Note:
         * This storage format can only encode values shorter than 0x7fff bytes.
         */
        assert(datum.cbData <= TWO_BYTE_MAX_LENGTH);

        if (isCompressedInt64) {
            // strip off leading zeros from 8-byte ints
            compressInt64(datum, tmpDataPtr);
        } else {
            // for varying-length data and data that is nullable, store
            // a length byte in either 1 or 2 bytes, depending on the length
            if (!omitLengthIndicator) {
                if (datum.cbData && (datum.cbData <= ONE_BYTE_MAX_LENGTH)) {
                    *tmpDataPtr = static_cast<uint8_t>(datum.cbData);
                    tmpDataPtr++;
                } else {
                    uint8_t higherByte =
                        (datum.cbData & TWO_BYTE_LENGTH_MASK1) >> 8 |
                            TWO_BYTE_LENGTH_BIT;
                    uint8_t lowerByte  = datum.cbData & TWO_BYTE_LENGTH_MASK2;
                    *tmpDataPtr = higherByte;
                    tmpDataPtr++;
                    *tmpDataPtr = lowerByte;
                    tmpDataPtr++;
                }
            }

            // store the value
            memcpy(tmpDataPtr, datum.pData, datum.cbData);
        }
    }
}

void UnalignedAttributeAccessor::loadValue(
    TupleDatum &datum,
    PConstBuffer pDataWithLen) const
{
    assert(isInitialized());
    assert(datum.pData);

    // fixed width, non-nullable data is stored without leading length byte(s)
    if (omitLengthIndicator) {
        datum.cbData = cbStorage;
        memcpy(const_cast<PBuffer>(datum.pData), pDataWithLen, datum.cbData);
    } else {
        uint8_t firstByte = *pDataWithLen;
        if (!firstByte) {
            // null value
            datum.pData = NULL;
        } else if (firstByte & TWO_BYTE_LENGTH_BIT) {
            // not null, so must have a length that requires 2 bytes to
            // store
            datum.cbData =
                ((firstByte & ONE_BYTE_LENGTH_MASK) << 8)
                | *(pDataWithLen + 1);
            memcpy(
                const_cast<PBuffer>(datum.pData),
                pDataWithLen + 2,
                datum.cbData);
        } else {
            if (isCompressedInt64) {
                // 8-byte integers are stored with leading zeros stripped off
                uncompressInt64(datum, pDataWithLen);
            } else {
                // data that requires 1 byte to store the length
                datum.cbData = firstByte;
                memcpy(
                    const_cast<PBuffer>(datum.pData),
                    pDataWithLen + 1,
                    datum.cbData);
            }
        }
    }
}

TupleStorageByteLength UnalignedAttributeAccessor::getStoredByteCount(
    PConstBuffer pDataWithLen) const
{
    assert(isInitialized());
    assert(pDataWithLen);

    if (omitLengthIndicator) {
        return cbStorage;
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

TupleStorageByteLength UnalignedAttributeAccessor::getMaxByteCount() const
{
    assert(isInitialized());

    if (omitLengthIndicator) {
        return cbStorage;
    } else {
        return cbStorage + 2;
    }
}

FENNEL_END_CPPFILE("$Id$");

// End UnalignedAttributeAccessor.cpp
