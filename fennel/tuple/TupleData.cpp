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

// TODO jvs 27-Nov-2005:  most of these methods are good candidates
// for inlining

TupleDatum::TupleDatum()
{
    cbData = 0;
    pData = NULL;
}

TupleDatum::TupleDatum(TupleDatum const &other)
{
    copyFrom(other);
}

TupleDatum::TupleDatum(PConstBuffer pDataWithLen)
{
    loadDatum(pDataWithLen);
}

TupleDatum &TupleDatum::operator = (TupleDatum const &other)
{
    copyFrom(other);
    return *this;
}

void TupleDatum::copyFrom(TupleDatum const &other)
{
    cbData = other.cbData;
    pData = other.pData;
}

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

// TODO jvs 27-Nov-2005: We need a name for the storage format used by
// storeDatum/loadDatum/etc below to distinguish it from the TupleAccessor
// marshalling format.  Perhaps
// storeCompressed/loadCompressed/getCompressedLength?  Also, we'll probably
// need to implement leading/trailing zero compression for numerics to match
// Broadbase.

void TupleDatum::storeDatum(PBuffer pDataWithLen)
{
    // REVIEW jvs 27-Nov-2005: This method doesn't handle NULL values.
    // Perhaps it should, but if it isn't supposed to, the method
    // comments should specify that, and we should assert(pData) here.
    
    PBuffer tmpDataPtr = pDataWithLen;
      
    /*
     * Note:
     * This storage format can only encode values shorter than 0x7f00 bytes.
     */
    assert(cbData <= TWO_BYTE_MAX_LENGTH);
      
    /*
     * Stores length.
     */
    if (cbData <= ONE_BYTE_MAX_LENGTH) {
        *tmpDataPtr = static_cast<uint8_t>(cbData);
        tmpDataPtr++;
    } else {
        // REVIEW jvs 27-Nov-2005:  Isn't this supposed to set
        // the TWO_BYTE_LENGTH_BIT also?
        uint8_t higherByte = (cbData & 0x00007f00) >> 8;
        uint8_t lowerByte  = cbData & 0x000000ff;
        *tmpDataPtr = higherByte;
        tmpDataPtr++;
        *tmpDataPtr = lowerByte;
        tmpDataPtr++;
    }
      
    /*
     * Stores value.
     */
    memcpy(tmpDataPtr, pData, cbData);
}

void TupleDatum::loadDatum(PConstBuffer pDataWithLen)
{
    // REVIEW jvs 27-Nov-2005: This method could cause alignment problems.
    // storeDatum used memcpy, so it may have stored the datum at
    // an unaligned address without problem; but here we will set pData
    // to that unaligned address, in which case the caller will
    // choke if it attempts to dereference it without a copy.
    // Perhaps we should only have the loadDatumWithBuffer version?
    
    /*
     * If length is longer than 127, use two bytes to store length.
     */
    if (*pDataWithLen & TWO_BYTE_LENGTH_BIT) {
        cbData = ((*pDataWithLen & ONE_BYTE_LENGTH_MASK) << 8)
            | *(pDataWithLen + 1);
        pData = pDataWithLen + 2;
    } else {
        cbData = *pDataWithLen;
        pData = pDataWithLen + 1;
    }
}

void TupleDatum::loadDatumWithBuffer(PConstBuffer pDataWithLen)
{
    assert (pData);

    /*
      if length is longer than 127, length indicator is two bytes long
    */
    if (*pDataWithLen & TWO_BYTE_LENGTH_BIT) {
        cbData =
            ((*pDataWithLen & ONE_BYTE_LENGTH_MASK) << 8)
            | *(pDataWithLen + 1);
        memcpy(const_cast<PBuffer>(pData), pDataWithLen + 2, cbData);
    } else {
        cbData = *pDataWithLen;
        memcpy(const_cast<PBuffer>(pData), pDataWithLen + 1, cbData);
    }
}

TupleStorageByteLength TupleDatum::getStorageLength(PConstBuffer pDataWithLen)
{
    // REVIEW jvs 27-Nov-2005: the method comments say this returns
    // the length of the data portion, but the sum below includes
    // the length indicator itself, right?
    
    if (pDataWithLen) {
        if (*pDataWithLen & TWO_BYTE_LENGTH_BIT) {
            return
                (((*pDataWithLen & ONE_BYTE_LENGTH_MASK) << 8)
                    | *(pDataWithLen + 1)
                    + 2);
        } else {
            return (*pDataWithLen + 1);
        }
    } else {
        // REVIEW jvs 27-Nov-2005: What's the purpose of accepting
        // a NULL pointer here?  If this is intended as a way to compute
        // the required buffer size before allocation, it should be
        // documented.  But it relies on cbData, which won't work for
        // types like VARCHAR (only the TupleDescriptor is guaranteed to
        // have the required info).
        if (cbData <= ONE_BYTE_MAX_LENGTH) {
            return cbData + 1;
        } else {
            return cbData + 2;
        }
    }
}    

TupleData::TupleData()
{
}

TupleData::TupleData(TupleDescriptor const &tupleDesc)
{
    compute(tupleDesc);
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
