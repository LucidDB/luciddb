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

void TupleDatum::storeDatum(PBuffer pDataWithLen)
{
    PBuffer tmpDataPtr = pDataWithLen;
      
    /*
     * Note:
     * This storage format can only encode values shorter than 0x7f00 bytes.
     */
    assert(cbData <= TWO_BYTE_MAX_LENGTH);
      
    FixedBuffer higherByte = (cbData & 0x00007f00) >> 8;
    FixedBuffer lowerByte  = cbData & 0x000000ff;
        
    /*
     * Stores length.
     */
    if (cbData <= ONE_BYTE_MAX_LENGTH) {
        *tmpDataPtr = (FixedBuffer)cbData;
        tmpDataPtr ++;
    }
    else {
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
    /*
     * If length is longer than 127, use two bytes to store length.
     */
    if (*pDataWithLen & TWO_BYTE_LENGTH_BIT) {
        cbData = ((*pDataWithLen & ONE_BYTE_LENGTH_MASK) << 8)
            | *(pDataWithLen + 1);
        pData = pDataWithLen + 2;
    }
    else {
        cbData = *pDataWithLen;
        pData = pDataWithLen + 1;
    }
}

void TupleDatum::loadDatumWithBuffer(PConstBuffer pDataWithLen)
{
    assert (pData);

    /*
     * If length is longer than 127, length comes from two bytes.
     */
    if (*pDataWithLen & TWO_BYTE_LENGTH_BIT) {
        cbData =
            ((*pDataWithLen & ONE_BYTE_LENGTH_MASK) << 8)
            | *(pDataWithLen + 1);
        memcpy(const_cast<PBuffer>(pData), pDataWithLen + 2, cbData);
    }
    else {
        cbData = *pDataWithLen;
        memcpy(const_cast<PBuffer>(pData), pDataWithLen + 1, cbData);
    }
}

TupleStorageByteLength TupleDatum::getStorageLength(PConstBuffer pDataWithLen)
{
    if (pDataWithLen) {
        if (*pDataWithLen & TWO_BYTE_LENGTH_BIT)
            return
                (((*pDataWithLen & ONE_BYTE_LENGTH_MASK) << 8)
                    | *(pDataWithLen + 1)
                    + 2);
        else
            return (*pDataWithLen + 1);
    } else {
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
