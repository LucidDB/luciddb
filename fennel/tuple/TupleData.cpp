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

// TODO jvs 27-Nov-2005:
// We'll probably need to implement leading/trailing zero compression for
// numerics to match Broadbase.

void TupleDatum::storeLcsDatum(PBuffer pDataWithLen)
{
    PBuffer tmpDataPtr = pDataWithLen;
      
    /*
     * Note:
     * This storage format can only encode values shorter than 0x7fff bytes.
     */
    assert(cbData <= TWO_BYTE_MAX_LENGTH);
      
    /*
     * Stores length.
     */
    
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
        if (cbData <= ONE_BYTE_MAX_LENGTH) {
            *tmpDataPtr = static_cast<uint8_t>(cbData);
            tmpDataPtr++;
        } else {
            uint8_t higherByte = (cbData & TWO_BYTE_LENGTH_MASK1) >> 8 | TWO_BYTE_LENGTH_BIT;
            uint8_t lowerByte  = cbData & TWO_BYTE_LENGTH_MASK2;
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
}

void TupleDatum::loadLcsDatum(PConstBuffer pDataWithLen)
{
    assert (pData);

    /*
      if length is longer than 127, length indicator is two bytes long
    */
    if (*pDataWithLen & TWO_BYTE_LENGTH_BIT) {
        cbData =
            ((*pDataWithLen & ONE_BYTE_LENGTH_MASK) << 8)
            | *(pDataWithLen + 1);
        if (cbData == 0) {
            /*
             * 0x8000 is used to indicate NULL value.
             */
            pData = NULL;
        } else {        
            memcpy(const_cast<PBuffer>(pData), pDataWithLen + 2, cbData);
        }
    } else {
        cbData = *pDataWithLen;
        memcpy(const_cast<PBuffer>(pData), pDataWithLen + 1, cbData);
    }
}

TupleStorageByteLength TupleDatum::getLcsLength(PConstBuffer pDataWithLen)
{
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
        //
        // NOTE rchen 2005-11-29: This function returns the storage length
        // required to store the value in pData, or the pointer passed in.
        //
        if (cbData <= ONE_BYTE_MAX_LENGTH) {
            return cbData + 1;
        } else {
            return cbData + 2;
        }
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
