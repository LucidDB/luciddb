/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2003-2005 Disruptive Tech
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

#ifndef Fennel_TupleData_Included
#define Fennel_TupleData_Included

#include <vector>


FENNEL_BEGIN_NAMESPACE


class TupleDescriptor;
class TupleProjection;

const TupleStorageByteLength ONE_BYTE_MAX_LENGTH = 127;
const TupleStorageByteLength TWO_BYTE_MAX_LENGTH = 32767;
const FixedBuffer ONE_BYTE_LENGTH_MASK = 0x7f;
const FixedBuffer TWO_BYTE_LENGTH_BIT = 0x80;

/**
 * A TupleDatum is a component of TupleData; see
 * <a href="structTupleDesign.html#TupleData">the design docs</a> for
 * more details.
 */
struct TupleDatum
{
    TupleStorageByteLength cbData;
    PConstBuffer pData;
  
    union
    {
        uint16_t data16;
        uint32_t data32;
        uint64_t data64;
    };
    
    explicit TupleDatum();
    TupleDatum(TupleDatum const &other);
    explicit TupleDatum(PConstBuffer pDataWithLen);

    /**
     * Copy assignment(shallow copy).
     *
     * @note
     * See the note of copyFrom method.
     * 
     * @param[in] other the source TupleDatum
     */
    TupleDatum &operator = (TupleDatum const &other);
    
    /**
     * Copies data from source(shallow copy).
     * 
     * @note
     * pData is set to the source data buffer. If pData points to any buffer
     * before this call, it will no longer point to that buffer after this
     * function call.
     * 
     * @param[in] other the source TupleDatum 
     */
    void copyFrom(TupleDatum const &other);

    /**
     * Copies data into TupleDatum's private buffer(deep copy).
     * 
     * @note
     * pData must point to allocated memory before calling this function. If
     * this TupleDatum is part of a TupleDataWithBuffer class,  pData will
     * point to valid memory after computeAndAllocate if no memory has been
     * allocated yet, or resetBuffer if computeAndAllocate had previously been
     * called. If this TupleDatum is not part of a TupleDataWithBuffer class,
     * the caller needs to allocate buffer and set up the pData pointer
     * manually.
     *
     * @par
     * Upon return, pData might no longer point to allocated memory if the
     * source has a null data pointer.
     * 
     * @param[in] other the source TupleDatum
     */
    void memCopyFrom(TupleDatum const &other);

    /**
     * Stores data with length information encoded into the buffer passed in.
     *
     * @note
     * These methods - storeDatum, loadDatum and loadDatumWithBuffer - store and
     * load TupleDatum to and from a preallocated buffer. The length of this
     * buffer is set to the number of bytes to store the length plus the length
     * of the data field(bound by the associated TupleDescriptor cbStorage
     * value). The storage format is different from the marshalled format of a
     * tuple (see TupleAccessor.h) since there's only one TupleDatum here, so
     * there is no need for storing the offset to provide "constant seek time"
     * of any column. The byte format of the result buffer from the storeDatum
     * function call is:
     *
     * @par
     * 0xxxxxxx\n
     * -------- -------- -------- -------- -------- ...\n
     * |length  |     data value bytes\n
     *
     * @par
     * 1xxxxxxx xxxxxxxx\n
     * -------- -------- -------- -------- -------- ...\n
     * |      length     |     data value bytes\n
     *
     * @par
     * where length(1 or 2 bytes) comes from TupleDatum.cbData(a 4 byte type)
     * and data value bytes are copied from TupleDatum.pData. The buffer to
     * allocate should be at least (cbStorage + 2) bytes long.
     *
     * @param[in, out] pDataWithLen data buffer to store to
     */
    void storeDatum(PBuffer pDataWithLen);

    /**
     * Loads TupleDatum from a buffer with length information encoded. This
     * function perform shallow copy.
     *
     * @note
     * See the note of copyFrom method.
     *
     * @param[in] pDataWithLen data buffer to load from
     */
    void loadDatum(PConstBuffer pDataWithLen);

    /**
     * Loads TupleDatum from a buffer with length information encoded.
     *
     * @note
     * See the note of memCopyFrom method.
     *
     * @param[in] pDataWithLen data buffer to load from
     */
    void loadDatumWithBuffer(PConstBuffer pDataWithLen);

    /**
     * Gets the length information from a stored data buffer.
     *
     * @param[in] pDataWithLen the data buffer to get the length from
     *
     * @return length of the data portion in the buffer
     */
    TupleStorageByteLength getStorageLength(PConstBuffer pDataWithLen = NULL);
};

/**
 * TupleData is an in-memory collection of independent data values, as
 * explained in <a href="structTupleDesign.html#TupleData">the design docs</a>.
 */
class TupleData : public std::vector<TupleDatum>
{
public:
    explicit TupleData();
    explicit TupleData(TupleDescriptor const &tupleDesc);

    void compute(TupleDescriptor const &);

    bool containsNull() const;

    /** project unmarshalled data; like TupleDescriptor::projectFrom */
    void projectFrom(TupleData const& src, TupleProjection const&);
};

FENNEL_END_NAMESPACE

#endif

// End TupleData.h
