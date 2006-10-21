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

#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/tuple/TupleDescriptor.h"

#include <vector>


FENNEL_BEGIN_NAMESPACE


class TupleDescriptor;
class TupleProjection;

/**
 * A TupleDatum is a component of TupleData; see
 * <a href="structTupleDesign.html#TupleData">the design docs</a> for
 * more details.
 */
struct TupleDatum
{
    static const TupleStorageByteLength ONE_BYTE_MAX_LENGTH = 127;
    static const TupleStorageByteLength TWO_BYTE_MAX_LENGTH = 32767;
    static const uint8_t ONE_BYTE_LENGTH_MASK = 0x7f;
    static const uint16_t TWO_BYTE_LENGTH_MASK1 = 0x7f00;
    static const uint16_t TWO_BYTE_LENGTH_MASK2 = 0x00ff;
    static const uint8_t TWO_BYTE_LENGTH_BIT = 0x80;

    TupleStorageByteLength cbData;
    PConstBuffer pData;
  
    union
    {
        uint16_t data16;
        uint32_t data32;
        uint64_t data64;
    };
    
    inline explicit TupleDatum();
    inline TupleDatum(TupleDatum const &other);
    
    /*
     * Test if this TupleDatum represents NULL value.
     *
     * @return true if this TupleDatum represents NULL.
     */
    inline bool isNull() const;

    /**
     * Copy assignment(shallow copy).
     *
     * @note
     * See the note of copyFrom method.
     * 
     * @param [in] other the source TupleDatum
     */
    inline TupleDatum &operator = (TupleDatum const &other);

    /**
     * Copies data from source(shallow copy).
     * 
     * @note
     * pData is set to the source data buffer. If pData points to any buffer
     * before this call, it will no longer point to that buffer after this
     * function call.
     * 
     * @param [in] other the source TupleDatum 
     */
    inline void copyFrom(TupleDatum const &other);

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
     * @param [in] other the source TupleDatum
     */
    void memCopyFrom(TupleDatum const &other);

    /**
     * Stores data, including length information encoded into the buffer
     * passed in.
     * 
     * @note
     * Two methods, storeLcsDatum and loadLcsDatum, store and load TupleDatum
     * to and from a preallocated buffer.  The storage format is different
     * from the marshalled format for a tuple (see TupleAccessor), since
     * there's only one TupleDatum involved and there is no need to store
     * the offset needed for "constant seek time".  The storage format
     * depends on the type of the data stored and may consist of leading bytes
     * containing the length of the data.
     *
     * <p>If the data is an 8-byte integer, the leading zeroes in the data are
     * stripped, and the length of the remaining bytes is stored in the first
     * byte, followed by the data.
     *
     * <p>If the data is fixed width and non-nullable, only the data
     * itself is stored.  We do not need to store the length of the data in
     * this case because it is fixed and can be determined from the type
     * descriptor corresponding to the data.
     *
     * <p>In all other cases, a length is encoded in the leading bytes of
     * the buffer, based on the number of bytes in the data.  The byte format
     * of the buffer after storeDatum is:
     *
     * @par
     * One length byte encodes value length from 0(0x0000) to 127(0x007f)\n
     * 0xxxxxxx\n
     * -------- -------- -------- -------- -------- ...\n
     * |length  |     data value bytes\n
     *
     * @par
     * Two length bytes encode value length from 128(0x0100) to 32767(0x7fff)\n
     * 1xxxxxxx xxxxxxxx\n
     * -------- -------- -------- -------- -------- ...\n
     * |      length     |     data value bytes\n
     *
     * @par
     * where length(1 or 2 bytes) comes from TupleDatum.cbData(a 4 byte type)
     * and data value bytes are copied from TupleDatum.pData. When storing NULL
     * values, the two-byte length value of 0x8000 is used but the length
     * value(0x0000) is not valid in the two byte length range. Empty string is
     * represented by a single length byte encoding the length value 0.
     *
     * @par
     * The caller needs to allocate a buffer of sufficient size. To do this,
     * the caller must use the getMaxLcsLength() method from the associated
     * TupleAttributeDescriptor(or the TupleDescriptor for the tuple, then
     * indexing into the corresponding TupleDatum location). That method
     * returns the value of TupleAttributeDescriptor.cbStorage + 2.
     *
     * @param [in, out] pDataWithLen data buffer to store to
     * @param [in] attrDesc attribute descriptor for the datum being stored
     */
    void storeLcsDatum(
        PBuffer pDataWithLen,
        TupleAttributeDescriptor const &attrDesc);
    
    /**
     * Loads TupleDatum from a buffer that may contain encoded length
     * information.
     *
     * @note
     * See note on memCopyFrom method.
     *
     * @param [in] pDataWithLen data buffer to load from
     * @param [in] attrDesc attribute descriptor for the datum being loaded
     */
    void loadLcsDatum(
        PConstBuffer pDataWithLen,
        TupleAttributeDescriptor const &attrDesc);

    /**
     * Gets the length information corresponding to the data stored in a buffer.
     *
     * @param [in] pDataWithLen the data buffer to get the length from
     * @param [in] attrDesc attribute descriptor corresponding to the data
     * stored in the buffer
     *
     * @return length of the value in storage format including the length bytes
     */
    TupleStorageByteLength getLcsLength(
        PConstBuffer pDataWithLen,
        TupleAttributeDescriptor const &attrDesc);

    /**
     * Compresses and stores an 8-byte integer by stripping off leading zeros.
     * The stored value includes a leading byte indicating the length of the
     * data.
     *
     * @param [in, out] pDest pointer to the buffer where the data will be
     * stored
     */
    void compress8ByteInt(PBuffer pDest);

    /**
     * Uncompresses and loads an 8-byte integer, expanding it back to its
     * original 8-byte value
     *
     * @param [in] pDataWithLen data buffer to load from
     */
    void uncompress8ByteInt(PConstBuffer pDataWithLen);
};

/**
 * TupleData is an in-memory collection of independent data values, as
 * explained in <a href="structTupleDesign.html#TupleData">the design docs</a>.
 */
class TupleData : public std::vector<TupleDatum>
{
public:
    inline explicit TupleData();
    inline explicit TupleData(TupleDescriptor const &tupleDesc);

    void compute(TupleDescriptor const &);

    bool containsNull() const;

    bool containsNull(TupleProjection const &tupleProj) const;

    /** project unmarshalled data; like TupleDescriptor::projectFrom */
    void projectFrom(TupleData const& src, TupleProjection const&);
};

/****************************************************
  Definitions of inline methods for class TupleDatum
 ****************************************************/

inline TupleDatum::TupleDatum()
{
    cbData = 0;
    pData = NULL;
}

inline TupleDatum::TupleDatum(TupleDatum const &other)
{
    copyFrom(other);
}

inline bool TupleDatum::isNull() const
{
    return (!pData);
}

inline TupleDatum &TupleDatum::operator = (TupleDatum const &other)
{
    copyFrom(other);
    return *this;
}

inline void TupleDatum::copyFrom(TupleDatum const &other)
{
    cbData = other.cbData;
    pData = other.pData;
}

/***************************************************
  Definitions of inline methods for class TupleData
 ***************************************************/

inline TupleData::TupleData()
{
}

inline TupleData::TupleData(TupleDescriptor const &tupleDesc)
{
    compute(tupleDesc);
}

FENNEL_END_NAMESPACE

#endif

// End TupleData.h
