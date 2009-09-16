/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2006-2009 The Eigenbase Project
// Copyright (C) 2006-2009 SQLstream, Inc.
// Copyright (C) 2006-2009 LucidEra, Inc.
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

#ifndef Fennel_UnalignedAttributeAccessor_Included
#define Fennel_UnalignedAttributeAccessor_Included

FENNEL_BEGIN_NAMESPACE

class TupleDatum;
class TupleAttributeDescriptor;

/**
 * UnalignedAttributeAccessor is similar to AttributeAccessor, except
 * that it provides a by-value access model intended for storing
 * individual data values with maximum compression (hence unaligned),
 * as opposed to the tuple-valued by-reference model of AttributeAccessor.
 *
 * @note Two methods, storeValue and loadValue, store and load
 * TupleDatum to and from a preallocated buffer.  The storage format is
 * different from the marshalled format for a tuple (see TupleAccessor),
 * since there's only one TupleDatum involved and there is no need to store
 * the offset needed for "constant seek time".  The storage format depends
 * on the type of the data stored and for variable-width values is prefixed
 * with leading bytes containing the length of the data.
 *
 * <p>If the data is an 8-byte integer (other than null), the
 * leading zeroes in the data are stripped, and the length of the remaining
 * bytes is stored in the first byte, followed by the data.
 *
 * <p>If the data is fixed-width and non-nullable, only the data
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
 * Two length bytes encode value length from 128(0x0080) to 32767(0x7fff)\n
 * 1xxxxxxx xxxxxxxx\n
 * -------- -------- -------- -------- -------- ...\n
 * |      length     |     data value bytes\n
 *
 * @par where length (1 or 2 bytes) comes from TupleDatum.cbData (a 4 byte
 * type) and data value bytes are copied from TupleDatum.pData. When
 * storing NULL values, the one-byte length value of 0x00 is used; empty
 * strings are special-cased as the two-byte length value of 0x8000
 * (because NULL values are much more common than empty strings)
 *
 *<p>
 *
 * TODO jvs 22-Oct-2006:  unify this up at the TupleAccessor level
 * as a new TUPLE_FORMAT_UNALIGNED.
 */
class FENNEL_TUPLE_EXPORT UnalignedAttributeAccessor
{
    static const TupleStorageByteLength ONE_BYTE_MAX_LENGTH = 127;
    static const TupleStorageByteLength TWO_BYTE_MAX_LENGTH = 32767;
    static const uint8_t ONE_BYTE_LENGTH_MASK = 0x7f;
    static const uint16_t TWO_BYTE_LENGTH_MASK1 = 0x7f00;
    static const uint16_t TWO_BYTE_LENGTH_MASK2 = 0x00ff;
    static const uint8_t TWO_BYTE_LENGTH_BIT = 0x80;

    uint cbStorage;

    bool omitLengthIndicator;

    bool isCompressedInt64;

    /**
     * Compresses and stores an 8-byte integer by stripping off leading zeros.
     * The stored value includes a leading byte indicating the length of the
     * data.
     *
     * @param [in] datum datum to compress
     * @param [in, out] pDest pointer to the buffer where the data will be
     * stored
     */
    inline void compressInt64(
        TupleDatum const &datum,
        PBuffer pDest) const;

    /**
     * Uncompresses and loads an 8-byte integer, expanding it back to its
     * original 8-byte value
     *
     * @param [in] datum datum to receive decompression result
     * @param [in] pDataWithLen data buffer to load from
     */
    inline void uncompressInt64(
        TupleDatum &datum,
        PConstBuffer pDataWithLen) const;

    bool isInitialized() const;

public:
    explicit UnalignedAttributeAccessor();

    /**
     * Creates an accessor for the given attribute descriptor.
     *
     * @param attrDescriptor descriptor for values which will be
     * accessed
     */
    explicit UnalignedAttributeAccessor(
        TupleAttributeDescriptor const &attrDescriptor);

    /**
     * Precomputes access for a descriptor.  Must be called
     * before any other method (or invoked explicitly
     * by non-default constructor).
     *
     * @param attrDescriptor descriptor for values which will be
     * accessed
     */
    void compute(
        TupleAttributeDescriptor const &attrDescriptor);

    /**
     * Stores a value by itself, including length information, encoding it
     * into the buffer passed in.
     *
     * @par
     * The caller needs to allocate a buffer of sufficient size. To do this,
     * use the getMaxByteCount() method.
     *
     * @param [in] datum value to be stored
     * @param [in, out] pDataWithLen data buffer to store to
     */
    void storeValue(
        TupleDatum const &datum,
        PBuffer pDataWithLen) const;

    /**
     * Loads a value from a buffer containing data encoded via
     * storeValue.
     *
     * @note See note on memCopyFrom method regarding why and how to
     * preallocate the buffer.
     *
     * @param [in] datum datum to receive loaded value
     * @param [in] pDataWithLen data buffer to load from
     */
    void loadValue(
        TupleDatum &datum,
        PConstBuffer pDataWithLen) const;

    /**
     * References without copying a value from a buffer containing data encoded
     * via storeValue.  The reference is not aligned, and for some datatypes
     * such as integers, may be to a raw (compressed) representation.
     *
     * @param [in] datum datum to receive reference value
     * @param [in] pDataWithLen data buffer to reference
     */
    void referenceValue(
        TupleDatum &datum,
        PConstBuffer pDataWithLen) const;

    /**
     * Gets the length information corresponding to the data stored in a buffer.
     *
     * @param [in] pDataWithLen the data buffer to get the length from
     *
     * @return length of the value in stored format including any
     * length indicator overhead
     */
    TupleStorageByteLength getStoredByteCount(
        PConstBuffer pDataWithLen) const;

    /**
     * Get the maximum number of bytes required to store any value of the
     * given attribute.
     *
     * @return maximum storage length required for this attribute
     */
    TupleStorageByteLength getMaxByteCount() const;
};

FENNEL_END_NAMESPACE

#endif

// End UnalignedAttributeAccessor.h
