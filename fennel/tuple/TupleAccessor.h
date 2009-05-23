/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 1999-2009 John V. Sichi
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

#ifndef Fennel_TupleAccessor_Included
#define Fennel_TupleAccessor_Included

#include "fennel/tuple/TupleFormat.h"

#include <boost/dynamic_bitset.hpp>
#include <boost/utility.hpp>
#include <vector>

FENNEL_BEGIN_NAMESPACE

class TupleData;
class TupleDescriptor;
class AttributeAccessor;

// NOTE: see comments on struct StoredNode before modifying the way tuples are
// stored.

/**
 * TupleAccessor defines how to efficiently marshal and unmarshal values in a
 * stored tuple.  The same logical tuple definition can have multiple storage
 * formats.  See <a href="structTupleDesign.html#TupleAccessor">the design
 * docs</a> for more details.
 */
class FENNEL_TUPLE_EXPORT TupleAccessor
    : public boost::noncopyable
{
    /**
     * Precomputed accessors for attributes, in logical tuple order.
     */
    std::vector<AttributeAccessor *> ppAttributeAccessors;

    /**
     * Array of 0-based indices of variable-width attributes.
     */
    VectorOfUint pVarWidthAttrIndices;

    /**
     * Permutation in which attributes should be marshalled; empty when
     * !bAlignedVar, in which case attributes should be marshalled in logical
     * order.
     */
    VectorOfUint marshalOrder;

    /**
     * @see getMaxByteCount()
     */
    uint cbMaxStorage;

    /**
     * @see getMinByteCount()
     */
    uint cbMinStorage;

    /**
     * Precomputed size of bit field array (in bits).
     */
    uint nBitFields;

    /**
     * Precomputed byte offset for bit array.
     */
    uint iBitFieldOffset;

    /**
     * Precomputed offset for indirect offset of end of first variable-width
     * attribute, or MAXU if there are no variable-width attributes.
     */
    uint iFirstVarEndIndirectOffset;

    /**
     * Precomputed offset for indirect offset of end of last variable-width
     * attribute, or MAXU if there are no variable-length attributes.
     */
    uint iLastVarEndIndirectOffset;

    /**
     * Precomputed offset for fixed start of first variable-width
     * attribute, or MAXU if there are no variable-width attributes.
     */
    uint iFirstVarOffset;

    /**
     * Whether any variable-width attributes with alignment requirements
     * (currently restricted to 2-byte alignment for UNICODE strings) are
     * present.
     */
    bool bAlignedVar;

    /**
     * @see getCurrentTupleBuf()
     */
    PConstBuffer pTupleBuf;

    /**
     * @see getBitFields()
     */
    boost::dynamic_bitset<FixedBuffer> bitFields;

    TupleFormat format;

    // private helpers
    void initFixedAccessors(TupleDescriptor const &,VectorOfUint &);
    void clear();

public:
    typedef uint16_t StoredValueOffset;

    /**
     * Constant value for true.  This is used as a singleton unmarshalling
     * address for true boolean values (since we can't reference individual
     * bits).
     */
    static const bool BOOL_TRUE;

    /**
     * Constant value for false.  This is used as a singleton unmarshalling
     * address for false boolean values (since we can't reference individual
     * bits).
     */
    static const bool BOOL_FALSE;

    explicit TupleAccessor();

    virtual ~TupleAccessor();

    /**
     * Precomputes access for a particular tuple format.  Must be called
     * before any other method.
     *
     * @param tuple the tuple to be accessed
     *
     * @param format how to store tuple
     */
    void compute(
        TupleDescriptor const &tuple,
        TupleFormat format = TUPLE_FORMAT_STANDARD);

    /**
     * @return the maximum possible tuple storage size in bytes
     */
    uint getMaxByteCount() const
    {
        return cbMaxStorage;
    }

    /**
     * @return the minimum possible tuple storage size in bytes
     */
    uint getMinByteCount() const
    {
        return cbMinStorage;
    }

    /**
     * @return whether all tuples have the same fixed size
     */
    bool isFixedWidth() const
    {
        return isMAXU(iFirstVarOffset);
    }

    /**
     * @return the offset of the first byte of bit fields, or MAXU if no bit
     * fields
     */
    uint getBitFieldOffset() const
    {
        return iBitFieldOffset;
    }

    /**
     * Accesses the buffer storing the current tuple image.
     *
     * @return address of tuple image, or NULL if no current tuple
     */
    PConstBuffer getCurrentTupleBuf() const
    {
        return pTupleBuf;
    }

    /**
     * Sets the buffer storing the current tuple image.  Must be called
     * before getCurrentByteCount and unmarshal.
     * @param pTupleBuf address of tuple image
     * @param valid (default: true) the buffer contains a marshalled tuple.
     *  False means the buffer is free space; unmarshal binds a TupleData to the
     *  buffer. This is useful only for TUPLE_FORMAT_ALL_FIXED.
     *
     * REVIEW: An alternative is to require the caller to zero out the buffer;
     * but that seems riskier.
     */
    void setCurrentTupleBuf(PConstBuffer pTupleBuf, bool valid = true);

    /**
     * Forgets the current tuple buffer.
     */
    void resetCurrentTupleBuf();

    /**
     * Determines the number of bytes stored in the current tuple buffer.  This
     * will always be greater than or equal to getMinByteCount() and less than
     * getMaxByteCount().
     *
     * @return byte count
     */
    inline uint getCurrentByteCount() const;

    /**
     * Determines the number of bytes stored in a tuple buffer without
     * actually preparing to unmarshal it.
     *
     * @param pBuf tuple buffer
     *
     * @return byte count
     */
    uint getBufferByteCount(PConstBuffer pBuf) const;

    /**
     * Determines the number of bytes required to store a tuple without actually
     * marshalling it.
     *
     * @param tuple the tuple data
     *
     * @return byte count
     */
    uint getByteCount(TupleData const &tuple) const;

    /**
     * Determines whether a buffer is big enough to fit marshalled tuple data.
     *
     * @param tuple the tuple to be marshalled
     *
     * @param cbBuffer the size of the candidate buffer
     *
     * @return true if cbBuffer is big enough
     */
    bool isBufferSufficient(TupleData const &tuple,uint cbBuffer) const;

    // TODO:  come up with a common interface for TupleProjectionAccessor, and
    // add an additional virtual interface
    /**
     * Unmarshals the current tuple buffer, setting a tuple's values
     * to reference the contents.
     *
     * @param tuple the tuple which will be modified to reference the
     * unmarshalled values
     *
     * @param iFirstDatum 0-based index of TupleDatum at which to start
     * writing to tuple (defaults to first TupleDatum); note that unmarshalling
     * always starts with the first attribute
     */
    void unmarshal(TupleData &tuple,uint iFirstDatum = 0) const;

    /**
     * Gets an accessor for an individual attribute.  This can be used to
     * unmarshal values individually.
     *
     * @param iAttribute 0-based index of the attribute within the tuple
     */
    AttributeAccessor const &getAttributeAccessor(uint iAttribute) const
    {
        return *(ppAttributeAccessors[iAttribute]);
    }

    /**
     * Marshals a tuple's values into a buffer.
     *
     * @param tuple the tuple to be marshalled
     *
     * @param pTupleBuf the buffer into which to marshal,
     * which also becomes the current tuple buffer
     */
    void marshal(TupleData const &tuple,PBuffer pTupleBuf);

    /**
     * @return number of attributes accessed
     */
    uint size() const
    {
        return ppAttributeAccessors.size();
    }

    // TODO:  private

    /**
     * @return the array of bit fields for the current tuple image
     */
    boost::dynamic_bitset<FixedBuffer> const &getBitFields() const
    {
        return bitFields;
    }

    /**
     * Resolves an indirect offset into a pointer to the data offset.
     *
     * @param iIndirectOffset indirect offset within tuple image
     *
     * @return pointer to data offset
     */
    StoredValueOffset const *referenceIndirectOffset(uint iIndirectOffset) const
    {
        return referenceIndirectOffset(
            const_cast<PBuffer>(pTupleBuf),
            iIndirectOffset);
    }

    /**
     * Resolves an indirect offset into a pointer to the data offset.
     *
     * @param pTupleBuf target buffer
     *
     * @param iIndirectOffset indirect offset within tuple image
     *
     * @return pointer to data offset
     */
    static StoredValueOffset *referenceIndirectOffset(
        PBuffer pTupleBuf,uint iIndirectOffset)
    {
        return reinterpret_cast<StoredValueOffset *>(
            pTupleBuf + iIndirectOffset);
    }
};

inline uint TupleAccessor::getCurrentByteCount() const
{
    assert(pTupleBuf);
    return getBufferByteCount(pTupleBuf);
}

FENNEL_END_NAMESPACE

#endif

// End TupleAccessor.h
