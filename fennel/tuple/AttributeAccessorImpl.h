/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 1999-2007 John V. Sichi
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

#ifndef Fennel_AttributeAccessorImpl_Included
#define Fennel_AttributeAccessorImpl_Included

#include "fennel/tuple/AttributeAccessor.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/tuple/TupleData.h"

#ifdef __MINGW32__
#include <../w32api/winsock2.h>
#else
#include <netinet/in.h>
#endif

FENNEL_BEGIN_NAMESPACE

/**
 * AttributeAccessorImpl is a common base for all implementations of the
 * AttributeAccessor interface.
 */
class AttributeAccessorImpl : public AttributeAccessor
{
public:
    explicit AttributeAccessorImpl();

    /**
     * Tests the null indicator for this attribute in the tuple's null bit
     * array.
     *
     * @param tupleAccessor containing TupleAccessor set up with the current
     * tuple image to be accessed
     *
     * @param value receives the null bit
     *
     * @return true if value is null; false otherwise
     */
    bool unmarshalNullableValue(
        TupleAccessor const &tupleAccessor,TupleDatum &value) const
    {
        if (tupleAccessor.getBitFields()[iNullBit]) {
            value.pData = NULL;
            return true;
        } else {
            return false;
        }
    }

    void marshalValueData(
        PBuffer pDestData,
        TupleDatum const &value) const
    {
        memcpy(pDestData,value.pData,value.cbData);
    }
};

/**
 * FixedWidthAccessor accesses NOT NULL fixed width attributes.
 */
class FixedWidthAccessor
    : public AttributeAccessorImpl
{
public:
    void unmarshalValue(
        TupleAccessor const &tupleAccessor,TupleDatum &value) const
    {
        value.pData = tupleAccessor.getCurrentTupleBuf() + iFixedOffset;
    }
};

/**
 * FixedWidthNetworkAccessor16 accesses NOT NULL fixed width 16-bit attributes
 * in network byte order.
 */
class FixedWidthNetworkAccessor16
    : public FixedWidthAccessor
{
public:
    void unmarshalValue(
        TupleAccessor const &tupleAccessor,TupleDatum &value) const
    {
        assert(value.cbData == sizeof(uint16_t));
        FixedWidthAccessor::unmarshalValue(tupleAccessor,value);
        value.data16 = ntohs(*reinterpret_cast<uint16_t const *>(value.pData));
        value.pData = reinterpret_cast<PConstBuffer>(&(value.data16));
    }

    void marshalValueData(
        PBuffer pDestData,
        TupleDatum const &value) const
    {
        assert(value.cbData == sizeof(uint16_t));
        *reinterpret_cast<uint16_t *>(pDestData) =
            htons(*reinterpret_cast<uint16_t const *>(value.pData));
    }
};

/**
 * FixedWidthNetworkAccessor32 accesses NOT NULL fixed width 32-bit attributes
 * in network byte order.
 */
class FixedWidthNetworkAccessor32
    : public FixedWidthAccessor
{
public:
    void unmarshalValue(
        TupleAccessor const &tupleAccessor,TupleDatum &value) const
    {
        assert(value.cbData == sizeof(uint32_t));
        FixedWidthAccessor::unmarshalValue(tupleAccessor,value);
        value.data32 = ntohl(*reinterpret_cast<uint32_t const *>(value.pData));
        value.pData = reinterpret_cast<PConstBuffer>(&(value.data32));
    }

    void marshalValueData(
        PBuffer pDestData,
        TupleDatum const &value) const
    {
        assert(value.cbData == sizeof(uint32_t));
        *reinterpret_cast<uint32_t *>(pDestData) =
            htonl(*reinterpret_cast<uint32_t const *>(value.pData));
    }
};

/**
 * FixedWidthNetworkAccessor64 accesses NOT NULL fixed width 64-bit attributes
 * in network byte order.
 */
class FixedWidthNetworkAccessor64
    : public FixedWidthAccessor
{
public:
    void unmarshalValue(
        TupleAccessor const &tupleAccessor,TupleDatum &value) const
    {
        assert(value.cbData == sizeof(uint64_t));
        FixedWidthAccessor::unmarshalValue(tupleAccessor,value);
        value.data64 = ntohll(*reinterpret_cast<uint64_t const *>(value.pData));
        value.pData = reinterpret_cast<PConstBuffer>(&(value.data64));
    }

    void marshalValueData(
        PBuffer pDestData,
        TupleDatum const &value) const
    {
        assert(value.cbData == sizeof(uint64_t));
        *reinterpret_cast<uint64_t *>(pDestData) =
            htonll(*reinterpret_cast<uint64_t const *>(value.pData));
    }
};

/**
 * FixedOffsetVarWidthAccessor accesses the first variable-width attribute if
 * it is NOT NULL.  This attribute is special because the offset is fixed, but
 * the width is not.
 */
template<bool network>
class FixedOffsetVarWidthAccessor
    : public AttributeAccessorImpl
{
public:
    void unmarshalValue(
        TupleAccessor const &tupleAccessor,TupleDatum &value) const
    {
        value.pData = tupleAccessor.getCurrentTupleBuf() + iFixedOffset;
        TupleAccessor::StoredValueOffset const *pEndOffset =
            tupleAccessor.referenceIndirectOffset(iEndIndirectOffset);
        uint16_t iEndOffset = *pEndOffset;
        if (network) {
            iEndOffset = ntohs(iEndOffset);
        }
        value.cbData = iEndOffset - iFixedOffset;
        assert(value.cbData <= cbStorage);
    }
};

/**
 * VarOffsetAccessor accesses subsequent variable-width attributes that are NOT
 * NULL.
 */
template<bool network>
class VarOffsetAccessor
    : public AttributeAccessorImpl
{
public:
    void unmarshalValue(
        TupleAccessor const &tupleAccessor,TupleDatum &value) const
    {
        TupleAccessor::StoredValueOffset const *pEndOffset =
            tupleAccessor.referenceIndirectOffset(iEndIndirectOffset);
        uint iOffset = pEndOffset[-1];
        uint iEndOffset = pEndOffset[0];
        if (network) {
            iOffset = ntohs(iOffset);
            iEndOffset = ntohs(iEndOffset);
        }
        value.pData = tupleAccessor.getCurrentTupleBuf() + iOffset;
        value.cbData = iEndOffset - iOffset;
        assert(value.cbData <= cbStorage);
    }
};

/**
 * BitAccessor accesses NOT NULL bit attributes.
 */
class BitAccessor
    : public AttributeAccessorImpl
{
public:
    void unmarshalValue(
        TupleAccessor const &tupleAccessor,TupleDatum &value) const
    {
        if (tupleAccessor.getBitFields()[iValueBit]) {
            value.pData = reinterpret_cast<PConstBuffer>(
                &(TupleAccessor::BOOL_TRUE));
        } else {
            value.pData = reinterpret_cast<PConstBuffer>(
                &(TupleAccessor::BOOL_FALSE));
        }
    }
};

template <class Accessor>
class NullableAccessor : public Accessor
{
public:
    void unmarshalValue(
        TupleAccessor const &tupleAccessor,TupleDatum &value) const
    {
        if (Accessor::unmarshalNullableValue(tupleAccessor,value)) {
            return;
        }
        return Accessor::unmarshalValue(tupleAccessor,value);
    }
};

FENNEL_END_NAMESPACE

#endif

// End AttributeAccessorImpl.h
