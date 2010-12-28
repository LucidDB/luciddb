/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 1999 John V. Sichi
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
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/AttributeAccessorImpl.h"
#include "fennel/tuple/StoredTypeDescriptor.h"
#include <boost/lambda/bind.hpp>
#include <boost/lambda/construct.hpp>

FENNEL_BEGIN_CPPFILE("$Id$");

const bool TupleAccessor::BOOL_TRUE = true;

const bool TupleAccessor::BOOL_FALSE = false;

// NOTE jvs 11-Feb-2006: Set this to 1 to debug problems with
// TupleAccessor::setCurrentTupleBuf being passed invalid tuple images.
// Typical symptom is a Boost assertion failure in ~TupleAccessor when
// dynamic_bitset detects invalid bits set.  When this is set to 1, we write a
// magic number as a prefix every time a tuple is marshalled, and verify it
// when setCurrentTupleBuf is called.  CAUTION: do NOT check in changes with
// this set to 1!  Also realize that this affects on-disk storage, so if you
// run without it and generate a datafile, then run with it and read that
// datafile, you should expect an assertion failure.
#define DEBUG_TUPLE_ACCESS 0

/**
 * Matches net.sf.farrago.runtime.FennelTupleWriter.
 */
static const MagicNumber TUPLE_MAGIC_NUMBER = 0x9897ab509de7dcf5LL;

TupleAccessor::TupleAccessor()
{
    pTupleBuf = NULL;
}

TupleAccessor::~TupleAccessor()
{
    clear();
}

void TupleAccessor::clear()
{
    using namespace boost::lambda;
    std::for_each(
        ppAttributeAccessors.begin(),
        ppAttributeAccessors.end(),
        bind(delete_ptr(),_1));
    ppAttributeAccessors.clear();
    pVarWidthAttrIndices.clear();
    marshalOrder.clear();
    pTupleBuf = NULL;
    bAlignedVar = false;
}

// TODO:  clean up template factory craziness below, and add network support
// for 64-bit types and UNICODE

// NOTE:  there's a small amount of code in Farrago which is sensitive to this
// algorithm, e.g. bit field ordering

void TupleAccessor::compute(
    TupleDescriptor const &tuple,TupleFormat formatInit)
{
    clear();
    format = formatInit;

    // these vectors keep track of the logical 0-based indices of the
    // attributes belonging to the various attribute storage classes
    VectorOfUint aligned8;
    VectorOfUint aligned4;
    VectorOfUint aligned2;
    VectorOfUint unalignedFixed;
    VectorOfUint unalignedVar;
    VectorOfUint alignedVar2;

    // special-case reference to the accessor for the first variable-width
    // attribute
    AttributeAccessor *pFirstVariableAccessor = NULL;

    // number of bit fields seen so far
    nBitFields = 0;

    // sum of max storage size for variable-width attributes seen so far
    uint cbVarDataMax = 0;

    // sum of total storage size seen so far; this is used as an accumulator
    // for assigning actual offsets
    cbMaxStorage = 0;

#if DEBUG_TUPLE_ACCESS
    cbMaxStorage += sizeof(MagicNumber);
#endif

    // first pass over all attributes in logical order:  collate them into
    // storage classes and precompute everything we can
    for (uint iAttr = 0; iAttr < tuple.size(); iAttr++) {
        AttributeAccessor *pNewAccessor;
        TupleAttributeDescriptor const &attr = tuple[iAttr];
        uint cbFixed = attr.pTypeDescriptor->getFixedByteCount();
        uint cbMin = attr.pTypeDescriptor->getMinByteCount(attr.cbStorage);
        if (cbFixed) {
            assert(cbFixed == attr.cbStorage);
            assert(cbFixed == cbMin);
        }
        bool bFixedWidth = (cbMin == attr.cbStorage);
        if (bFixedWidth && !attr.cbStorage) {
            if (!attr.pTypeDescriptor->getMinByteCount(1)) {
                // this is a "0-length variable-width" field masquerading
                // as a fixed-width field
                bFixedWidth = false;
            }
        }
        bool bNullable = attr.isNullable;
        uint nBits = (attr.pTypeDescriptor->getBitCount());
        assert(nBits <= 1);
        if (format == TUPLE_FORMAT_ALL_FIXED) {
            bFixedWidth = true;
            nBits = 0;
        }
        uint iAlign = attr.pTypeDescriptor->getAlignmentByteCount(
            attr.cbStorage);
        if (!bFixedWidth) {
            cbVarDataMax += attr.cbStorage;
            if (iAlign == 2) {
                alignedVar2.push_back(iAttr);
                bAlignedVar = true;
            } else {
                assert(iAlign == 1);
                unalignedVar.push_back(iAttr);
            }
            // We need to defer actual creation of the accessor until
            // after we've sorted out aligned from unaligned, so just
            // fill a placeholder into the array for now and
            // we'll overwrite it later.
            pNewAccessor = new VarOffsetAccessor<false>();
        } else if (nBits) {
            if (bNullable) {
                pNewAccessor = new NullableAccessor<BitAccessor>;
            } else {
                pNewAccessor = new BitAccessor;
            }
            pNewAccessor->iValueBit = nBitFields;
            nBitFields++;
        } else {
            assert((cbMin % iAlign) == 0);
            bool bArray =
                StandardTypeDescriptor::isArray(
                    StandardTypeDescriptorOrdinal(
                        attr.pTypeDescriptor->getOrdinal()));
            switch (iAlign) {
            case 2:
                if (bNullable) {
                    if ((format == TUPLE_FORMAT_NETWORK) && !bArray) {
                        pNewAccessor =
                            new NullableAccessor<FixedWidthNetworkAccessor16>;
                    } else {
                        pNewAccessor =
                            new NullableAccessor<FixedWidthAccessor>;
                    }
                } else {
                    if ((format == TUPLE_FORMAT_NETWORK) && !bArray) {
                        pNewAccessor = new FixedWidthNetworkAccessor16;
                    } else {
                        pNewAccessor = new FixedWidthAccessor;
                    }
                }
                break;
            case 4:
                if (bNullable) {
                    if (format == TUPLE_FORMAT_NETWORK) {
                        pNewAccessor =
                            new NullableAccessor<FixedWidthNetworkAccessor32>;
                    } else {
                        pNewAccessor =
                            new NullableAccessor<FixedWidthAccessor>;
                    }
                } else {
                    if (format == TUPLE_FORMAT_NETWORK) {
                        pNewAccessor = new FixedWidthNetworkAccessor32;
                    } else {
                        pNewAccessor = new FixedWidthAccessor;
                    }
                }
                break;
            case 8:
                if (bNullable) {
                    if (format == TUPLE_FORMAT_NETWORK) {
                        pNewAccessor =
                            new NullableAccessor<FixedWidthNetworkAccessor64>;
                    } else {
                        pNewAccessor =
                            new NullableAccessor<FixedWidthAccessor>;
                    }
                } else {
                    if (format == TUPLE_FORMAT_NETWORK) {
                        pNewAccessor = new FixedWidthNetworkAccessor64;
                    } else {
                        pNewAccessor = new FixedWidthAccessor;
                    }
                }
                break;
            default:
                if (bNullable) {
                    pNewAccessor = new NullableAccessor<FixedWidthAccessor>;
                } else {
                    pNewAccessor = new FixedWidthAccessor;
                }
                break;
            }
            switch (iAlign) {
            case 1:
                unalignedFixed.push_back(iAttr);
                break;
            case 2:
                aligned2.push_back(iAttr);
                break;
            case 4:
                aligned4.push_back(iAttr);
                break;
            case 8:
                aligned8.push_back(iAttr);
                break;
            default:
                permAssert(false);
            }
        }
        if (bNullable) {
            pNewAccessor->iNullBit = nBitFields;
            nBitFields++;
        }
        pNewAccessor->cbStorage = attr.cbStorage;
        ppAttributeAccessors.push_back(pNewAccessor);
    }
    bitFields.resize(nBitFields);

    // fill in variable-width attributes, since we had to defer them
    // above so that we could collect aligned ones before unaligned ones
    pVarWidthAttrIndices.resize(alignedVar2.size() + unalignedVar.size());
    std::copy(
        alignedVar2.begin(), alignedVar2.end(),
        pVarWidthAttrIndices.begin());
    std::copy(
        unalignedVar.begin(), unalignedVar.end(),
        pVarWidthAttrIndices.begin() + alignedVar2.size());
    for (uint i = 0; i < pVarWidthAttrIndices.size(); ++i) {
        uint iAttr = pVarWidthAttrIndices[i];
        TupleAttributeDescriptor const &attr = tuple[iAttr];
        bool bNullable = attr.isNullable;
        AttributeAccessor *pNewAccessor;
        if (pFirstVariableAccessor) {
            if (bNullable) {
                if (format == TUPLE_FORMAT_NETWORK) {
                    pNewAccessor =
                        new NullableAccessor< VarOffsetAccessor<true> >;
                } else {
                    pNewAccessor =
                        new NullableAccessor< VarOffsetAccessor<false> >;
                }
            } else {
                if (format == TUPLE_FORMAT_NETWORK) {
                    pNewAccessor = new VarOffsetAccessor<true>;
                } else {
                    pNewAccessor = new VarOffsetAccessor<false>;
                }
            }
        } else {
            if (bNullable) {
                if (format == TUPLE_FORMAT_NETWORK) {
                    pFirstVariableAccessor =
                        new NullableAccessor<
                        FixedOffsetVarWidthAccessor<true> >;
                } else {
                    pFirstVariableAccessor =
                        new NullableAccessor<
                        FixedOffsetVarWidthAccessor<false> >;
                }
            } else {
                if (format == TUPLE_FORMAT_NETWORK) {
                    pFirstVariableAccessor =
                        new FixedOffsetVarWidthAccessor<true>;
                } else {
                    pFirstVariableAccessor =
                        new FixedOffsetVarWidthAccessor<false>;
                }
            }
            pNewAccessor = pFirstVariableAccessor;
        }
        AttributeAccessor *pPlaceholder = ppAttributeAccessors[iAttr];
        pNewAccessor->cbStorage = attr.cbStorage;
        pNewAccessor->iNullBit = pPlaceholder->iNullBit;
        // overwrite placeholder
        ppAttributeAccessors[iAttr] = pNewAccessor;
        delete pPlaceholder;
    }

    // now, make a pass over each storage class, calculating actual offsets;
    // note that initFixedAccessors advances cbMaxStorage as a side-effect
    initFixedAccessors(tuple, aligned8);
    initFixedAccessors(tuple, aligned4);
    initFixedAccessors(tuple, aligned2);

    if (pFirstVariableAccessor) {
        iFirstVarEndIndirectOffset = cbMaxStorage;
    } else {
        iFirstVarEndIndirectOffset = MAXU;
    }

    for (uint i = 0; i < pVarWidthAttrIndices.size(); i++) {
        ppAttributeAccessors[pVarWidthAttrIndices[i]]->iEndIndirectOffset =
            cbMaxStorage;
        cbMaxStorage += sizeof(StoredValueOffset);
    }

    if (pFirstVariableAccessor) {
        iLastVarEndIndirectOffset = cbMaxStorage - sizeof(StoredValueOffset);
    } else {
        iLastVarEndIndirectOffset = MAXU;
    }

    initFixedAccessors(tuple, unalignedFixed);

    if (nBitFields) {
        iBitFieldOffset = cbMaxStorage;
    } else {
        iBitFieldOffset = MAXU;
    }
    cbMaxStorage += bytesForBits(nBitFields);
    if (pFirstVariableAccessor) {
        if (bAlignedVar) {
            // First variable-width value needs to be 2-byte aligned,
            // so add one byte of padding if necessary.
            if (cbMaxStorage & 1) {
                ++cbMaxStorage;
            }
        }
        pFirstVariableAccessor->iFixedOffset = cbMaxStorage;
        iFirstVarOffset = cbMaxStorage;
    } else {
        iFirstVarOffset = MAXU;
    }
    cbMinStorage = cbMaxStorage;
    cbMaxStorage += cbVarDataMax;

    // Avoid 0-byte tuples, because it's very hard to count something
    // that isn't there.  This bumps them up to 1-byte, which will get
    // further bumped up to the minimum alignment unit below.
    if (!cbMaxStorage) {
        cbMinStorage = 1;
        cbMaxStorage = 1;
    }

    // now round the entire row width up to the next alignment boundary;
    // this only affects the end of the row, which is why it is done
    // AFTER computing cbMaxStorage based on the unaligned cbMinStorage
    cbMinStorage = alignRoundUp(cbMinStorage);
    cbMaxStorage = alignRoundUp(cbMaxStorage);

    // if aligned variable-width fields are present, permute the marshalling
    // order so that they come before unaligned variable-width fields
    if (bAlignedVar) {
        // add all of the fixed-width attributes
        for (uint i = 0; i < tuple.size(); ++i) {
            AttributeAccessor const &accessor = getAttributeAccessor(i);
            if (isMAXU(accessor.iEndIndirectOffset)) {
                marshalOrder.push_back(i);
            }
        }
        uint nFixed = marshalOrder.size();
        assert(nFixed + pVarWidthAttrIndices.size() == tuple.size());
        marshalOrder.resize(tuple.size());
        // then all of the variable-width attributes, in the correct order
        std::copy(
            pVarWidthAttrIndices.begin(),
            pVarWidthAttrIndices.end(),
            marshalOrder.begin() + nFixed);
    }
}

void TupleAccessor::initFixedAccessors(
    TupleDescriptor const &tuple,VectorOfUint &v)
{
    for (uint i = 0; i < v.size(); i++) {
        uint iAttr = v[i];
        TupleAttributeDescriptor const &attr = tuple[iAttr];
        AttributeAccessor &accessor = *(ppAttributeAccessors[iAttr]);
        accessor.iFixedOffset = cbMaxStorage;
        cbMaxStorage += attr.cbStorage;
    }
}

uint TupleAccessor::getBufferByteCount(PConstBuffer pBuf) const
{
    if (isFixedWidth()) {
        return cbMaxStorage;
    } else {
        // variable-width tuple:  use the end of the last variable-width
        // attribute
        StoredValueOffset cb =
            *referenceIndirectOffset(
                const_cast<PBuffer>(pBuf),
                iLastVarEndIndirectOffset);
        if (format == TUPLE_FORMAT_NETWORK) {
            cb = ntohs(cb);
        }
        // round up for alignment padding
        return alignRoundUp(cb);
    }
}

uint TupleAccessor::getByteCount(TupleData const &tuple) const
{
    if (isFixedWidth()) {
        return cbMaxStorage;
    } else {
        // variable-width tuple:  add up all var-width fields
        uint cb = iFirstVarOffset;
        for (uint i = 0; i < pVarWidthAttrIndices.size(); ++i) {
            TupleDatum const &datum = tuple[pVarWidthAttrIndices[i]];
            if (datum.pData) {
                cb += datum.cbData;
            }
        }
        // round up for alignment padding
        return alignRoundUp(cb);
    }
}

bool TupleAccessor::isBufferSufficient(
    TupleData const &tuple,uint cbBuffer) const
{
    // fast optimistic check
    if (getMaxByteCount() <= cbBuffer) {
        return true;
    }
    // slower conservative check
    return getByteCount(tuple) <= cbBuffer;
}

void TupleAccessor::setCurrentTupleBuf(PConstBuffer pTupleBufInit, bool valid)
{
    assert(pTupleBufInit);
    pTupleBuf = pTupleBufInit;          // bind to buffer
    if (!isMAXU(iBitFieldOffset)) {
        // if buffer holds a valid marshalled tuple, load its bitFields
        if (valid) {
#if DEBUG_TUPLE_ACCESS
            assert(
                *reinterpret_cast<MagicNumber const *>(pTupleBuf)
                == TUPLE_MAGIC_NUMBER);
#endif
            // TODO:  trick dynamic_bitset to avoid copy
            boost::from_block_range(
                pTupleBuf + iBitFieldOffset,
                pTupleBuf + iBitFieldOffset + bitFields.num_blocks(),
                bitFields);
        }
    }
}

void TupleAccessor::resetCurrentTupleBuf()
{
    pTupleBuf = NULL;
}

void TupleAccessor::unmarshal(TupleData &tuple,uint iFirstDatum) const
{
    uint n = std::min(tuple.size() - iFirstDatum, ppAttributeAccessors.size());

    if ((format == TUPLE_FORMAT_NETWORK) || bAlignedVar) {
        // for TUPLE_FORMAT_NETWORK, unmarshal attributes individually
        for (uint i = 0; i < n; ++i) {
            getAttributeAccessor(i).unmarshalValue(
                *this,tuple[iFirstDatum + i]);
        }
        return;
    }

    // for other formats, we can go a little faster by avoiding per-attribute
    // call overhead

    uint iNextVarOffset = iFirstVarOffset;
    StoredValueOffset const *pNextVarEndOffset =
        referenceIndirectOffset(iFirstVarEndIndirectOffset);

    for (uint i = 0; i < n; i++) {
        TupleDatum &value = tuple[i + iFirstDatum];
        AttributeAccessor const &accessor = getAttributeAccessor(i);
        if (!isMAXU(accessor.iNullBit)) {
            if (bitFields[accessor.iNullBit]) {
                value.pData = NULL;
                if (!isMAXU(accessor.iEndIndirectOffset)) {
                    pNextVarEndOffset++;
                }
                continue;
            }
        }
        if (!isMAXU(accessor.iFixedOffset)) {
            value.pData = getCurrentTupleBuf() + accessor.iFixedOffset;
        } else if (isMAXU(accessor.iValueBit)) {
            value.pData = getCurrentTupleBuf() + iNextVarOffset;
        } else {
            static_cast<BitAccessor const &>(accessor).unmarshalValue(
                *this,value);
        }
        if (!isMAXU(accessor.iEndIndirectOffset)) {
            assert(
                pNextVarEndOffset
                == referenceIndirectOffset(accessor.iEndIndirectOffset));
            uint iEndOffset = *pNextVarEndOffset;
            pNextVarEndOffset++;
            value.cbData = iEndOffset - iNextVarOffset;
            iNextVarOffset = iEndOffset;
        }
        assert(value.cbData <= accessor.cbStorage);
    }
}

void TupleAccessor::marshal(TupleData const &tuple,PBuffer pTupleBufDest)
{
#if DEBUG_TUPLE_ACCESS
    *reinterpret_cast<MagicNumber *>(pTupleBufDest) = TUPLE_MAGIC_NUMBER;
#endif

    pTupleBuf = pTupleBufDest;

    uint iNextVarOffset = iFirstVarOffset;
    StoredValueOffset *pNextVarEndOffset =
        referenceIndirectOffset(pTupleBufDest, iFirstVarEndIndirectOffset);

    for (uint i = 0; i < tuple.size(); i++) {
        uint iAttr;
        if (bAlignedVar) {
            iAttr = marshalOrder[i];
        } else {
            iAttr = i;
        }
        TupleDatum const &value = tuple[iAttr];
        AttributeAccessor const &accessor = getAttributeAccessor(iAttr);
        if (!isMAXU(accessor.iNullBit)) {
            bitFields[accessor.iNullBit] = value.pData ? false : true;
        }
        if (value.pData) {
            if (isMAXU(accessor.iValueBit)) {
                uint iOffset;
                if (!isMAXU(accessor.iFixedOffset)) {
                    iOffset = accessor.iFixedOffset;
                } else {
                    iOffset = iNextVarOffset;
                }
                assert(value.cbData <= accessor.cbStorage);
                accessor.marshalValueData(
                    pTupleBufDest + iOffset,
                    value);
            } else {
                bitFields[accessor.iValueBit] =
                    *reinterpret_cast<bool const *>(value.pData);
            }
        } else {
            // if you hit this assert, most likely the result produced a null
            // but type derivation in SqlValidator derived a non-nullable
            // result type
            assert(!isMAXU(accessor.iNullBit));
        }
        if (!isMAXU(accessor.iEndIndirectOffset)) {
            assert(
                pNextVarEndOffset
                == referenceIndirectOffset(accessor.iEndIndirectOffset));
            if (value.pData) {
                iNextVarOffset += value.cbData;
            }
            // regardless of whether the value is null, we need to record the
            // end offset since it also marks the start of the next
            // non-null value
            if (format == TUPLE_FORMAT_NETWORK) {
                *pNextVarEndOffset =
                    htons(static_cast<StoredValueOffset>(iNextVarOffset));
            } else {
                *pNextVarEndOffset = iNextVarOffset;
            }
            pNextVarEndOffset++;
        }
    }
    if (!isMAXU(iBitFieldOffset)) {
        // TODO:  trick dynamic_bitset to avoid copy
        boost::to_block_range(
            bitFields,
            pTupleBufDest + iBitFieldOffset);
    }
}

FENNEL_END_CPPFILE("$Id$");

// End TupleAccessor.cpp
