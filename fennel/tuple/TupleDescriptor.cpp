/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2003-2009 SQLstream, Inc.
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

#include "fennel/common/CommonPreamble.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/tuple/StoredTypeDescriptor.h"
#include "fennel/tuple/StoredTypeDescriptorFactory.h"
#include "fennel/common/DataVisitor.h"
#include "fennel/common/ByteInputStream.h"
#include "fennel/common/ByteOutputStream.h"

// REVIEW:  this is here for netinet/in.h platform independence, but shouldn't
// need to include the whole thing
#include "fennel/tuple/AttributeAccessorImpl.h"

FENNEL_BEGIN_CPPFILE("$Id$");

TupleAttributeDescriptor::TupleAttributeDescriptor()
{
    pTypeDescriptor = NULL;
    isNullable = false;
    cbStorage = 0;
}

TupleAttributeDescriptor::TupleAttributeDescriptor(
    StoredTypeDescriptor const &typeDescriptor,
    bool isNullableInit,
    TupleStorageByteLength cbStorageInit)
{
    pTypeDescriptor = &typeDescriptor;
    isNullable = isNullableInit;
    if (cbStorageInit) {
        uint cbFixed = typeDescriptor.getFixedByteCount();
        assert(!cbFixed || (cbFixed == cbStorageInit));
        cbStorage = cbStorageInit;
    } else {
        cbStorage = typeDescriptor.getFixedByteCount();
    }
}

bool TupleAttributeDescriptor::operator == (
    TupleAttributeDescriptor const &other) const
{
    return
        (pTypeDescriptor->getOrdinal() == other.pTypeDescriptor->getOrdinal())
        && (isNullable == other.isNullable)
        && (cbStorage == other.cbStorage);
}

void TupleDescriptor::projectFrom(
    TupleDescriptor const &tupleDescriptor,
    TupleProjection const &tupleProjection)
{
    clear();
    for (uint i = 0; i < tupleProjection.size(); ++i) {
        push_back(tupleDescriptor[tupleProjection[i]]);
    }
}

int TupleDescriptor::compareTuples(
    TupleData const &tuple1,
    TupleData const &tuple2) const
{
    int keyComp;
    // REVIEW:  should pass n as a param instead of recalculating it each time
    size_t keyCount = std::min(tuple1.size(), tuple2.size());
    keyCount = std::min(keyCount, size());
    keyComp = compareTuplesKey(tuple1, tuple2, keyCount);
    return keyComp;
}

int TupleDescriptor::compareTuples(
    TupleData const &tuple1, TupleProjection const &proj1,
    TupleData const &tuple2, TupleProjection const &proj2,
    bool *containsNullKey) const
{
    size_t keyCount = std::min(proj1.size(), proj2.size());

    if (containsNullKey) {
        *containsNullKey = false;
    }
    for (uint i = 0; i < keyCount; ++i) {
        TupleDatum const &datum1 = tuple1[proj1[i]];
        TupleDatum const &datum2 = tuple2[proj2[i]];
        // TODO:  parameterize NULL-value collation
        if (!datum1.pData) {
            if (containsNullKey) {
                *containsNullKey = true;
            }
            if (!datum2.pData) {
                continue;
            }
            return -(i + 1);
        } else if (!datum2.pData) {
            if (containsNullKey) {
                *containsNullKey = true;
            }
            return (i + 1);
        }
        int c = (*this)[i].pTypeDescriptor->compareValues(
            datum1.pData,
            datum1.cbData,
            datum2.pData,
            datum2.cbData);
        if (c > 0) {
            return (i + 1);
        } else if (c < 0) {
            return -(i + 1);
        }
    }
    return 0;

}

int TupleDescriptor::compareTuplesKey(
    TupleData const &tuple1,
    TupleData const &tuple2,
    uint keyCount) const
{
    assert(keyCount <= std::min(tuple1.size(), tuple2.size()));

    for (uint i = 0; i < keyCount; ++i) {
        TupleDatum const &datum1 = tuple1[i];
        TupleDatum const &datum2 = tuple2[i];
        // TODO:  parameterize NULL-value collation
        if (!datum1.pData) {
            if (!datum2.pData) {
                continue;
            }
            return -(i + 1);
        } else if (!datum2.pData) {
            return (i + 1);
        }
        int c = (*this)[i].pTypeDescriptor->compareValues(
            datum1.pData,
            datum1.cbData,
            datum2.pData,
            datum2.cbData);
        if (c > 0) {
            return (i + 1);
        } else if (c < 0) {
            return -(i + 1);
        }
    }
    return 0;
}

void TupleDescriptor::visit(
    TupleData const &tuple,
    DataVisitor &dataVisitor,
    bool visitLengths) const
{
    for (uint i = 0; i < tuple.size(); ++i) {
        if (!tuple[i].pData) {
            if (visitLengths) {
                dataVisitor.visitUnsignedInt(0);
            }
            dataVisitor.visitBytes(NULL, 0);
        } else {
            if (visitLengths) {
                dataVisitor.visitUnsignedInt(tuple[i].cbData);
            }
            (*this)[i].pTypeDescriptor->visitValue(
                dataVisitor,
                tuple[i].pData,
                tuple[i].cbData);
        }
    }
}

// NOTE: read comments on struct StoredNode before modifying the persistence
// code below.  Also note that we use specific type sizes and network byte order
// since TupleDescriptors may be transmitted as binary over the network/JNI.
// May want to use XML for that instead and make this code perform better
// (since it's used by transaction logging).

void TupleDescriptor::writePersistent(ByteOutputStream &stream) const
{
    uint32_t iData = htonl(size());
    stream.writeValue(iData);
    for (uint i = 0; i < size(); ++i) {
        TupleAttributeDescriptor const &attrDesc = (*this)[i];
        iData = htonl(attrDesc.pTypeDescriptor->getOrdinal());
        stream.writeValue(iData);
        iData = htonl(attrDesc.isNullable);
        stream.writeValue(iData);
        // Assume TupleAttributeDescriptor is long in htonl()
        iData = htonl(attrDesc.cbStorage);
        stream.writeValue(iData);
    }
}

void TupleDescriptor::readPersistent(
    ByteInputStream &stream,
    StoredTypeDescriptorFactory const &typeFactory)
{
    clear();
    uint32_t n;
    stream.readValue(n);
    n = ntohl(n);
    for (uint i = 0; i < n; ++i) {
        uint32_t iData;
        stream.readValue(iData);
        StoredTypeDescriptor const &typeDescriptor =
            typeFactory.newDataType(ntohl(iData));
        stream.readValue(iData);
        bool isNullable = ntohl(iData);
        stream.readValue(iData);
        TupleStorageByteLength cbStorage = ntohl(iData);
        push_back(
            TupleAttributeDescriptor(
                typeDescriptor, isNullable, cbStorage));
    }
}

void TupleProjection::writePersistent(
    ByteOutputStream &stream) const
{
    uint32_t iData = htonl(size());
    stream.writeValue(iData);
    for (uint i = 0; i < size(); ++i) {
        iData = htonl((*this)[i]);
        stream.writeValue(iData);
    }
}

void TupleProjection::readPersistent(
    ByteInputStream &stream)
{
    clear();
    uint32_t n;
    stream.readValue(n);
    n = ntohl(n);
    for (uint i = 0; i < n; ++i) {
        uint32_t iData;
        stream.readValue(iData);
        push_back(ntohl(iData));
    }
}

void TupleProjection::projectFrom(
    TupleProjection const &sourceProjection,
    TupleProjection const &tupleProjection)
{
    clear();
    for (uint i = 0; i < tupleProjection.size(); ++i) {
        push_back(sourceProjection[tupleProjection[i]]);
    }
}

bool TupleDescriptor::containsNullable() const
{
    for (uint i = 0; i < size(); ++i) {
        if ((*this)[i].isNullable) {
            return true;
        }
    }
    return false;
}

bool TupleDescriptor::storageEqual(
    TupleDescriptor const &other) const
{
    uint sz = size();
    if (other.size() != sz) {
        return false;
    }

    TupleAttributeDescriptor const * us;
    TupleAttributeDescriptor const * them;

    for (uint i = 0; i < sz; ++i) {
        us = &(*this)[i];
        them = &other[i];
        if ((us->pTypeDescriptor->getOrdinal()
             != them->pTypeDescriptor->getOrdinal())
            || us->cbStorage != them->cbStorage)
        {
            return false;
        }
    }
    return true;
}

TupleStorageByteLength TupleDescriptor::getMaxByteCount() const
{
    TupleStorageByteLength length = 0;

    for (uint i = 0; i < size(); i ++) {
        length += (*this)[i].cbStorage;
    }
    return length;
}

std::ostream &operator<<(std::ostream &str,TupleDescriptor const &tupleDesc)
{
    str << "{" << std::endl;
    for (uint i = 0; i < tupleDesc.size(); ++i) {
        str << "\t" << i << ":  ";
        str << tupleDesc[i];
        str << std::endl;
    }
    str << "}" << std::endl;
    return str;
}

std::ostream &operator<<(
    std::ostream &str,
    TupleAttributeDescriptor const &attrDesc)
{
    StoredTypeDescriptor::Ordinal ordinal =
        attrDesc.pTypeDescriptor->getOrdinal();

    if (ordinal < STANDARD_TYPE_END) {
        str << "type = " << StandardTypeDescriptor::toString(
            StandardTypeDescriptorOrdinal(ordinal));
    } else {
        str << "type ordinal = " << ordinal;
    }
    str << ", isNullable = " << attrDesc.isNullable;
    str << ", cbStorage = " << attrDesc.cbStorage;
    return str;
}

FENNEL_END_CPPFILE("$Id$");

// End TupleDescriptor.cpp
