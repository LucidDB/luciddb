/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2003-2007 SQLstream, Inc.
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

#include "fennel/common/CommonPreamble.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/common/DataVisitor.h"

#include <limits>

FENNEL_BEGIN_CPPFILE("$Id$");

// TODO:  move these

StoredTypeDescriptor::~StoredTypeDescriptor()
{
}

StoredTypeDescriptorFactory::~StoredTypeDescriptorFactory()
{
}

template <class T,StandardTypeDescriptorOrdinal typeOrdinal>
class NumericType : public StoredTypeDescriptor
{
    virtual Ordinal getOrdinal() const
    {
        return typeOrdinal;
    }

    virtual uint getBitCount() const
    {
        return 0;
    }

    virtual uint getFixedByteCount() const
    {
        return sizeof(T);
    }

    virtual uint getMinByteCount(uint cbMaxWidth) const
    {
        assert(cbMaxWidth == sizeof(T));
        return cbMaxWidth;
    }

    virtual uint getAlignmentByteCount(uint cbWidth) const
    {
        return sizeof(T);
    }

    virtual void visitValue(
        DataVisitor &dataVisitor,
        void const *pData,
        TupleStorageByteLength cbData) const
    {
        T t = *static_cast<T const *>(pData);
        assert(cbData == sizeof(T));
        if (std::numeric_limits<T>::is_signed) {
            dataVisitor.visitSignedInt(t);
        } else {
            dataVisitor.visitUnsignedInt(t);
        }
    }

    virtual int compareValues(
        void const *pData1,
        TupleStorageByteLength cbData1,
        void const *pData2,
        TupleStorageByteLength cbData2) const
    {
        assert(cbData1 == sizeof(T));
        assert(cbData2 == sizeof(T));
        T t1 = *static_cast<T const *>(pData1);
        T t2 = *static_cast<T const *>(pData2);
        if (t1 < t2) {
            return -1;
        } else if (t1 > t2) {
            return 1;
        } else {
            return 0;
        }
    }
};

template<>
void NumericType<double,STANDARD_TYPE_DOUBLE>::visitValue(
    DataVisitor &dataVisitor,
    void const *pData,
    TupleStorageByteLength cbData) const
{
    double d = *static_cast<double const *>(pData);
    assert(cbData == sizeof(double));
    dataVisitor.visitDouble(d);
}

template<>
void NumericType<float,STANDARD_TYPE_REAL>::visitValue(
    DataVisitor &dataVisitor,
    void const *pData,
    TupleStorageByteLength cbData) const
{
    float d = *static_cast<float const *>(pData);
    assert(cbData == sizeof(float));
    dataVisitor.visitFloat(d);
}

template<>
uint NumericType<bool,STANDARD_TYPE_BOOL>::getBitCount() const
{
    return 1;
}

class CharType : public StoredTypeDescriptor
{
    virtual Ordinal getOrdinal() const
    {
        return STANDARD_TYPE_CHAR;
    }

    virtual uint getBitCount() const
    {
        return 0;
    }

    virtual uint getFixedByteCount() const
    {
        return 0;
    }

    virtual uint getMinByteCount(uint cbMaxWidth) const
    {
        return cbMaxWidth;
    }

    virtual uint getAlignmentByteCount(uint cbWidth) const
    {
        return 1;
    }

    virtual void visitValue(
        DataVisitor &dataVisitor,
        void const *pData,
        TupleStorageByteLength cbData) const
    {
        char const *pStr = static_cast<char const *>(pData);
        dataVisitor.visitChars(pStr,cbData);
    }

    virtual int compareValues(
        void const *pData1,
        TupleStorageByteLength cbData1,
        void const *pData2,
        TupleStorageByteLength cbData2) const
    {
        assert(cbData1 == cbData2);
        // REVIEW jvs:  should be using strncmp here and below?
        return memcmp(pData1,pData2,cbData1);
    }
};

class UnicodeCharType : public StoredTypeDescriptor
{
    virtual Ordinal getOrdinal() const
    {
        return STANDARD_TYPE_UNICODE_CHAR;
    }

    virtual uint getBitCount() const
    {
        return 0;
    }

    virtual uint getFixedByteCount() const
    {
        return 0;
    }

    virtual uint getMinByteCount(uint cbMaxWidth) const
    {
        return cbMaxWidth;
    }

    virtual uint getAlignmentByteCount(uint cbWidth) const
    {
        return 2;
    }

    virtual void visitValue(
        DataVisitor &dataVisitor,
        void const *pData,
        TupleStorageByteLength cbData) const
    {
        assert((cbData & 1) == 0);
        Ucs2ConstBuffer pStr = static_cast<Ucs2ConstBuffer>(pData);
        dataVisitor.visitUnicodeChars(pStr,(cbData >> 1));
    }

    virtual int compareValues(
        void const *pData1,
        TupleStorageByteLength cbData1,
        void const *pData2,
        TupleStorageByteLength cbData2) const
    {
        assert(cbData1 == cbData2);
        assert((cbData1 & 1) == 0);
        Ucs2ConstBuffer pStr1 = static_cast<Ucs2ConstBuffer>(pData1);
        Ucs2ConstBuffer pStr2 = static_cast<Ucs2ConstBuffer>(pData2);
        uint nChars = (cbData1 >> 1);
        int c = compareStrings(pStr1, pStr2, nChars);
        return c;
    }

public:
    static inline int compareStrings(
        Ucs2ConstBuffer pStr1, Ucs2ConstBuffer pStr2, uint nChars)
    {
        for (uint i = 0; i < nChars; ++i) {
            int c = *pStr1;
            c -= *pStr2;
            if (c) {
                return c;
            }
            ++pStr1;
            ++pStr2;
        }
        return 0;
    }
};

class VarCharType : public StoredTypeDescriptor
{
    virtual Ordinal getOrdinal() const
    {
        return STANDARD_TYPE_VARCHAR;
    }

    virtual uint getBitCount() const
    {
        return 0;
    }

    virtual uint getFixedByteCount() const
    {
        return 0;
    }

    virtual uint getMinByteCount(uint cbMaxWidth) const
    {
        return 0;
    }

    virtual uint getAlignmentByteCount(uint cbWidth) const
    {
        return 1;
    }

    virtual void visitValue(
        DataVisitor &dataVisitor,
        void const *pData,
        TupleStorageByteLength cbData) const
    {
        char const *pStr = static_cast<char const *>(pData);
        dataVisitor.visitChars(pStr,cbData);
    }

    virtual int compareValues(
        void const *pData1,
        TupleStorageByteLength cbData1,
        void const *pData2,
        TupleStorageByteLength cbData2) const
    {
        TupleStorageByteLength cbMin = std::min(cbData1,cbData2);
        int rc = memcmp(pData1, pData2, cbMin);
        if (rc) {
            return rc;
        }
        if (cbData1 == cbData2) {
            return 0;
        }
        PConstBuffer pBuf1 = static_cast<PConstBuffer>(pData1);
        PConstBuffer pBuf2 = static_cast<PConstBuffer>(pData2);
        PConstBuffer trailStart,trailEnd;
        if (cbData1 > cbData2) {
            trailStart = pBuf1 + cbMin;
            trailEnd = pBuf1 + cbData1;
            rc = 1;
        } else {
            trailStart = pBuf2 + cbMin;
            trailEnd = pBuf2 + cbData2;
            rc = -1;
        }
        for (; trailStart < trailEnd; trailStart++) {
            if (*trailStart != ' ') {
                return rc;
            }
        }
        return 0;
    }
};

class UnicodeVarCharType : public StoredTypeDescriptor
{
    virtual Ordinal getOrdinal() const
    {
        return STANDARD_TYPE_UNICODE_VARCHAR;
    }

    virtual uint getBitCount() const
    {
        return 0;
    }

    virtual uint getFixedByteCount() const
    {
        return 0;
    }

    virtual uint getMinByteCount(uint cbMaxWidth) const
    {
        return 0;
    }

    virtual uint getAlignmentByteCount(uint cbWidth) const
    {
        return 2;
    }

    virtual void visitValue(
        DataVisitor &dataVisitor,
        void const *pData,
        TupleStorageByteLength cbData) const
    {
        assert((cbData & 1) == 0);
        Ucs2ConstBuffer pStr = static_cast<Ucs2ConstBuffer>(pData);
        dataVisitor.visitUnicodeChars(pStr,(cbData >> 1));
    }

    virtual int compareValues(
        void const *pData1,
        TupleStorageByteLength cbData1,
        void const *pData2,
        TupleStorageByteLength cbData2) const
    {
        assert((cbData1 & 1) == 0);
        assert((cbData2 & 1) == 0);
        Ucs2ConstBuffer pStr1 = static_cast<Ucs2ConstBuffer>(pData1);
        Ucs2ConstBuffer pStr2 = static_cast<Ucs2ConstBuffer>(pData2);
        TupleStorageByteLength cbMin = std::min(cbData1,cbData2);
        uint nCharsMin = (cbMin >> 1);
        int rc = UnicodeCharType::compareStrings(pStr1, pStr2, nCharsMin);
        if (rc) {
            return rc;
        }
        if (cbData1 == cbData2) {
            return 0;
        }
        Ucs2ConstBuffer trailStart,trailEnd;
        if (cbData1 > cbData2) {
            trailStart = pStr1 + nCharsMin;
            trailEnd = pStr1 + (cbData1 >> 1);
            rc = 1;
        } else {
            trailStart = pStr2 + nCharsMin;
            trailEnd = pStr2 + (cbData2 >> 1);
            rc = -1;
        }
        for (; trailStart < trailEnd; trailStart++) {
            if (*trailStart != ' ') {
                return rc;
            }
        }
        return 0;
    }
};

class BinaryType : public StoredTypeDescriptor
{
    virtual Ordinal getOrdinal() const
    {
        return STANDARD_TYPE_BINARY;
    }

    virtual uint getBitCount() const
    {
        return 0;
    }

    virtual uint getFixedByteCount() const
    {
        return 0;
    }

    virtual uint getMinByteCount(uint cbMaxWidth) const
    {
        return cbMaxWidth;
    }

    virtual uint getAlignmentByteCount(uint cbWidth) const
    {
        return 1;
    }

    virtual void visitValue(
        DataVisitor &dataVisitor,
        void const *pData,
        TupleStorageByteLength cbData) const
    {
        dataVisitor.visitBytes(pData,cbData);
    }

    virtual int compareValues(
        void const *pData1,
        TupleStorageByteLength cbData1,
        void const *pData2,
        TupleStorageByteLength cbData2) const
    {
        assert(cbData1 == cbData2);
        return memcmp(pData1,pData2,cbData1);
    }
};

class VarBinaryType : public StoredTypeDescriptor
{
    virtual Ordinal getOrdinal() const
    {
        return STANDARD_TYPE_VARBINARY;
    }

    virtual uint getBitCount() const
    {
        return 0;
    }

    virtual uint getFixedByteCount() const
    {
        return 0;
    }

    virtual uint getMinByteCount(uint cbMaxWidth) const
    {
        return 0;
    }

    virtual uint getAlignmentByteCount(uint cbWidth) const
    {
        return 1;
    }

    virtual void visitValue(
        DataVisitor &dataVisitor,
        void const *pData,
        TupleStorageByteLength cbData) const
    {
        dataVisitor.visitBytes(pData,cbData);
    }

    virtual int compareValues(
        void const *pData1,
        TupleStorageByteLength cbData1,
        void const *pData2,
        TupleStorageByteLength cbData2) const
    {
        TupleStorageByteLength cbMin = std::min(cbData1,cbData2);
        int rc = memcmp(pData1, pData2, cbMin);
        if (rc) {
            return rc;
        }
        if (cbData1 == cbData2) {
            return 0;
        }
        if (cbData1 > cbData2) {
            return 1;
        } else {
            return -1;
        }
    }
};

static NumericType<int8_t,STANDARD_TYPE_INT_8> stdINT_8;
static NumericType<uint8_t,STANDARD_TYPE_UINT_8> stdUINT_8;
static NumericType<int16_t,STANDARD_TYPE_INT_16> stdINT_16;
static NumericType<uint16_t,STANDARD_TYPE_UINT_16> stdUINT_16;
static NumericType<int32_t,STANDARD_TYPE_INT_32> stdINT_32;
static NumericType<uint32_t,STANDARD_TYPE_UINT_32> stdUINT_32;
static NumericType<int64_t,STANDARD_TYPE_INT_64> stdINT_64;
static NumericType<uint64_t,STANDARD_TYPE_UINT_64> stdUINT_64;
static NumericType<float,STANDARD_TYPE_REAL> stdREAL;
static NumericType<double,STANDARD_TYPE_DOUBLE> stdDOUBLE;
static NumericType<bool,STANDARD_TYPE_BOOL> stdBOOL;
static CharType stdCHAR;
static VarCharType stdVARCHAR;
static BinaryType stdBINARY;
static VarBinaryType stdVARBINARY;
static UnicodeCharType stdUNICODE_CHAR;
static UnicodeVarCharType stdUNICODE_VARCHAR;

/**
 * NOTE: Any changes must be copied into
 * 1) enum StandardTypeDescriptorOrdinal
 * 2) net.sf.farrago.query.FennelRelUtil.convertSqlTypeNumberToFennelTypeOrdinal
 * 3) StandardTypeDescriptor class
 * 4) StoredTypeDescriptor standardTypes
 */
static StoredTypeDescriptor const *standardTypes[] = {
    NULL,                       // for 0
    &stdINT_8,
    &stdUINT_8,
    &stdINT_16,
    &stdUINT_16,
    &stdINT_32,
    &stdUINT_32,
    &stdINT_64,
    &stdUINT_64,
    &stdBOOL,
    &stdREAL,
    &stdDOUBLE,
    &stdCHAR,
    &stdVARCHAR,
    &stdBINARY,
    &stdVARBINARY,
    &stdUNICODE_CHAR,
    &stdUNICODE_VARCHAR,
};

StandardTypeDescriptorFactory::StandardTypeDescriptorFactory()
{
}

StoredTypeDescriptor const &StandardTypeDescriptorFactory::newDataType(
    StoredTypeDescriptor::Ordinal iTypeOrdinal) const
{
    return *(standardTypes[iTypeOrdinal]);
}

FENNEL_END_CPPFILE("$Id$");

// End StandardTypeDescriptor.cpp
