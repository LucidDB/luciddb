/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
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
    
    virtual uint getAlignmentByteCount(uint) const
    {
        return sizeof(T);
    }

    virtual void visitValue(
        DataVisitor &dataVisitor,
        void const *pData,
        uint cbData) const
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
        uint cbData1,
        void const *pData2,
        uint cbData2) const
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
    uint cbData) const
{
    double d = *static_cast<double const *>(pData);
    assert(cbData == sizeof(double));
    dataVisitor.visitDouble(d);
}

template<>
void NumericType<float,STANDARD_TYPE_REAL>::visitValue(
    DataVisitor &dataVisitor,
    void const *pData,
    uint cbData) const
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
    
    virtual uint getAlignmentByteCount(uint) const
    {
        return 1;
    }

    virtual void visitValue(
        DataVisitor &dataVisitor,
        void const *pData,
        uint cbData) const
    {
        char const *pStr = static_cast<char const *>(pData);
        dataVisitor.visitChars(pStr,cbData);
    }

    virtual int compareValues(
        void const *pData1,
        uint cbData1,
        void const *pData2,
        uint cbData2) const
    {
        assert(cbData1 == cbData2);
        // REVIEW jvs:  should be using strncmp here and below?
        return memcmp(pData1,pData2,cbData1);
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
    
    virtual uint getMinByteCount(uint) const
    {
        return 0;
    }
    
    virtual uint getAlignmentByteCount(uint) const
    {
        return 1;
    }

    virtual void visitValue(
        DataVisitor &dataVisitor,
        void const *pData,
        uint cbData) const
    {
        char const *pStr = static_cast<char const *>(pData);
        dataVisitor.visitChars(pStr,cbData);
    }

    virtual int compareValues(
        void const *pData1,
        uint cbData1,
        void const *pData2,
        uint cbData2) const
    {
        uint cbMin = std::min(cbData1,cbData2);
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
    
    virtual uint getAlignmentByteCount(uint) const
    {
        return 1;
    }

    virtual void visitValue(
        DataVisitor &dataVisitor,
        void const *pData,
        uint cbData) const
    {
        dataVisitor.visitBytes(pData,cbData);
    }

    virtual int compareValues(
        void const *pData1,
        uint cbData1,
        void const *pData2,
        uint cbData2) const
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
    
    virtual uint getMinByteCount(uint) const
    {
        return 0;
    }
    
    virtual uint getAlignmentByteCount(uint) const
    {
        return 1;
    }

    virtual void visitValue(
        DataVisitor &dataVisitor,
        void const *pData,
        uint cbData) const
    {
        dataVisitor.visitBytes(pData,cbData);
    }

    virtual int compareValues(
        void const *pData1,
        uint cbData1,
        void const *pData2,
        uint cbData2) const
    {
        uint cbMin = std::min(cbData1,cbData2);
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
