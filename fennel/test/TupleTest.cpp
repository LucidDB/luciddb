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
#include "fennel/test/TestBase.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/tuple/TuplePrinter.h"
#include "fennel/tuple/AttributeAccessor.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/common/TraceSource.h"

#include <boost/test/test_tools.hpp>
#include <boost/scoped_array.hpp>
#include <limits>

using namespace fennel;

class TupleTest : virtual public TestBase, public TraceSource
{
    static const uint MAX_WIDTH = 512;
    
    TupleDescriptor tupleDesc;
    TupleAccessor tupleAccessor;
    
    void writeMinData(TupleDatum &datum,uint typeOrdinal);
    void writeMaxData(TupleDatum &datum,uint typeOrdinal);
    void writeSampleData(TupleDatum &datum,uint typeOrdinal);
    uint testMarshal(TupleData const &tupleDataFixed);
    void checkData(TupleData const &tupleData1,TupleData const &tupleData2);
        
    void testStandardTypesNullable();
    void testStandardTypesNotNull();
    void testStandardTypesNetworkNullable();
    void testStandardTypesNetworkNotNull();
    void testStandardTypes(TupleFormat,bool nullable);

    void traceTuple(TupleData const &tupleData)
    {
        std::ostringstream oss;
        TuplePrinter tuplePrinter;
        tuplePrinter.print(oss,tupleDesc,tupleData);
        std::string s = oss.str();
        FENNEL_TRACE(TRACE_FINE,s);
    }
    
public:
    explicit TupleTest()
        : TraceSource(this,"TupleTest")
    {
        FENNEL_UNIT_TEST_CASE(TupleTest,testStandardTypesNotNull);
        FENNEL_UNIT_TEST_CASE(TupleTest,testStandardTypesNullable);
        FENNEL_UNIT_TEST_CASE(TupleTest,testStandardTypesNetworkNotNull);
        FENNEL_UNIT_TEST_CASE(TupleTest,testStandardTypesNetworkNullable);
    }
    
    virtual ~TupleTest()
    {
    }
};

void TupleTest::testStandardTypesNullable()
{
    testStandardTypes(TUPLE_FORMAT_STANDARD,true);
}

void TupleTest::testStandardTypesNotNull()
{
    testStandardTypes(TUPLE_FORMAT_STANDARD,false);
}

void TupleTest::testStandardTypesNetworkNullable()
{
    testStandardTypes(TUPLE_FORMAT_NETWORK,true);
}

void TupleTest::testStandardTypesNetworkNotNull()
{
    testStandardTypes(TUPLE_FORMAT_NETWORK,false);
}

void TupleTest::testStandardTypes(
    TupleFormat format,bool nullable)
{
    StandardTypeDescriptorFactory typeFactory;
    uint cbMin = 0;
    tupleDesc.clear();
    for (uint i = STANDARD_TYPE_MIN; i < STANDARD_TYPE_END; ++i) {
        StoredTypeDescriptor const &typeDesc = typeFactory.newDataType(i);
        uint cbFixed = typeDesc.getFixedByteCount();
        if (cbFixed) {
            cbMin += cbFixed;
        } else {
            cbMin += typeDesc.getMinByteCount(MAX_WIDTH);
        }
        tupleDesc.push_back(
            TupleAttributeDescriptor(
                typeDesc,
                nullable,
                cbFixed ? 0 : MAX_WIDTH));
    }

    tupleAccessor.compute(tupleDesc,format);
    BOOST_CHECK(tupleAccessor.getMinByteCount() >= cbMin);
    BOOST_CHECK(tupleAccessor.getMaxByteCount() > cbMin);

    TupleAccessor tupleAccessorFixed;
    tupleAccessorFixed.compute(
        tupleDesc,
        TUPLE_FORMAT_ALL_NOT_NULL_AND_FIXED);

    boost::scoped_array<FixedBuffer> pTupleBufFixed(
        new FixedBuffer[tupleAccessor.getMaxByteCount()]);
    tupleAccessorFixed.setCurrentTupleBuf(pTupleBufFixed.get());
    
    TupleData tupleDataFixed(tupleDesc);
    tupleAccessorFixed.unmarshal(tupleDataFixed);
    
    TupleData::iterator pDatum = tupleDataFixed.begin();
    for (uint i = STANDARD_TYPE_MIN; i < STANDARD_TYPE_END; ++i) {
        writeMinData(*pDatum,i);
        ++pDatum;
    }
    FENNEL_TRACE(TRACE_FINE,"testMarshal(MinData)");
    uint cbMinData = testMarshal(tupleDataFixed);
    BOOST_CHECK(cbMinData >= tupleAccessor.getMinByteCount());
    BOOST_CHECK(cbMinData < tupleAccessor.getMaxByteCount());

    pDatum = tupleDataFixed.begin();
    for (uint i = STANDARD_TYPE_MIN; i < STANDARD_TYPE_END; ++i) {
        writeMaxData(*pDatum,i);
        ++pDatum;
    }
    FENNEL_TRACE(TRACE_FINE,"testMarshal(MaxData)");
    uint cbMaxData = testMarshal(tupleDataFixed);
    BOOST_CHECK(cbMaxData > cbMinData);
    BOOST_CHECK(cbMaxData <= tupleAccessor.getMaxByteCount());
    
    pDatum = tupleDataFixed.begin();
    for (uint i = STANDARD_TYPE_MIN; i < STANDARD_TYPE_END; ++i) {
        writeSampleData(*pDatum,i);
        ++pDatum;
    }
    FENNEL_TRACE(TRACE_FINE,"testMarshal(SampleData)");
    uint cbSampleData = testMarshal(tupleDataFixed);
    BOOST_CHECK(cbSampleData >= tupleAccessor.getMinByteCount());
    BOOST_CHECK(cbSampleData <= tupleAccessor.getMaxByteCount());

    if (nullable) {
        pDatum = tupleDataFixed.begin();
        for (uint i = STANDARD_TYPE_MIN; i < STANDARD_TYPE_END; ++i) {
            pDatum->pData = NULL;
            ++pDatum;
        }
        FENNEL_TRACE(TRACE_FINE,"testMarshal(NullData)");
        uint cbNullData = testMarshal(tupleDataFixed);
        BOOST_CHECK(cbNullData >= tupleAccessor.getMinByteCount());
        BOOST_CHECK(cbNullData < tupleAccessor.getMaxByteCount());
    }
}

uint TupleTest::testMarshal(TupleData const &tupleDataFixed)
{
    FENNEL_TRACE(TRACE_FINE,"reference tuple:");
    traceTuple(tupleDataFixed);
    boost::scoped_array<FixedBuffer> pTupleBufVar( 
        new FixedBuffer[tupleAccessor.getMaxByteCount()]);

    uint cbTuple = tupleAccessor.getByteCount(tupleDataFixed);
    tupleAccessor.marshal(tupleDataFixed,pTupleBufVar.get());
    BOOST_CHECK_EQUAL(cbTuple,tupleAccessor.getCurrentByteCount());

    TupleData tupleDataTogether(tupleDesc);
    tupleAccessor.unmarshal(tupleDataTogether);
    FENNEL_TRACE(TRACE_FINE,"unmarshalled tuple (together):");
    traceTuple(tupleDataTogether);
    BOOST_CHECK_EQUAL(cbTuple,tupleAccessor.getByteCount(tupleDataTogether));
    checkData(tupleDataFixed,tupleDataTogether);
    
    TupleData tupleDataIndividual(tupleDesc);
    for (uint i = 0; i < tupleDataIndividual.size(); ++i) {
        tupleAccessor.getAttributeAccessor(i).unmarshalValue(
            tupleAccessor,tupleDataIndividual[i]);
    }
    FENNEL_TRACE(TRACE_FINE,"unmarshalled tuple (individual):");
    traceTuple(tupleDataIndividual);
    BOOST_CHECK_EQUAL(cbTuple,tupleAccessor.getByteCount(tupleDataIndividual));
    checkData(tupleDataFixed,tupleDataIndividual);

    return tupleAccessor.getCurrentByteCount();
}

void TupleTest::checkData(
    TupleData const &tupleData1,TupleData const &tupleData2)
{
    for (uint i = 0; i < tupleData1.size(); ++i) {
        TupleDatum const &datum1 = tupleData1[i];
        TupleDatum const &datum2 = tupleData2[i];
        if (!datum1.pData || !datum2.pData) {
            BOOST_CHECK_EQUAL(
                static_cast<void const *>(datum1.pData),
                static_cast<void const *>(datum2.pData));
            continue;
        }
        BOOST_CHECK_EQUAL(datum1.cbData,datum2.cbData);
        BOOST_CHECK_EQUAL_COLLECTIONS(
            datum1.pData,
            datum1.pData + datum1.cbData,
            datum2.pData);
    }
}

void TupleTest::writeMinData(TupleDatum &datum,uint typeOrdinal)
{
    PBuffer pData = const_cast<PBuffer>(datum.pData);
    switch(typeOrdinal) {
    case STANDARD_TYPE_BOOL:
        *(reinterpret_cast<bool *>(pData)) = false;
        break;
    case STANDARD_TYPE_INT_8:
        *(reinterpret_cast<int8_t *>(pData)) =
            std::numeric_limits<int8_t>::min();
        break;
    case STANDARD_TYPE_UINT_8:
        *(reinterpret_cast<uint8_t *>(pData)) =
            std::numeric_limits<uint8_t>::min();
        break;
    case STANDARD_TYPE_INT_16:
        *(reinterpret_cast<int16_t *>(pData)) =
            std::numeric_limits<int16_t>::min();
        break;
    case STANDARD_TYPE_UINT_16:
        *(reinterpret_cast<uint16_t *>(pData)) =
            std::numeric_limits<uint16_t>::min();
        break;
    case STANDARD_TYPE_INT_32:
        *(reinterpret_cast<int32_t *>(pData)) =
            std::numeric_limits<int32_t>::min();
        break;
    case STANDARD_TYPE_UINT_32:
        *(reinterpret_cast<uint32_t *>(pData)) =
            std::numeric_limits<uint32_t>::min();
        break;
    case STANDARD_TYPE_INT_64:
        *(reinterpret_cast<int64_t *>(pData)) =
            std::numeric_limits<int64_t>::min();
        break;
    case STANDARD_TYPE_UINT_64:
        *(reinterpret_cast<uint64_t *>(pData)) =
            std::numeric_limits<uint64_t>::min();
        break;
    case STANDARD_TYPE_REAL:
        *(reinterpret_cast<float *>(pData)) =
            std::numeric_limits<float>::min();
        break;
    case STANDARD_TYPE_DOUBLE:
        *(reinterpret_cast<double *>(pData)) =
            std::numeric_limits<double>::min();
        break;
    case STANDARD_TYPE_BINARY:
        memset(pData,0,datum.cbData);
        break;
    case STANDARD_TYPE_CHAR:
        memset(pData,'A',datum.cbData);
        break;
    case STANDARD_TYPE_VARCHAR:
    case STANDARD_TYPE_VARBINARY:
        datum.cbData = 0;
        break;
    default:
        permAssert(false);
    }
}

void TupleTest::writeMaxData(TupleDatum &datum,uint typeOrdinal)
{
    PBuffer pData = const_cast<PBuffer>(datum.pData);
    switch(typeOrdinal) {
    case STANDARD_TYPE_BOOL:
        *(reinterpret_cast<bool *>(pData)) = true;
        break;
    case STANDARD_TYPE_INT_8:
        *(reinterpret_cast<int8_t *>(pData)) =
            std::numeric_limits<int8_t>::max();
        break;
    case STANDARD_TYPE_UINT_8:
        *(reinterpret_cast<uint8_t *>(pData)) =
            std::numeric_limits<uint8_t>::max();
        break;
    case STANDARD_TYPE_INT_16:
        *(reinterpret_cast<int16_t *>(pData)) =
            std::numeric_limits<int16_t>::max();
        break;
    case STANDARD_TYPE_UINT_16:
        *(reinterpret_cast<uint16_t *>(pData)) =
            std::numeric_limits<uint16_t>::max();
        break;
    case STANDARD_TYPE_INT_32:
        *(reinterpret_cast<int32_t *>(pData)) =
            std::numeric_limits<int32_t>::max();
        break;
    case STANDARD_TYPE_UINT_32:
        *(reinterpret_cast<uint32_t *>(pData)) =
            std::numeric_limits<uint32_t>::max();
        break;
    case STANDARD_TYPE_INT_64:
        *(reinterpret_cast<int64_t *>(pData)) =
            std::numeric_limits<int64_t>::max();
        break;
    case STANDARD_TYPE_UINT_64:
        *(reinterpret_cast<uint64_t *>(pData)) =
            std::numeric_limits<uint64_t>::max();
        break;
    case STANDARD_TYPE_REAL:
        *(reinterpret_cast<float *>(pData)) =
            std::numeric_limits<float>::max();
        break;
    case STANDARD_TYPE_DOUBLE:
        *(reinterpret_cast<double *>(pData)) =
            std::numeric_limits<double>::max();
        break;
    case STANDARD_TYPE_BINARY:
        memset(pData,0xFF,datum.cbData);
        break;
    case STANDARD_TYPE_CHAR:
        memset(pData,'z',datum.cbData);
        break;
    case STANDARD_TYPE_VARCHAR:
        datum.cbData = MAX_WIDTH;
        memset(pData,'z',datum.cbData);
        break;
    case STANDARD_TYPE_VARBINARY:
        datum.cbData = MAX_WIDTH;
        memset(pData,0xFF,datum.cbData);
        break;
    default:
        permAssert(false);
    }
}

void TupleTest::writeSampleData(TupleDatum &datum,uint typeOrdinal)
{
    /* Some sample data that's between min and max */
    std::subtractive_rng randomNumberGenerator(time(NULL));
    PBuffer pData = const_cast<PBuffer>(datum.pData);
    switch(typeOrdinal) {
    case STANDARD_TYPE_BOOL:
        if (randomNumberGenerator(2))
            *(reinterpret_cast<bool *>(pData)) = true;
        else *(reinterpret_cast<bool *>(pData)) = false;
        break;
    case STANDARD_TYPE_INT_8:
        *(reinterpret_cast<int8_t *>(pData)) =  0x28;
        break;
    case STANDARD_TYPE_UINT_8:
        *(reinterpret_cast<uint8_t *>(pData)) = 0x54;
        break;
    case STANDARD_TYPE_INT_16:
        *(reinterpret_cast<int16_t *>(pData)) = 0xfedc;
        break;
    case STANDARD_TYPE_UINT_16:
        *(reinterpret_cast<uint16_t *>(pData)) = 0x1234;
        break;
    case STANDARD_TYPE_INT_32:
        *(reinterpret_cast<int32_t *>(pData)) = 0xfedcba98;
        break;
    case STANDARD_TYPE_REAL:
    case STANDARD_TYPE_UINT_32:
        *(reinterpret_cast<uint32_t *>(pData)) = 0x12345678;
        break;
    case STANDARD_TYPE_INT_64:
        *(reinterpret_cast<int64_t *>(pData)) = 0xfedcba0987654321LL;
        break;
    case STANDARD_TYPE_DOUBLE:
    case STANDARD_TYPE_UINT_64:
        *(reinterpret_cast<uint64_t *>(pData)) = 0x1234567890abcdefLL;
        break;
    case STANDARD_TYPE_BINARY:
        for (int i = 0; i < datum.cbData; i++) {
            pData[i] = i % 256;
        }
        break;
    case STANDARD_TYPE_CHAR:
        for (int i = 0; i < datum.cbData; i++) {
            pData[i] = i % ('z' - ' ') + ' ';
        }
        break;
    case STANDARD_TYPE_VARCHAR:
        datum.cbData = randomNumberGenerator(MAX_WIDTH);
        for (int i = 0; i < datum.cbData; i++) {
            pData[i] = i % ('z' - ' ') + ' ';
        }
        break;
    case STANDARD_TYPE_VARBINARY:
        datum.cbData = randomNumberGenerator(MAX_WIDTH);
        for (int i = 0; i < datum.cbData; i++) {
            pData[i] = i % 256;
        }
        break;
    default:
        assert(false);
    }
}

FENNEL_UNIT_TEST_SUITE(TupleTest);

// End TupleTest.cpp

