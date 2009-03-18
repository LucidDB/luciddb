/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
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
#include "fennel/test/TestBase.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/tuple/TuplePrinter.h"
#include "fennel/tuple/AttributeAccessor.h"
#include "fennel/tuple/UnalignedAttributeAccessor.h"
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
    void checkAlignment(
        TupleAttributeDescriptor const &desc, PConstBuffer pBuf);

    void testStandardTypesNullable();
    void testStandardTypesNotNull();
    void testStandardTypesNetworkNullable();
    void testStandardTypesNetworkNotNull();
    void testStandardTypes(TupleFormat,bool nullable);
    void testZeroByteTuple();
    void testDebugAccess();
    void testLoadStoreUnaligned();
    void loadStore8ByteInts(int64_t initialValue, uint8_t nextByte);
    void loadAndStore8ByteInt(int64_t intVal);
    void loadStore2ByteLenData(uint dataLen);
    void loadStoreNullData(uint typeOrdinal, uint len);

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
        : TraceSource(shared_from_this(),"TupleTest")
    {
        FENNEL_UNIT_TEST_CASE(TupleTest,testStandardTypesNotNull);
        FENNEL_UNIT_TEST_CASE(TupleTest,testStandardTypesNullable);
        FENNEL_UNIT_TEST_CASE(TupleTest,testStandardTypesNetworkNotNull);
        FENNEL_UNIT_TEST_CASE(TupleTest,testStandardTypesNetworkNullable);
        FENNEL_UNIT_TEST_CASE(TupleTest,testZeroByteTuple);
        FENNEL_UNIT_TEST_CASE(TupleTest,testLoadStoreUnaligned);

        // This one should fail when TupleAccessor.cpp's DEBUG_TUPLE_ACCESS
        // is set to 1.
        FENNEL_EXTRA_UNIT_TEST_CASE(TupleTest,testDebugAccess);
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
        TUPLE_FORMAT_ALL_FIXED);

    boost::scoped_array<FixedBuffer> pTupleBufFixed(
        new FixedBuffer[tupleAccessor.getMaxByteCount()]);
    tupleAccessorFixed.setCurrentTupleBuf(pTupleBufFixed.get(), false);

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
        checkAlignment(tupleDesc[i], datum1.pData);
        checkAlignment(tupleDesc[i], datum2.pData);
        BOOST_CHECK_EQUAL(datum1.cbData,datum2.cbData);
        BOOST_CHECK_EQUAL_COLLECTIONS(
            datum1.pData,
            datum1.pData + datum1.cbData,
            datum2.pData,
            datum2.pData + datum2.cbData);
    }
}

void TupleTest::checkAlignment(
    TupleAttributeDescriptor const &desc, PConstBuffer pBuf)
{
    uint iAlign = desc.pTypeDescriptor->getAlignmentByteCount(
        desc.cbStorage);
    switch (iAlign) {
    case 1:
        return;
    case 2:
        BOOST_CHECK_EQUAL(0, uintptr_t(pBuf) & 1);
        break;
    case 4:
        BOOST_CHECK_EQUAL(0, uintptr_t(pBuf) & 3);
        break;
    case 8:
        BOOST_CHECK_EQUAL(0, uintptr_t(pBuf) & 7);
        break;
    }
}

void TupleTest::writeMinData(TupleDatum &datum,uint typeOrdinal)
{
    PBuffer pData = const_cast<PBuffer>(datum.pData);
    switch (typeOrdinal) {
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
    case STANDARD_TYPE_UNICODE_CHAR:
        {
            Ucs2Buffer pStr =
                reinterpret_cast<Ucs2Buffer>(pData);
            uint nChars = (datum.cbData >> 1);
            for (uint i = 0; i < nChars; ++i) {
                pStr[i] = 'A';
            }
        }
        break;
    case STANDARD_TYPE_VARCHAR:
    case STANDARD_TYPE_VARBINARY:
    case STANDARD_TYPE_UNICODE_VARCHAR:
        datum.cbData = 0;
        break;
    default:
        permAssert(false);
    }
}

void TupleTest::writeMaxData(TupleDatum &datum,uint typeOrdinal)
{
    PBuffer pData = const_cast<PBuffer>(datum.pData);
    switch (typeOrdinal) {
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
    case STANDARD_TYPE_UNICODE_CHAR:
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
    case STANDARD_TYPE_UNICODE_VARCHAR:
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
    switch (typeOrdinal) {
    case STANDARD_TYPE_BOOL:
        if (randomNumberGenerator(2)) {
            *(reinterpret_cast<bool *>(pData)) = true;
        } else {
            *(reinterpret_cast<bool *>(pData)) = false;
        }
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
    case STANDARD_TYPE_UNICODE_CHAR:
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
    case STANDARD_TYPE_UNICODE_VARCHAR:
    case STANDARD_TYPE_VARBINARY:
        datum.cbData = randomNumberGenerator(MAX_WIDTH);
        if (typeOrdinal == STANDARD_TYPE_UNICODE_VARCHAR) {
            if (datum.cbData & 1) {
                // need an even number of bytes for doublebyte characters
                datum.cbData--;
            }
        }
        for (int i = 0; i < datum.cbData; i++) {
            pData[i] = i % 256;
        }
        break;
    default:
        assert(false);
    }
}

void TupleTest::testDebugAccess()
{
    StandardTypeDescriptorFactory typeFactory;

    // Just to set up tupleAccessor
    testStandardTypesNullable();

    boost::scoped_array<FixedBuffer> buf(
        new FixedBuffer[tupleAccessor.getMaxByteCount()]);
    memset(buf.get(), 0, tupleAccessor.getMaxByteCount());

    // This should cause an assertion failure when TupleAccessor.cpp's
    // DEBUG_TUPLE_ACCESS is set to 1.
    tupleAccessor.setCurrentTupleBuf(buf.get());
}

void TupleTest::testZeroByteTuple()
{
    StandardTypeDescriptorFactory typeFactory;
    tupleDesc.clear();
    tupleDesc.push_back(
        TupleAttributeDescriptor(
            typeFactory.newDataType(STANDARD_TYPE_CHAR),
            false,
            0));
    TupleAccessor tupleAccessor;
    tupleAccessor.compute(tupleDesc);

    // verify that we didn't end up with a 0-byte tuple layout,
    // and that the min and max are equal since it's fixed-width
    BOOST_CHECK(tupleAccessor.getMinByteCount());
    BOOST_CHECK(tupleAccessor.getMaxByteCount());
    BOOST_CHECK_EQUAL(
        tupleAccessor.getMinByteCount(),
        tupleAccessor.getMaxByteCount());
}

void TupleTest::testLoadStoreUnaligned()
{
    // test compression of 8-byte integers
    loadStore8ByteInts(0, 0xff);
    loadStore8ByteInts(0x80, 0);

    // make sure zero is handled correctly
    loadAndStore8ByteInt(0);

    // test data that requires a 2-byte storage length
    loadStore2ByteLenData(128);
    loadStore2ByteLenData(129);
    loadStore2ByteLenData(255);
    loadStore2ByteLenData(256);
    loadStore2ByteLenData(257);
    loadStore2ByteLenData(510);
    loadStore2ByteLenData(511);
    loadStore2ByteLenData(512);

    // test special case of empty string
    loadStore2ByteLenData(0);

    // test null data
    loadStoreNullData(STANDARD_TYPE_INT_64, 8);
    loadStoreNullData(STANDARD_TYPE_INT_32, 4);

    // test fixed width data
    TupleDatum tupleDatum;
    tupleDatum.cbData = 2;
    int16_t intVal = 43981;
    tupleDatum.pData = (PConstBuffer) &intVal;
    FixedBuffer storageBuf[4];
    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor attrDesc_int16(
        stdTypeFactory.newDataType(STANDARD_TYPE_INT_16));
    UnalignedAttributeAccessor accessor_int16(attrDesc_int16);
    accessor_int16.storeValue(tupleDatum, storageBuf);
    uint len = accessor_int16.getStoredByteCount(storageBuf);
    BOOST_REQUIRE(len == 2);

    FixedBuffer loadBuf[4];
    tupleDatum.cbData = 0xff;
    tupleDatum.pData = loadBuf;
    accessor_int16.loadValue(tupleDatum, storageBuf);

    BOOST_REQUIRE(tupleDatum.cbData == 2);
    bool rc = (intVal == *reinterpret_cast<int16_t const *> (tupleDatum.pData));
    BOOST_REQUIRE(rc);

    // test data with 1-byte storage length
    tupleDatum.cbData = 3;
    FixedBuffer data[3];
    data[0] = 0xba;
    data[0] = 0xdc;
    data[0] = 0xfe;
    tupleDatum.pData = data;
    TupleAttributeDescriptor attrDesc_varBinary(
        stdTypeFactory.newDataType(STANDARD_TYPE_VARBINARY),
        true,
        4);
    UnalignedAttributeAccessor accessor_varBinary(attrDesc_varBinary);
    accessor_varBinary.storeValue(tupleDatum, storageBuf);
    len = accessor_varBinary.getStoredByteCount(storageBuf);
    BOOST_REQUIRE(len == 4);

    tupleDatum.cbData = 0xff;
    tupleDatum.pData = loadBuf;
    accessor_varBinary.loadValue(tupleDatum, storageBuf);

    BOOST_REQUIRE(tupleDatum.cbData == 3);
    BOOST_REQUIRE(memcmp(tupleDatum.pData, data, 3) == 0);
}

void TupleTest::loadStore8ByteInts(int64_t initialValue, uint8_t nextByte)
{
    // Take the intial value, shift it to the left and OR the nextByte value
    // to generate the different test values.  Do this 8 times.  For each
    // value, try the value, value - 1, value + 1, as well as the negative of
    // each of those three values.

    int64_t intVal = initialValue;
    for (int i = 0; i < 8; i++) {
        intVal <<= 8;
        intVal |= nextByte;

        intVal--;
        loadAndStore8ByteInt(intVal);
        loadAndStore8ByteInt(-intVal);

        intVal++;
        loadAndStore8ByteInt(intVal);
        loadAndStore8ByteInt(-intVal);

        intVal++;
        loadAndStore8ByteInt(intVal);
        loadAndStore8ByteInt(-intVal);

        intVal--;
    }
}

void TupleTest::loadAndStore8ByteInt(int64_t intVal)
{
    TupleDatum tupleDatum;
    // need 8+1 bytes in buffer; +1 for the length byte
    FixedBuffer storageBuf[9];
    FixedBuffer loadBuf[9];

    // compress and uncompress various 8-byte integers; verify the data
    // by storing the original value, loading the stored value and then
    // checking that it's the same as the original value
    tupleDatum.cbData = 8;
    tupleDatum.pData = (PConstBuffer) &intVal;
    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor attrDesc(
        stdTypeFactory.newDataType(STANDARD_TYPE_INT_64));
    UnalignedAttributeAccessor accessor(attrDesc);
    accessor.storeValue(tupleDatum, storageBuf);

    // load the data into a different buffer so we're sure we're not reusing
    // the original stored value
    tupleDatum.cbData = 0;
    tupleDatum.pData = loadBuf;
    accessor.loadValue(tupleDatum, storageBuf);
    bool rc = (intVal == *reinterpret_cast<int64_t const *> (tupleDatum.pData));
    BOOST_REQUIRE(rc);
    BOOST_REQUIRE(tupleDatum.cbData == 8);
}

void TupleTest::loadStore2ByteLenData(uint dataLen)
{
    // initialize the source TupleDatum
    TupleDatum tupleDatum;
    tupleDatum.cbData = dataLen;
    boost::scoped_array<FixedBuffer> dataBuf(new FixedBuffer[dataLen + 2]);
    for (int i = 0; i < dataLen; i++) {
        dataBuf[i] = i;
    }
    tupleDatum.pData = dataBuf.get();

    // store it and verify the storage length
    boost::scoped_array<FixedBuffer> storageBuf(new FixedBuffer[dataLen + 2]);
    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor attrDesc(
        stdTypeFactory.newDataType(STANDARD_TYPE_BINARY), true, dataLen);
    UnalignedAttributeAccessor accessor(attrDesc);
    accessor.storeValue(tupleDatum, storageBuf.get());
    uint len = accessor.getStoredByteCount(storageBuf.get());
    BOOST_REQUIRE(len == dataLen + 2);

    // load the stored value and compare it against the original data buffer
    boost::scoped_array<FixedBuffer> loadBuf(new FixedBuffer[dataLen + 2]);
    tupleDatum.cbData = 0;
    tupleDatum.pData = loadBuf.get();
    accessor.loadValue(tupleDatum, storageBuf.get());
    BOOST_REQUIRE(tupleDatum.cbData == dataLen);
    BOOST_REQUIRE(memcmp(tupleDatum.pData, dataBuf.get(), dataLen) == 0);
}

void TupleTest::loadStoreNullData(uint typeOrdinal, uint dataLen)
{
    FixedBuffer storageBuf[2];
    TupleDatum tupleDatum;
    tupleDatum.cbData = 0;
    tupleDatum.pData = 0;

    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor attrDesc(
        stdTypeFactory.newDataType(typeOrdinal),
        true,
        dataLen);
    UnalignedAttributeAccessor accessor(attrDesc);

    accessor.storeValue(tupleDatum, storageBuf);
    uint len = accessor.getStoredByteCount(storageBuf);
    BOOST_REQUIRE(len == 1);

    FixedBuffer loadBuf[2];
    tupleDatum.cbData = 0xff;
    tupleDatum.pData = loadBuf;
    accessor.loadValue(tupleDatum, storageBuf);

    BOOST_REQUIRE(tupleDatum.pData == NULL);
}

FENNEL_UNIT_TEST_SUITE(TupleTest);

// End TupleTest.cpp

