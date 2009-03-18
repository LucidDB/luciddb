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
#include "fennel/test/SegStorageTestBase.h"
#include "fennel/btree/BTreeBuilder.h"
#include "fennel/btree/BTreeNonLeafReader.h"
#include "fennel/btree/BTreeLeafReader.h"
#include "fennel/segment/SegInputStream.h"
#include "fennel/segment/SegOutputStream.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/tuple/TupleAccessor.h"

#include <boost/scoped_array.hpp>
#include <boost/test/test_tools.hpp>

using namespace fennel;

#include <functional>

/**
 * This test unit tests the BTreeNonLeafReader and BTreeLeafReader classes.
 */
class BTreeReadersTest : virtual public SegStorageTestBase
{
    enum {
        nRandomSeed = 1000000,
        iValueMax = 1000000000
    };

    struct LeafRecord
    {
        int32_t key;
        int32_t value;
    };

    BTreeDescriptor treeDescriptor;
    TupleDescriptor nonLeafDescriptor;

    TupleAccessor leafTupleAccessor;
    TupleData keyData;
    TupleData leafTupleData;
    TupleData nonLeafTupleData;
    LeafRecord leafRecord;
    boost::scoped_array<FixedBuffer> nonLeafRecordBuf;
    boost::scoped_array<FixedBuffer> leafRecordBuf;

    int32_t readLeafKey();
    int32_t readNonLeafKey();
    PageId readPageId();
    int32_t readLeafValue();

    void testOneLevel()
    {
        testReaders(200);
    }

    void testTwoLevels()
    {
        testReaders(20000);
    }

    void testThreeLevels()
    {
        testReaders(200000);
    }

    void testReaders(uint nRecords);
    void testScan(SharedByteInputStream, uint nRecords);
    void testSearch(SharedByteInputStream, uint nRecords);

    void marshalLeafRecord();
    void unmarshalLeafRecord(SharedByteInputStream pInputStream);

public:
    explicit BTreeReadersTest()
    {
        FENNEL_UNIT_TEST_CASE(BTreeReadersTest,testOneLevel);
        FENNEL_UNIT_TEST_CASE(BTreeReadersTest,testTwoLevels);
        FENNEL_UNIT_TEST_CASE(BTreeReadersTest,testThreeLevels);

        StandardTypeDescriptorFactory stdTypeFactory;
        TupleAttributeDescriptor attrDesc(
            stdTypeFactory.newDataType(STANDARD_TYPE_INT_32));
        treeDescriptor.tupleDescriptor.push_back(attrDesc);
        treeDescriptor.tupleDescriptor.push_back(attrDesc);
        treeDescriptor.keyProjection.push_back(0);
        leafTupleAccessor.compute(treeDescriptor.tupleDescriptor);
        leafRecordBuf.reset(
            new FixedBuffer[leafTupleAccessor.getMaxByteCount()]);
        leafTupleData.compute(treeDescriptor.tupleDescriptor);

        nonLeafDescriptor.push_back(attrDesc);
        TupleAttributeDescriptor pageIdDesc(
            stdTypeFactory.newDataType(STANDARD_TYPE_UINT_64));
        nonLeafDescriptor.push_back(pageIdDesc);
        nonLeafTupleData.compute(nonLeafDescriptor);
    }

    virtual void testCaseSetUp();
    virtual void testCaseTearDown();
};

void BTreeReadersTest::testCaseSetUp()
{
    openStorage(DeviceMode::createNew);
    openRandomSegment();

    treeDescriptor.segmentAccessor.pSegment = pRandomSegment;
    treeDescriptor.segmentAccessor.pCacheAccessor = pCache;
}

void BTreeReadersTest::testCaseTearDown()
{
    treeDescriptor.segmentAccessor.reset();
    SegStorageTestBase::testCaseTearDown();
}

void BTreeReadersTest::marshalLeafRecord()
{
    leafTupleData[0].pData = reinterpret_cast<PBuffer>(&leafRecord.key);
    leafTupleData[1].pData = reinterpret_cast<PBuffer>(&leafRecord.value);
    leafTupleAccessor.marshal(leafTupleData, leafRecordBuf.get());
}

void BTreeReadersTest::unmarshalLeafRecord(SharedByteInputStream pInputStream)
{
    PConstBuffer pBuf = pInputStream->getReadPointer(1);
    leafTupleAccessor.setCurrentTupleBuf(pBuf);
    uint cbTuple = leafTupleAccessor.getCurrentByteCount();
    leafTupleAccessor.unmarshal(leafTupleData);
    leafRecord.key = readLeafKey();
    leafRecord.value = readLeafValue();
    pInputStream->consumeReadPointer(cbTuple);
}

void BTreeReadersTest::testReaders(uint nRecords)
{
    // Load the btree with random values

    treeDescriptor.rootPageId = NULL_PAGE_ID;
    BTreeBuilder builder(treeDescriptor, pRandomSegment);

    keyData.compute(builder.getKeyDescriptor());
    keyData[0].pData = reinterpret_cast<PConstBuffer>(&leafRecord.key);

    builder.createEmptyRoot();
    treeDescriptor.rootPageId = builder.getRootPageId();

    // Generate random key/value data
    SegmentAccessor segmentAccessor(pRandomSegment,pCache);
    SharedSegOutputStream pOutputStream =
        SegOutputStream::newSegOutputStream(segmentAccessor);
    std::subtractive_rng randomNumberGenerator(nRandomSeed);
    leafRecord.key = 0;
    for (uint i = 0; i < nRecords; i++) {
        // +2 to guarantee holes
        leafRecord.key += (randomNumberGenerator(10)) + 2;
        leafRecord.value = randomNumberGenerator(iValueMax);

        marshalLeafRecord();
        uint cbTuple = leafTupleAccessor.getCurrentByteCount();
        PBuffer pBuffer = pOutputStream->getWritePointer(cbTuple);
        memcpy(pBuffer,leafRecordBuf.get(),cbTuple);
        pOutputStream->consumeWritePointer(cbTuple);
    }
    PageId pageId = pOutputStream->getFirstPageId();
    pOutputStream.reset();
    SharedSegInputStream pInputStream =
        SegInputStream::newSegInputStream(segmentAccessor,pageId);
    SegStreamPosition startPos;
    pInputStream->getSegPos(startPos);

    // Load the data into the tree
    builder.build(*pInputStream,nRecords,1.0);
    treeDescriptor.rootPageId = builder.getRootPageId();

    // Make sure we can search for each key individually
    pInputStream->seekSegPos(startPos);
    testSearch(pInputStream,nRecords);

    // Make sure we can scan all tuples
    pInputStream->seekSegPos(startPos);
    testScan(pInputStream,nRecords);

    // Deallocate the test data storage
    pInputStream->seekSegPos(startPos);
    pInputStream->setDeallocate(true);
    pInputStream.reset();

    // And deallocate the tree's storage
    builder.truncate(true);
}

void BTreeReadersTest::testSearch(
    SharedByteInputStream pInputStream,
    uint nRecords)
{
    // Search for each key in the btree using both leaf and non-leaf readers

    BTreeNonLeafReader nonLeafReader(treeDescriptor);
    BTreeLeafReader leafReader(treeDescriptor);
    for (uint i = 0; i < nRecords; ++i) {
        // First search for the leaf pageId containing the desired key value
        // using the nonLeafReader, provided this is a multi-level tree.
        // If not, then directly search the leaf.
        unmarshalLeafRecord(pInputStream);

        PageId leafPageId;
        if (nonLeafReader.isRootOnly()) {
            leafPageId = nonLeafReader.getRootPageId();
        } else {
            nonLeafReader.searchForKey(keyData, DUP_SEEK_ANY);
            BOOST_CHECK(!nonLeafReader.isSingular());
            nonLeafReader.getTupleAccessorForRead().unmarshal(nonLeafTupleData);
            if (!nonLeafReader.isPositionedOnInfinityKey() &&
                readNonLeafKey() < leafRecord.key)
            {
                BOOST_FAIL(
                    "Non-leaf key is less than expected key.  Expected key = "
                    << leafRecord.key << ".  Key read = " << readNonLeafKey());
            }
            leafPageId = readPageId();
        }

        // Use the leafReader to locate the key in the leaf page
        leafReader.setCurrentPageId(leafPageId);
        if (!leafReader.searchForKey(keyData, DUP_SEEK_ANY)) {
            BOOST_FAIL(
                "Couldn't locate key " << leafRecord.key << " in leaf page");
        }
        leafReader.getTupleAccessorForRead().unmarshal(leafTupleData);
        BOOST_CHECK_EQUAL(leafRecord.key, readLeafKey());
        BOOST_CHECK_EQUAL(leafRecord.value, readLeafValue());
    }
}

void BTreeReadersTest::testScan(
    SharedByteInputStream pInputStream,
    uint nRecords)
{
    // Read records from both leaf and non-leaf pages starting from left
    // to right.

    BTreeNonLeafReader nonLeafReader(treeDescriptor);
    BTreeLeafReader leafReader(treeDescriptor);

    // Position to the first record in the non-leaf page, just above the leaf
    // level.
    bool found;
    int32_t lastKey = -1;
    bool rootOnly = nonLeafReader.isRootOnly();
    if (!rootOnly) {
        found = nonLeafReader.searchFirst();
        if (!found) {
            BOOST_FAIL("searchFirst on non-leaf found nothing");
        }
    }

    for (uint i = 0; i < nRecords;) {
        unmarshalLeafRecord(pInputStream);
        PageId leafPageId;
        if (rootOnly) {
            leafPageId = nonLeafReader.getRootPageId();
        } else {
            // Read the non-leaf record to locate the leaf pageId.
            nonLeafReader.getTupleAccessorForRead().unmarshal(nonLeafTupleData);
            if (!nonLeafReader.isPositionedOnInfinityKey() &&
                readNonLeafKey() < leafRecord.key)
            {
                BOOST_FAIL(
                    "Non-leaf key is less than expected key.  Expected key = "
                    << leafRecord.key << ".  Key read = " << readNonLeafKey());
            }
            leafPageId = readPageId();
        }
        leafReader.setCurrentPageId(leafPageId);

        // Position to the start of that leaf page.
        found = leafReader.searchFirst();
        if (!found) {
            BOOST_FAIL("searchFirst on leaf found nothing");
        }

        // Iterate over each record in the leaf page until we hit the
        // end of the page.
        while (true) {
            leafReader.getTupleAccessorForRead().unmarshal(leafTupleData);
            lastKey = readLeafKey();
            BOOST_CHECK_EQUAL(leafRecord.key, lastKey);
            BOOST_CHECK_EQUAL(leafRecord.value, readLeafValue());
            i++;
            if (i == nRecords) {
                break;
            }
            found = leafReader.searchNext();
            if (!found) {
                break;
            }
            unmarshalLeafRecord(pInputStream);
        }

        // Check that the last key on the leaf matches the last one read
        // in the loop above.
        found = leafReader.searchLast();
        if (!found) {
            BOOST_FAIL("seachLast on leaf found nothing");
        }
        leafReader.getTupleAccessorForRead().unmarshal(leafTupleData);
        BOOST_CHECK_EQUAL(lastKey, readLeafKey());
        leafReader.endSearch();

        if (i == nRecords) {
            break;
        }

        if (!rootOnly) {
            // Position to the next record in the non-leaf page, then loop back
            // and repeat.
            found = nonLeafReader.searchNext();
            if (!found) {
                BOOST_FAIL("searchNext on non-leaf found nothing");
            }
        }
    }

    nonLeafReader.endSearch();
}

int32_t BTreeReadersTest::readLeafKey()
{
    return *reinterpret_cast<int32_t const *>(
        leafTupleData[0].pData);
}

int32_t BTreeReadersTest::readLeafValue()
{
    return *reinterpret_cast<int32_t const *>(
        leafTupleData[1].pData);
}

int32_t BTreeReadersTest::readNonLeafKey()
{
    return *reinterpret_cast<int32_t const *>(
        nonLeafTupleData[0].pData);
}

PageId BTreeReadersTest::readPageId()
{
    return *reinterpret_cast<PageId const *>(
        nonLeafTupleData[1].pData);
}

FENNEL_UNIT_TEST_SUITE(BTreeReadersTest);

// End BTreeReadersTest.cpp
