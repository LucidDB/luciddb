/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
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
#include "fennel/btree/BTreeVerifier.h"
#include "fennel/btree/BTreeReader.h"
#include "fennel/btree/BTreeWriter.h"
#include "fennel/segment/SegInputStream.h"
#include "fennel/segment/SegOutputStream.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/tuple/TupleAccessor.h"

#include <boost/test/test_tools.hpp>

using namespace fennel;

#include <functional>

class BTreeTest : virtual public SegStorageTestBase
{
    enum {
        nRandomSeed = 1000000,
        iValueMax = 1000000000
    };

    enum InsertType {
        MONOTONIC,
        SEQUENTIAL,
        RANDOM
    };

    struct Record 
    {
        int32_t key;
        int32_t secondKey;
        int32_t value;
    };

    BTreeDescriptor descriptor;
    
    TupleAccessor tupleAccessor;
    TupleData keyData;
    TupleData tupleData;
    Record record;
    boost::scoped_array<FixedBuffer> recordBuf;

    int32_t readKey();
    int32_t readSecondKey();
    int32_t readValue();
    int32_t readMultiKeyValue();
    void verifyTree(uint nRecordsExpected,uint nLevelsExpected);
    
    void testBulkLoadOneLevelNewRoot()
    {
        testBulkLoad(200,1,true);
    }
    
    void testBulkLoadOneLevelReuseRoot()
    {
        testBulkLoad(200,1,false);
    }
    
    void testBulkLoadTwoLevelsNewRoot()
    {
        testBulkLoad(20000,2,true);
    }
    
    void testBulkLoadTwoLevelsReuseRoot()
    {
        testBulkLoad(20000,2,false);
    }
    
    void testBulkLoadThreeLevels()
    {
        testBulkLoad(200000,3,true);
    }
    
    void testBulkLoad(uint nRecords,uint nLevelsExpected,bool newRoot);
    void testScan(
        SharedByteInputStream,
        uint nRecords,
        bool alternating,
        bool deletion);
    void testSearch(SharedByteInputStream,uint nRecords,bool leastUpper);
    void testSearchLast();

    void testSequentialInserts()
    {
        testInserts(SEQUENTIAL);
    }

    void testRandomInserts()
    {
        testInserts(RANDOM);
    }

    void testMonotonicInserts()
    {
        testInserts(MONOTONIC);
    }

    void testInserts(InsertType insertType);

    void marshalRecord();
    void marshalMultiKeyRecord();
    void unmarshalRecord(SharedByteInputStream pInputStream);
    
    void testMultiKeySearches();

public:
    explicit BTreeTest()
    {
        FENNEL_UNIT_TEST_CASE(BTreeTest,testBulkLoadOneLevelNewRoot);
        FENNEL_UNIT_TEST_CASE(BTreeTest,testBulkLoadOneLevelReuseRoot);
        FENNEL_UNIT_TEST_CASE(BTreeTest,testBulkLoadTwoLevelsNewRoot);
        FENNEL_UNIT_TEST_CASE(BTreeTest,testBulkLoadTwoLevelsReuseRoot);
        FENNEL_UNIT_TEST_CASE(BTreeTest,testBulkLoadThreeLevels);
        FENNEL_UNIT_TEST_CASE(BTreeTest,testSequentialInserts);
        FENNEL_UNIT_TEST_CASE(BTreeTest,testRandomInserts);
        FENNEL_UNIT_TEST_CASE(BTreeTest,testMonotonicInserts);

        // Since this testcase converts the single key descriptor into a
        // multi-key descriptor, any tests that require the single key
        // descriptor should run before this one.
        FENNEL_UNIT_TEST_CASE(BTreeTest,testMultiKeySearches);
        
        StandardTypeDescriptorFactory stdTypeFactory;
        TupleAttributeDescriptor attrDesc(
            stdTypeFactory.newDataType(STANDARD_TYPE_INT_32));
        descriptor.tupleDescriptor.push_back(attrDesc);
        descriptor.tupleDescriptor.push_back(attrDesc);
        descriptor.keyProjection.push_back(0);
        tupleAccessor.compute(descriptor.tupleDescriptor);
        recordBuf.reset(new FixedBuffer[tupleAccessor.getMaxByteCount()]);
        tupleData.compute(descriptor.tupleDescriptor);
    }

    virtual void testCaseSetUp();
    virtual void testCaseTearDown();
};

void BTreeTest::testCaseSetUp()
{
    openStorage(DeviceMode::createNew);
    openRandomSegment();

    descriptor.segmentAccessor.pSegment = pRandomSegment;
    descriptor.segmentAccessor.pCacheAccessor = pCache;
}

void BTreeTest::testCaseTearDown()
{
    descriptor.segmentAccessor.reset();
    SegStorageTestBase::testCaseTearDown();
}

void BTreeTest::marshalRecord()
{
    tupleData[0].pData = reinterpret_cast<PBuffer>(&record.key);
    tupleData[1].pData = reinterpret_cast<PBuffer>(&record.value);
    tupleAccessor.marshal(tupleData, recordBuf.get());
}

void BTreeTest::marshalMultiKeyRecord()
{
    tupleData[0].pData = reinterpret_cast<PBuffer>(&record.key);
    tupleData[1].pData = reinterpret_cast<PBuffer>(&record.secondKey);
    tupleData[2].pData = reinterpret_cast<PBuffer>(&record.value);
    tupleAccessor.marshal(tupleData, recordBuf.get());
}

void BTreeTest::unmarshalRecord(SharedByteInputStream pInputStream)
{
    PConstBuffer pBuf = pInputStream->getReadPointer(1);
    tupleAccessor.setCurrentTupleBuf(pBuf);
    uint cbTuple = tupleAccessor.getCurrentByteCount();
    tupleAccessor.unmarshal(tupleData);
    record.key = readKey();
    record.value = readValue();
    pInputStream->consumeReadPointer(cbTuple);
}

void BTreeTest::testBulkLoad(uint nRecords,uint nLevelsExpected,bool newRoot)
{
    BlockNum nPagesAllocatedInitially =
        pRandomSegment->getAllocatedSizeInPages();
    
    descriptor.rootPageId = NULL_PAGE_ID;
    BTreeBuilder builder(descriptor,pRandomSegment);
    
    keyData.compute(builder.getKeyDescriptor());
    keyData[0].pData = reinterpret_cast<PConstBuffer>(&record.key);

    // NOTE jvs 15-Nov-2005:  This looks like it's doing the opposite of
    // what it's supposed to, but it's actually correct.  What we're doing
    // here is testing for preservation of the root PageId of an existing
    // truncated index.  This is important for catalogs which store
    // the root PageId as an attribute of an index.  So, newRoot=true
    // means let the builder allocate a new root; newRoot=false means
    // verify that the build preserves the location of the existing root.
    // To verify that, we create an empty root here (simulating the
    // truncation of an existing index before reload).
    if (!newRoot) {
        builder.createEmptyRoot();
        descriptor.rootPageId = builder.getRootPageId();
    }

    // Generate random key/value data
    SegmentAccessor segmentAccessor(pRandomSegment,pCache);
    SharedSegOutputStream pOutputStream =
        SegOutputStream::newSegOutputStream(segmentAccessor);
    std::subtractive_rng randomNumberGenerator(nRandomSeed);
    record.key = 0;
    for (uint i = 0; i < nRecords; i++) {
        // +2 to guarantee holes
        record.key += (randomNumberGenerator(10)) + 2;
        record.value = randomNumberGenerator(iValueMax);

        marshalRecord();
        uint cbTuple = tupleAccessor.getCurrentByteCount();
        PBuffer pBuffer = pOutputStream->getWritePointer(cbTuple);
        memcpy(pBuffer,recordBuf.get(),cbTuple);
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
    descriptor.rootPageId = builder.getRootPageId();

    // Check tree integrity
    verifyTree(nRecords,nLevelsExpected);

    // Make sure we can search for each key individually
    pInputStream->seekSegPos(startPos);
    testSearch(pInputStream,nRecords,true);

    // Do same search, but searching for greatest lower bound during
    // intermediate searches
    pInputStream->seekSegPos(startPos);
    testSearch(pInputStream,nRecords,false);
    
    // Make sure we can scan all tuples
    pInputStream->seekSegPos(startPos);
    testScan(pInputStream,nRecords,false,false);

    // Now delete every other tuple
    pInputStream->seekSegPos(startPos);
    testScan(pInputStream,nRecords,true,true);

    // Recheck tree integriy
    verifyTree(nRecords/2,nLevelsExpected);
    
    // Rescan to make sure deletions were performed correctly
    pInputStream->seekSegPos(startPos);
    testScan(pInputStream,nRecords,true,false);

    // Deallocate the test data storage
    pInputStream->seekSegPos(startPos);
    pInputStream->setDeallocate(true);
    pInputStream.reset();

    // And deallocate the tree's storage
    builder.truncate(true);

    // Make sure there are no leaks
    BOOST_CHECK_EQUAL(
        pRandomSegment->getAllocatedSizeInPages(),
        nPagesAllocatedInitially);
}

void BTreeTest::verifyTree(uint nRecordsExpected,uint nLevelsExpected)
{
    BTreeVerifier verifier(descriptor);
    verifier.verify();

    BTreeStatistics const &stats = verifier.getStatistics();
    BOOST_CHECK_EQUAL(stats.nLevels,nLevelsExpected);
    BOOST_CHECK_EQUAL(stats.nTuples,nRecordsExpected);
}

void BTreeTest::testSearch(
    SharedByteInputStream pInputStream,uint nRecords,bool leastUpper)
{
    BTreeReader reader(descriptor);
    for (uint i = 0; i < nRecords; ++i) {
        unmarshalRecord(pInputStream);
        if (!reader.searchForKey(keyData,DUP_SEEK_ANY,leastUpper)) {
            BOOST_FAIL("LeastUpper:" << leastUpper <<
                       ". Could not find key #" << i << ":  " << record.key);
        }
        reader.getTupleAccessorForRead().unmarshal(tupleData);
        BOOST_CHECK_EQUAL(record.key,readKey());
        BOOST_CHECK_EQUAL(record.value,readValue());
        if (!(i%10000)) {
            BOOST_MESSAGE(
                "found value = " << readValue()
                << " key = " << readKey());
        }
        record.key++;
        if (reader.searchForKey(keyData,DUP_SEEK_ANY)) {
            BOOST_FAIL("Found key " << record.key << " (shouldn't exist!)");
        }
    }
}

void BTreeTest::testScan(
    SharedByteInputStream pInputStream,uint nRecords,
    bool alternating,bool deletion)
{
    BTreeReader realReader(descriptor);

    SegmentAccessor scratchAccessor = pSegmentFactory->newScratchSegment(
        pCache,
        1);

    BTreeWriter writer(descriptor,scratchAccessor);

    bool found;
    int32_t lastKey = -1;
    BTreeReader &reader = deletion ? writer : realReader;
    found = reader.searchFirst();
    if (!found) {
        BOOST_FAIL("searchFirst found nothing");
    }
    if (alternating && !deletion) {
        nRecords /= 2;
    }
    for (uint i = 0; i < nRecords; ++i) {
        unmarshalRecord(pInputStream);
        if (alternating && !deletion) {
            unmarshalRecord(pInputStream);
        }
        if (!found) {
            BOOST_FAIL("Could not searchNext for key #"
                       << i << ":  " << record.key);
        }
        reader.getTupleAccessorForRead().unmarshal(tupleData);
        lastKey = readKey();
        BOOST_CHECK_EQUAL(record.key,lastKey);
        BOOST_CHECK_EQUAL(record.value,readValue());
        if (!(i%10000)) {
            BOOST_MESSAGE(
                "scanned value = " << readValue()
                << " key = " << lastKey);
        }
        if (deletion) {
            if (!alternating || !(i & 1)) {
                writer.deleteCurrent();
            }
        }
        found = reader.searchNext();
    }

    reader.endSearch();
    
    if (!deletion) {
        found = reader.searchLast();
        if (!found) {
            BOOST_FAIL("searchLast found nothing");
        }
        reader.getTupleAccessorForRead().unmarshal(tupleData);
        BOOST_CHECK_EQUAL(lastKey,readKey());
        reader.endSearch();
    }
}

void BTreeTest::testSearchLast()
{
    BTreeReader reader(descriptor);
    bool found = reader.searchLast();
    if (!found) {
        BOOST_FAIL("searchLast found nothing");
    }
}

int32_t BTreeTest::readKey()
{
    return *reinterpret_cast<int32_t const *>(
        tupleData[0].pData);
}

int32_t BTreeTest::readSecondKey()
{
    return *reinterpret_cast<int32_t const *>(
        tupleData[1].pData);
}

int32_t BTreeTest::readValue()
{
    return *reinterpret_cast<int32_t const *>(
        tupleData[1].pData);
}

int32_t BTreeTest::readMultiKeyValue()
{
    return *reinterpret_cast<int32_t const *>(
        tupleData[2].pData);
}

void BTreeTest::testInserts(InsertType insertType)
{
    uint nRecords = 200000;
    descriptor.rootPageId = NULL_PAGE_ID;
    BTreeBuilder builder(descriptor,pRandomSegment);
    
    keyData.compute(builder.getKeyDescriptor());
    keyData[0].pData = reinterpret_cast<PConstBuffer>(&record.key);

    tupleData.compute(descriptor.tupleDescriptor);

    builder.createEmptyRoot();
    descriptor.rootPageId = builder.getRootPageId();

    SegmentAccessor scratchAccessor = pSegmentFactory->newScratchSegment(
        pCache,
        1);

    bool monotonic = (insertType == MONOTONIC);
    BTreeWriter writer(descriptor,scratchAccessor,monotonic);

    std::vector<int32_t> keys;
    for (uint i = 0; i < nRecords; i++) {
        keys.push_back(i);
    }
    if (insertType == RANDOM) {
        std::random_shuffle(keys.begin(), keys.end());
    }

    // insert the records and then read them back to make sure
    // they got inserted
    for (uint i = 0; i < nRecords; i++) {
        record.key = keys[i];
        record.value = -keys[i];
        marshalRecord();
        writer.insertTupleFromBuffer(
            recordBuf.get(), DUP_FAIL);
    }

    BTreeReader reader(descriptor);
    for (uint i = 0; i < nRecords; ++i) {
        record.key = i;
        record.value = -i;
        if (!reader.searchForKey(keyData,DUP_SEEK_ANY)) {
            BOOST_FAIL("Could not find key #" << i << ":  " << record.key);
        }
        reader.getTupleAccessorForRead().unmarshal(tupleData);
        BOOST_CHECK_EQUAL(record.key,readKey());
        BOOST_CHECK_EQUAL(record.value,readValue());
    }
}

void BTreeTest::testMultiKeySearches()
{
    // Add one additional key
    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor attrDesc(
        stdTypeFactory.newDataType(STANDARD_TYPE_INT_32));
    descriptor.tupleDescriptor.push_back(attrDesc);
    descriptor.keyProjection.push_back(1);

    tupleAccessor.compute(descriptor.tupleDescriptor);
    recordBuf.reset(new FixedBuffer[tupleAccessor.getMaxByteCount()]);

    // 800 records should be large enough to create several leaf pages.
    // The first index key will contain 200 distinct values with the second
    // key sequencing from 0-3.
    uint nKey1 = 200;
    uint nKey2 = 4;
    descriptor.rootPageId = NULL_PAGE_ID;
    BTreeBuilder builder(descriptor,pRandomSegment);
    
    tupleData.compute(descriptor.tupleDescriptor);

    builder.createEmptyRoot();
    descriptor.rootPageId = builder.getRootPageId();

    SegmentAccessor scratchAccessor = pSegmentFactory->newScratchSegment(
        pCache,
        1);

    BTreeWriter writer(descriptor,scratchAccessor,false);

    for (uint i = 0; i < nKey1; i++) {
        for (uint j = 0; j < nKey2; j++) {
            record.key = i;
            record.secondKey = j;
            record.value = i + j;
            marshalMultiKeyRecord();
            writer.insertTupleFromBuffer(recordBuf.get(), DUP_FAIL);
        }
    }

    // Search based on only the first key
    TupleDescriptor searchKeyDesc;
    searchKeyDesc.push_back(attrDesc);
    keyData.compute(searchKeyDesc);
    keyData[0].pData = reinterpret_cast<PConstBuffer>(&record.key);

    // First, try searching using DUP_SEEK_BEGIN, then DUP_SEEK_END mode
    BTreeReader reader(descriptor);
    for (uint i = 0; i < nKey1; ++i) {
        record.key = i;

        if (!reader.searchForKey(keyData,DUP_SEEK_BEGIN)) {
            BOOST_FAIL("Could not find begin key #" << i);
        }
        reader.getTupleAccessorForRead().unmarshal(tupleData);
        BOOST_CHECK_EQUAL(i,readKey());
        BOOST_CHECK_EQUAL(0,readSecondKey());
        BOOST_CHECK_EQUAL(i,readMultiKeyValue());

        if (!reader.searchForKey(keyData,DUP_SEEK_END)) {
            BOOST_FAIL("Could not find end key #" << i);
        }
        if (i == nKey1 - 1) {
            if (!reader.isSingular()) {
                BOOST_FAIL(
                    "Should have reached end of tree for end key #" << i);
            }
        } else {
            reader.getTupleAccessorForRead().unmarshal(tupleData);
            BOOST_CHECK_EQUAL(i+1,readKey());
            BOOST_CHECK_EQUAL(0,readSecondKey());
            BOOST_CHECK_EQUAL(i+1,readMultiKeyValue());
        }
    }
    reader.endSearch();

    // Test the scenario where we delete the last key on a leaf page and
    // then reinsert it with a larger second key value forcing it into the 
    // next leaf page.  Since we don't know which key is at the end of a
    // leaf page, delete/reinsert all of them.
    //
    // To make sure we can find the relocated keys, do the following types
    // of search:
    // 1) partial begin search
    // 2) partial end search
    // 3) end search on a non-existent full key

    TupleData multiKeyData;
    multiKeyData.compute(builder.getKeyDescriptor());
    multiKeyData[0].pData = reinterpret_cast<PConstBuffer>(&record.key);
    multiKeyData[1].pData = reinterpret_cast<PConstBuffer>(&record.secondKey);
    for (uint i = 0; i < nKey1; i++) {
        for (uint j = 0; j < nKey2; j++) {
            record.key = i;
            record.secondKey = j;
            if (!writer.searchForKey(multiKeyData,DUP_SEEK_ANY)) {
                BOOST_FAIL("Could not find key #" << i);
            }
            writer.deleteCurrent();

            record.secondKey = j + nKey2;
            record.value = i + j + nKey2;
            marshalMultiKeyRecord();
            writer.insertTupleFromBuffer(recordBuf.get(), DUP_FAIL);
        }
    }
    writer.endSearch();
    for (uint i = 0; i < nKey1; i++) {
        record.key = i;
        if (!reader.searchForKey(keyData,DUP_SEEK_BEGIN)) {
            BOOST_FAIL("Could not find begin key #" << i);
        }
        reader.getTupleAccessorForRead().unmarshal(tupleData);
        BOOST_CHECK_EQUAL(i,readKey());
        BOOST_CHECK_EQUAL(nKey2,readSecondKey());
        BOOST_CHECK_EQUAL(i+nKey2,readMultiKeyValue());

        if (!reader.searchForKey(keyData,DUP_SEEK_END)) {
            BOOST_FAIL("Could not find end key #" << i);
        }
        if (i == nKey1 - 1) {
            if (!reader.isSingular()) {
                BOOST_FAIL(
                    "Should have reached end of tree for end key #" << i);
            }
        } else {
            reader.getTupleAccessorForRead().unmarshal(tupleData);
            BOOST_CHECK_EQUAL(i+1,readKey());
            BOOST_CHECK_EQUAL(nKey2,readSecondKey());
            BOOST_CHECK_EQUAL(i+1+nKey2,readMultiKeyValue());
        }

        record.secondKey = 0;
        if (reader.searchForKey(multiKeyData,DUP_SEEK_END)) {
            BOOST_FAIL("Should not have found multikey #" << i);
        }
        reader.getTupleAccessorForRead().unmarshal(tupleData);
        BOOST_CHECK_EQUAL(i,readKey());
        BOOST_CHECK_EQUAL(nKey2,readSecondKey());
        BOOST_CHECK_EQUAL(i+nKey2,readMultiKeyValue());
    }
}

FENNEL_UNIT_TEST_SUITE(BTreeTest);

// End BTreeTest.cpp
