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

class TestBTree : virtual public SegStorageTestBase
{
    enum {
        nRandomSeed = 1000000,
        iValueMax = 1000000000
    };

    // NOTE:  this matches the Fennel Tuple format, so it can be used
    // directly for input into BTreeBuilder
    struct Record 
    {
        int32_t key;
        int32_t value;
    };

    BTreeDescriptor descriptor;
    
    TupleData keyData;
    TupleData tupleData;
    Record record;

    int32_t readKey();
    int32_t readValue();
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
    void testSearch(SharedByteInputStream,uint nRecords);
    
public:
    explicit TestBTree()
    {
        FENNEL_UNIT_TEST_CASE(TestBTree,testBulkLoadOneLevelNewRoot);
        FENNEL_UNIT_TEST_CASE(TestBTree,testBulkLoadOneLevelReuseRoot);
        FENNEL_UNIT_TEST_CASE(TestBTree,testBulkLoadTwoLevelsNewRoot);
        FENNEL_UNIT_TEST_CASE(TestBTree,testBulkLoadTwoLevelsReuseRoot);
        FENNEL_UNIT_TEST_CASE(TestBTree,testBulkLoadThreeLevels);
        
        StandardTypeDescriptorFactory stdTypeFactory;
        TupleAttributeDescriptor attrDesc(
            stdTypeFactory.newDataType(STANDARD_TYPE_INT_32));
        descriptor.tupleDescriptor.push_back(attrDesc);
        descriptor.tupleDescriptor.push_back(attrDesc);
        descriptor.keyProjection.push_back(0);
    }

    virtual void testCaseSetUp();
    virtual void testCaseTearDown();
};

void TestBTree::testCaseSetUp()
{
    openStorage(DeviceMode::createNew);
    
    // reopen will interpret pages as already allocated
    closeStorage();
    openStorage(DeviceMode::load);

    pRandomSegment = pSegmentFactory->newRandomAllocationSegment(
        pLinearSegment,true);
    pLinearSegment.reset();

    descriptor.segmentAccessor.pSegment = pRandomSegment;
    descriptor.segmentAccessor.pCacheAccessor = pCache;
}

void TestBTree::testCaseTearDown()
{
    descriptor.segmentAccessor.reset();
    SegStorageTestBase::testCaseTearDown();
}

void TestBTree::testBulkLoad(uint nRecords,uint nLevelsExpected,bool newRoot)
{
    BlockNum nPagesAllocatedInitially =
        pRandomSegment->getAllocatedSizeInPages();
    
    descriptor.rootPageId = NULL_PAGE_ID;
    BTreeBuilder builder(descriptor,pRandomSegment);
    
    keyData.compute(builder.getKeyDescriptor());
    keyData[0].pData = reinterpret_cast<PConstBuffer>(&record.key);

    tupleData.compute(descriptor.tupleDescriptor);

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

        // NOTE:  don't use pOutputStream->writeValue(record) since
        // BTreeBuilder expects contiguous tuples
        PBuffer pBuffer = pOutputStream->getWritePointer(sizeof(record));
        memcpy(pBuffer,&record,sizeof(record));
        pOutputStream->consumeWritePointer(sizeof(record));
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
    testSearch(pInputStream,nRecords);
    
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

void TestBTree::verifyTree(uint nRecordsExpected,uint nLevelsExpected)
{
    BTreeVerifier verifier(descriptor);
    verifier.verify();

    BTreeStatistics const &stats = verifier.getStatistics();
    BOOST_CHECK_EQUAL(stats.nLevels,nLevelsExpected);
    BOOST_CHECK_EQUAL(stats.nTuples,nRecordsExpected);
}

void TestBTree::testSearch(
    SharedByteInputStream pInputStream,uint nRecords)
{
    BTreeReader reader(descriptor);
    for (uint i = 0; i < nRecords; ++i) {
        pInputStream->readValue(record);
        if (!reader.searchForKey(keyData,DUP_SEEK_ANY)) {
            BOOST_FAIL("Could not find key #" << i << ":  " << record.key);
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

void TestBTree::testScan(
    SharedByteInputStream pInputStream,uint nRecords,
    bool alternating,bool deletion)
{
    BTreeReader realReader(descriptor);

    SegmentAccessor scratchAccessor = pSegmentFactory->newScratchSegment(
        pCache,
        1);

    BTreeWriter writer(descriptor,scratchAccessor);
    
    BTreeReader &reader = deletion ? writer : realReader;
    bool found = reader.searchFirst();
    if (!found) {
        BOOST_FAIL("searchFirst found nothing");
    }
    if (alternating && !deletion) {
        nRecords /= 2;
    }
    for (uint i = 0; i < nRecords; ++i) {
        pInputStream->readValue(record);
        if (alternating && !deletion) {
            pInputStream->readValue(record);
        }
        if (!found) {
            BOOST_FAIL("Could not searchNext for key #"
                       << i << ":  " << record.key);
        }
        reader.getTupleAccessorForRead().unmarshal(tupleData);
        BOOST_CHECK_EQUAL(record.key,readKey());
        BOOST_CHECK_EQUAL(record.value,readValue());
        if (!(i%10000)) {
            BOOST_MESSAGE(
                "scanned value = " << readValue()
                << " key = " << readKey());
        }
        if (deletion) {
            if (!alternating || !(i & 1)) {
                writer.deleteCurrent();
            }
        }
        found = reader.searchNext();
    }
}

int32_t TestBTree::readKey()
{
    return *reinterpret_cast<int32_t const *>(
        tupleData[0].pData);
}

int32_t TestBTree::readValue()
{
    return *reinterpret_cast<int32_t const *>(
        tupleData[1].pData);
}

FENNEL_UNIT_TEST_SUITE(TestBTree);

// End TestBTree.cpp
