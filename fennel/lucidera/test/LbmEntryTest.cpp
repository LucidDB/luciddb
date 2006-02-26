/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2006-2006 LucidEra, Inc.
// Copyright (C) 2006-2006 The Eigenbase Project
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
#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/lucidera/bitmap/LbmEntry.h"
#include "fennel/cache/Cache.h"
#include <stdarg.h>

#include <boost/test/test_tools.hpp>

using namespace fennel;

struct LbmEntryList
{
    LbmEntry entry;
    PBuffer pBuf;
    TupleData entryTuple;
};

enum EntryType {
    SBM_ADJ,            // 0 - single bitmap, adjacent to previous
    SGT_ADJ,            // 1 - singleton, adjacent to previous
    CMP_ADJ,            // 2 - compressed, adjacent to previous
    SBM_NADJ,           // 3 - single bitmap, non-adjacent to previous
    SGT_NADJ,           // 4 - singleton, non-adjacent to previous
    CMP_NADJ,           // 5 - compressed, non-adjacent to previous
    SBM_OVLP,           // 6 - single bitmap, overlaps previous
    SGT_OVLP,           // 7 - singleton, overlaps previous
    CMP_OVLP            // 8 - compressed, overlaps previous
};

/**
 * Testcase for LbmEntry class
 */
class LbmEntryTest : virtual public SegStorageTestBase
{
    uint bitmapColSize;
    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor attrDesc_bitmap;
    TupleAttributeDescriptor attrDesc_int64;
    TupleDescriptor entryTupleDesc;
    uint bufSize;
    uint bufUsed;
    PBuffer pBuf;
    TupleData entryTuple;

    void testRandom(uint nUniqueKeys, uint nRows, uint scratchBufferSize);

    /**
     * Main test routine for exercising mergeEntry.
     *
     * @param vector of rid values to construct bitmaps for
     * @param nRidsPerBitmap vector of values indicating how many rids
     * to include in each bitmap
     * @param scratchBufferSize size of scratch buffer used to construct
     * bitmaps; size only indicates space to be used by bitmaps
     */
    void testMergeEntry(
        std::vector<LcsRid> const &ridValues,
        std::vector<uint> const &nRidsPerBitmap, uint scratchBufferSize);

    void newLbmEntry(
        std::vector<LbmEntryList> &entryList, LcsRid startRid,
        SegPageLock &bufferLock);

    bool compareExpected(
        LbmEntry *entry, std::vector<LcsRid> const &ridValues, uint &ridPos);

    void recurseCombos(uint curr, uint nEntries, std::vector<uint> &eTypes);

    void generateBitmaps(
        EntryType etype, std::vector<LcsRid> &ridValues,
        std::vector<uint> &nRidsPerBitmap);

    void generateSingleBitmaps(
        std::vector<LcsRid> &ridValues, std::vector<uint> &nRidsPerBitmap,
        LcsRid startRid, uint nRids);

    void generateCompressedBitmaps(
        std::vector<LcsRid> &ridValues, std::vector<uint> &nRidsPerBitmap,
        LcsRid startRid, uint nRids);

public:
    explicit LbmEntryTest()
    {
        FENNEL_UNIT_TEST_CASE(LbmEntryTest, testRandom1);
        FENNEL_UNIT_TEST_CASE(LbmEntryTest, testRandom2);
        FENNEL_UNIT_TEST_CASE(LbmEntryTest, testRandom3);
        FENNEL_UNIT_TEST_CASE(LbmEntryTest, testRandom4);
        FENNEL_UNIT_TEST_CASE(LbmEntryTest, testRandom5);
        FENNEL_UNIT_TEST_CASE(LbmEntryTest, testCombos);
        FENNEL_UNIT_TEST_CASE(LbmEntryTest, testldb35);
    }

    virtual void testCaseSetUp();
    virtual void testCaseTearDown();

    void testRandom1();
    void testRandom2();
    void testRandom3();
    void testRandom4();
    void testRandom5();
    void testCombos();
    void testldb35();
};

void LbmEntryTest::testRandom1()
{
    uint nUniqueKeys = 10;
    uint nRows = 1000;
    uint scratchBufferSize = 40;

    testRandom(nUniqueKeys, nRows, scratchBufferSize);
}

void LbmEntryTest::testRandom2()
{
    uint nUniqueKeys = 30;
    uint nRows = 2000;
    uint scratchBufferSize = 50;

    testRandom(nUniqueKeys, nRows, scratchBufferSize);
}

void LbmEntryTest::testRandom3()
{
    uint nUniqueKeys = 25;
    uint nRows = 1500;
    uint scratchBufferSize = 30;

    testRandom(nUniqueKeys, nRows, scratchBufferSize);
}

void LbmEntryTest::testRandom4()
{
    uint nUniqueKeys = 20;
    uint nRows = 2500;
    uint scratchBufferSize = 35;

    testRandom(nUniqueKeys, nRows, scratchBufferSize);
}

void LbmEntryTest::testRandom5()
{
    uint nUniqueKeys = 15;
    uint nRows = 3000;
    uint scratchBufferSize = 45;

    testRandom(nUniqueKeys, nRows, scratchBufferSize);
}

void LbmEntryTest::testRandom(
    uint nUniqueKeys, uint nRows, uint scratchBufferSize)
{
    std::vector<LcsRid> ridValues;
    std::vector<uint> nRidsPerBitmap;
    uint totalRids = nRows/nUniqueKeys;

    assert(scratchBufferSize >= 8);

    // generate random rid values, ensuring that they are unique, then
    // sort them
    LcsRid rid = LcsRid(rand() % nRows);
    ridValues.push_back(rid);
    for (uint i = 1; i < totalRids; ) {
        rid = LcsRid(rand() % nRows);
        uint j;
        for (j = 0; j < i; j++) {
            if (rid == ridValues[j]) {
                break;
            }
        }
        if (j >= i) {
            ridValues.push_back(rid);
            i++;
            continue;
        }
    }
    // sort the rid values
    std::sort(ridValues.begin(), ridValues.end());
#if 0
    std::cout << "Rid Values After Sort" << std::endl;
    for (uint i = 0; i < ridValues.size(); i++) {
        std::cout << ridValues[i] << std::endl;
    }
#endif

    // generate random values that will determine how many rids
    // there are per bitmap
    uint ridCount = 0;
    while (ridCount < totalRids) {
        // divide by 4 so we'll later merge roughly 2-4 bitmaps per
        // entry
        uint nRids = rand() % (scratchBufferSize/4) + 1;
        if (nRids + ridCount < totalRids) {
            nRidsPerBitmap.push_back(nRids);
            ridCount += nRids;
        } else {
            nRidsPerBitmap.push_back(totalRids - ridCount);
            break;
        }
    }

    testMergeEntry(ridValues, nRidsPerBitmap, scratchBufferSize);
}

void LbmEntryTest::testMergeEntry(
    std::vector<LcsRid> const &ridValues,
    std::vector<uint> const &nRidsPerBitmap, uint scratchBufferSize)
{
    std::vector<LbmEntryList> entryList;

    // bitmap entries will be created without a leading key, so the
    // first column is the startRid
    // initialize scratch accessor for scratch buffers used to
    // construct bitmaps
    SegmentAccessor scratchAccessor =
        pSegmentFactory->newScratchSegment(pCache, 10);
    bufSize = scratchAccessor.pSegment->getUsablePageSize();
    SegPageLock bufferLock;
    bufferLock.accessSegment(scratchAccessor);
    bufUsed = bufSize;

    bitmapColSize = scratchBufferSize;
    attrDesc_bitmap =
        TupleAttributeDescriptor(
            stdTypeFactory.newDataType(STANDARD_TYPE_VARBINARY),
            true, bitmapColSize);
    attrDesc_int64 =
        TupleAttributeDescriptor(
            stdTypeFactory.newDataType(STANDARD_TYPE_INT_64));

    entryTupleDesc.push_back(attrDesc_int64);
    entryTupleDesc.push_back(attrDesc_bitmap);
    entryTupleDesc.push_back(attrDesc_bitmap);
    entryTuple.compute(entryTupleDesc);
    entryTuple[1].pData = NULL;
    entryTuple[2].pData = NULL;

    // Generate bitmaps using the list of rid values. The number of rids
    // in each bitmap is based on the list of nRidsPerBitmap.

    uint totalRids = ridValues.size();
    uint nRidPos = 0;
    for (uint i = 0; i < totalRids; ) {

        newLbmEntry(entryList, ridValues[i], bufferLock);
        i++;

        for (uint j = 1; i < totalRids && j < nRidsPerBitmap[nRidPos];
            i++, j++)
        {
            if (!entryList.back().entry.setRID(ridValues[i])) {
                newLbmEntry(entryList, ridValues[i], bufferLock);
            }
        }
        nRidPos++;
        // keep adding rids that are within the prior byte range
        LcsRid endRid =
            LbmSegment::roundToByteBoundary(ridValues[i-1]) +
                LbmSegment::LbmOneByteSize;
        while (i < totalRids && ridValues[i] < endRid) {
            if (!entryList.back().entry.setRID(ridValues[i])) {
                newLbmEntry(entryList, ridValues[i], bufferLock);
            }
            i++;
        }
    }

    // produce the tuples corresponding to each LbmEntry
    for (uint i = 0; i < entryList.size(); i++) {
        entryList[i].entryTuple = entryList[i].entry.produceEntryTuple();
    }

#if 0
    std::cout << "Generated Entries Before Merge" << std::endl;
    for (uint i = 0; i < entryList.size(); i++) {
        std::cout << LbmEntry::toString(entryList[i].entryTuple, true)
            << std::endl;
    }
#endif

    // merge the entries constructed
    uint ridPos = 0;
    LbmEntry *pEntry = &entryList[0].entry;
    pEntry->setEntryTuple(entryList[0].entryTuple);
    for (uint i = 1; i < entryList.size(); i++) {
        if (pEntry->mergeEntry(entryList[i].entryTuple)) {
            continue;
        }
        // not able to merge, so need to produce entry and compare against
        // expected rids before starting to merge on the next entry
        bool rc = compareExpected(pEntry, ridValues, ridPos);
        if (!rc) {
            BOOST_REQUIRE(rc);
        }
        pEntry = &entryList[i].entry;
        if (i < entryList.size() - 1) {
            pEntry->setEntryTuple(entryList[i].entryTuple);
        }
    }
    // if this is the last remaining entry, compare it against
    // expected rid values
    if (ridPos < totalRids) {
        bool rc = compareExpected(pEntry, ridValues, ridPos);
        if (!rc) {
            BOOST_REQUIRE(rc);
        }
    }
    scratchAccessor.pSegment->deallocatePageRange(NULL_PAGE_ID, NULL_PAGE_ID);
}

void LbmEntryTest::newLbmEntry(
    std::vector<LbmEntryList> &entryList, LcsRid startRid,
    SegPageLock &bufferLock)
{
    LbmEntryList listElement;

    // allocate a new scratch page if not enough space is left on
    // the current
    if (bufUsed + bitmapColSize + sizeof(LcsRid) > bufSize) {
        bufferLock.allocatePage();
        pBuf = bufferLock.getPage().getWritableData();
        bufferLock.unlock();
        bufUsed = 0;
    }

    listElement.pBuf = pBuf + bufUsed;
    bufUsed += bitmapColSize + sizeof(LcsRid);
    listElement.entry.init(
        listElement.pBuf, bitmapColSize + sizeof(LcsRid), entryTupleDesc);
    entryList.push_back(listElement);
    entryTuple[0].pData = (PConstBuffer) &startRid;
    entryList.back().entry.setEntryTuple(entryTuple);
}

bool LbmEntryTest::compareExpected(
    LbmEntry *generatedEntry, std::vector<LcsRid> const &ridValues,
    uint &ridPos)
{
    TupleData const &generatedTuple = generatedEntry->produceEntryTuple();
    std::vector<LcsRid> expectedRids;
    LbmEntry::generateRIDs(generatedTuple, expectedRids);

    LcsRid endRid;
    uint rowCount = generatedEntry->getRowCount();
    if (rowCount == 1) {
        // singleton
        endRid = ridValues[ridPos] + 1;
    } else {
        endRid =
            LbmSegment::roundToByteBoundary(ridValues[ridPos]) + rowCount;
    }

    uint i;
    for (i = 0;
        ridPos < ridValues.size() && ridValues[ridPos] < endRid &&
            i < expectedRids.size();
        i++, ridPos++)
    {
        if (ridValues[ridPos] != expectedRids[i]) {
            break;
        }
    }
#if 0
    std::cout << "Generated Entry:" << LbmEntry::toString(generatedTuple, true)
        << std::endl;
#endif
    if (i < expectedRids.size()) {
        std::cout << "Mismatch in rid.  Actual = " << expectedRids[i] <<
            ", Expected = " << ridValues[ridPos] << std::endl;
        return false;
    }
    return true;
}

void LbmEntryTest::testldb35()
{
    std::vector<LbmEntryList> entryList;
    std::vector<LcsRid> ridValues;

    // bitmap entries will be created without a leading key, so the
    // first column is the startRid
    // initialize scratch accessor for scratch buffers used to
    // construct bitmaps
    SegmentAccessor scratchAccessor =
        pSegmentFactory->newScratchSegment(pCache, 10);
    bufSize = scratchAccessor.pSegment->getUsablePageSize();
    SegPageLock bufferLock;
    bufferLock.accessSegment(scratchAccessor);
    bufferLock.allocatePage();
    pBuf = bufferLock.getPage().getWritableData();
    bufferLock.unlock();
    bufUsed = 0;

    bitmapColSize = 16;
    attrDesc_bitmap =
        TupleAttributeDescriptor(
            stdTypeFactory.newDataType(STANDARD_TYPE_VARBINARY),
            true, bitmapColSize);
    attrDesc_int64 =
        TupleAttributeDescriptor(
            stdTypeFactory.newDataType(STANDARD_TYPE_INT_64));

    entryTupleDesc.push_back(attrDesc_int64);
    entryTupleDesc.push_back(attrDesc_bitmap);
    entryTupleDesc.push_back(attrDesc_bitmap);
    entryTuple.compute(entryTupleDesc);

    LbmEntryList listElement;

    // first entry -- single bitmap with rid 98601
    
    listElement.pBuf = pBuf + bufUsed;
    bufUsed += bitmapColSize + sizeof(LcsRid);
    listElement.entry.init(
        listElement.pBuf, bitmapColSize + sizeof(LcsRid), entryTupleDesc);
    entryList.push_back(listElement);
    LcsRid rid = LcsRid(98061);
    ridValues.push_back(rid);
    LcsRid roundedRid = LbmSegment::roundToByteBoundary(rid);
    entryTuple[0].pData = (PConstBuffer) &roundedRid;
    entryTuple[1].pData = NULL;
    uint8_t byte =
        (uint8_t) (1 << (opaqueToInt(rid) % LbmSegment::LbmOneByteSize));
    entryTuple[2].pData = &byte;
    entryTuple[2].cbData = 1;
    entryList.back().entry.setEntryTuple(entryTuple);

    // second entry -- compressed bitmap with only rid 98070 set

    listElement.pBuf = pBuf + bufUsed;
    bufUsed += bitmapColSize + sizeof(LcsRid);
    listElement.entry.init(
        listElement.pBuf, bitmapColSize + sizeof(LcsRid), entryTupleDesc);
    entryList.push_back(listElement);
    rid = LcsRid(98070);
    ridValues.push_back(rid);
    roundedRid = LbmSegment::roundToByteBoundary(rid);
    entryTuple[0].pData = (PConstBuffer) &roundedRid;
    uint8_t twoBytes[2];
    twoBytes[0] = 0xd;
    twoBytes[1] = 0xf0;
    entryTuple[1].pData = twoBytes;
    entryTuple[1].cbData = 2;
    byte = (uint8_t) (1 << (opaqueToInt(rid) % LbmSegment::LbmOneByteSize));
    entryTuple[2].pData = &byte;
    entryTuple[2].cbData = 1;
    entryList.back().entry.setEntryTuple(entryTuple);

    // third entry -- compressed bitmap with only rid 99992 set

    listElement.pBuf = pBuf + bufUsed;
    bufUsed += bitmapColSize + sizeof(LcsRid);
    listElement.entry.init(
        listElement.pBuf, bitmapColSize + sizeof(LcsRid), entryTupleDesc);
    entryList.push_back(listElement);
    rid = LcsRid(99992);
    ridValues.push_back(rid);
    roundedRid = LbmSegment::roundToByteBoundary(rid);
    entryTuple[0].pData = (PConstBuffer) &roundedRid;
    twoBytes[0] = 0xd;
    twoBytes[1] = 0x10;
    entryTuple[1].pData = twoBytes;
    entryTuple[1].cbData = 2;
    byte = (uint8_t) (1 << (opaqueToInt(rid) % LbmSegment::LbmOneByteSize));
    entryTuple[2].pData = &byte;
    entryTuple[2].cbData = 1;
    entryList.back().entry.setEntryTuple(entryTuple);

    // fourth entry -- singleton with rid 100133

    listElement.pBuf = pBuf + bufUsed;
    bufUsed += bitmapColSize + sizeof(LcsRid);
    listElement.entry.init(
        listElement.pBuf, bitmapColSize + sizeof(LcsRid), entryTupleDesc);
    entryList.push_back(listElement);
    rid = LcsRid(100133);
    ridValues.push_back(rid);
    entryTuple[0].pData = (PConstBuffer) &rid;
    entryTuple[1].pData = NULL;
    entryTuple[2].pData = NULL;
    entryList.back().entry.setEntryTuple(entryTuple);

    // fifth entry -- singleton with rid 100792

    listElement.pBuf = pBuf + bufUsed;
    bufUsed += bitmapColSize + sizeof(LcsRid);
    listElement.entry.init(
        listElement.pBuf, bitmapColSize + sizeof(LcsRid), entryTupleDesc);
    entryList.push_back(listElement);
    rid = LcsRid(100792);
    ridValues.push_back(rid);
    entryTuple[0].pData = (PConstBuffer) &rid;
    entryTuple[1].pData = NULL;
    entryTuple[2].pData = NULL;
    entryList.back().entry.setEntryTuple(entryTuple);

    // produce the tuples corresponding to each LbmEntry
    for (uint i = 0; i < entryList.size(); i++) {
        entryList[i].entryTuple = entryList[i].entry.produceEntryTuple();
    }

    uint ridPos = 0;
    LbmEntry *pEntry = &entryList[0].entry;
    pEntry->setEntryTuple(entryList[0].entryTuple);
    for (uint i = 1; i < entryList.size(); i++) {
        if (pEntry->mergeEntry(entryList[i].entryTuple)) {
            continue;
        }
        // not able to merge, so need to produce entry and compare against
        // expected rids before starting to merge on the next entry
        bool rc = compareExpected(pEntry, ridValues, ridPos);
        if (!rc) {
            BOOST_REQUIRE(rc);
        }
        pEntry = &entryList[i].entry;
        if (i < entryList.size() - 1) {
            pEntry->setEntryTuple(entryList[i].entryTuple);
        }
    }
    // if this is the last remaining entry, compare it against
    // expected rid values
    if (ridPos < ridValues.size()) {
        bool rc = compareExpected(pEntry, ridValues, ridPos);
        if (!rc) {
            BOOST_REQUIRE(rc);
        }
    }
    scratchAccessor.pSegment->deallocatePageRange(NULL_PAGE_ID, NULL_PAGE_ID);
}

void LbmEntryTest::testCombos()
{
    uint nEntries = 5;
    std::vector<uint> eTypes;
    
    for (uint i = 0; i < nEntries; i++) {
        eTypes.push_back(0);
    }
    recurseCombos(0, nEntries, eTypes);
}

void LbmEntryTest::recurseCombos(
    uint curr, uint nEntries, std::vector<uint> &eTypes)
{
    std::vector<LcsRid> ridValues;
    std::vector<uint> nRidsPerBitmap;

    uint nETypes = (curr == 0) ? 3 : 9;
    for (uint i = 0; i < nETypes; i++) {
        eTypes[curr] = i;
        if (curr < nEntries - 1) {
            recurseCombos(curr + 1, nEntries, eTypes);
        } else {

            // generate the "nEntries" bitmaps
            for (uint n = 0; n < nEntries; n++) {
                generateBitmaps(
                    EntryType(eTypes[n]), ridValues, nRidsPerBitmap);
            }

            // generate the last entry
            ridValues.push_back(ridValues.back() + 16);
            nRidsPerBitmap.push_back(1);

            // merge them
            testMergeEntry(ridValues, nRidsPerBitmap, nEntries * 24);

            ridValues.clear();
            nRidsPerBitmap.clear();
            entryTupleDesc.clear();
            entryTuple.clear();
        }
    }
}

void LbmEntryTest::generateBitmaps(
    EntryType etype, std::vector<LcsRid> &ridValues,
    std::vector<uint> &nRidsPerBitmap)
{
    LcsRid prev;

    if (ridValues.size() == 0) {
        prev = LcsRid(0);
    } else {
        prev = ridValues.back();
    }

    switch (etype) {
    case SBM_ADJ:
        generateSingleBitmaps(
            ridValues, nRidsPerBitmap, prev + LbmSegment::LbmOneByteSize, 4);
        break;
    case SBM_NADJ:
        generateSingleBitmaps(
            ridValues, nRidsPerBitmap, prev + LbmSegment::LbmOneByteSize*2, 4);
        break;
    case SBM_OVLP:
        generateSingleBitmaps(ridValues, nRidsPerBitmap, prev + 2, 4);
        break;
    case SGT_ADJ:
        generateSingleBitmaps(
            ridValues, nRidsPerBitmap, prev + LbmSegment::LbmOneByteSize, 1);
        break;
    case SGT_NADJ:
        generateSingleBitmaps(
            ridValues, nRidsPerBitmap, prev + LbmSegment::LbmOneByteSize*2, 1);
        break;
    case SGT_OVLP:
        generateSingleBitmaps(ridValues, nRidsPerBitmap, prev + 2, 1);
        break;
    case CMP_ADJ:
        generateCompressedBitmaps(
            ridValues, nRidsPerBitmap, prev + LbmSegment::LbmOneByteSize, 10);
        break;
    case CMP_NADJ:
        generateCompressedBitmaps(
            ridValues, nRidsPerBitmap, prev + LbmSegment::LbmOneByteSize*2, 10);
        break;
    case CMP_OVLP:
        generateCompressedBitmaps(ridValues, nRidsPerBitmap, prev + 2, 10);
        break;
    }
}

void LbmEntryTest::generateSingleBitmaps(
    std::vector<LcsRid> &ridValues, std::vector<uint> &nRidsPerBitmap,
    LcsRid startRid, uint nRids)
{
    for (uint i = 0;  i < nRids; i++) {
        ridValues.push_back(startRid);
        startRid += LbmSegment::LbmOneByteSize;
    }
    nRidsPerBitmap.push_back(nRids);
}

void LbmEntryTest::generateCompressedBitmaps(
    std::vector<LcsRid> &ridValues, std::vector<uint> &nRidsPerBitmap,
    LcsRid startRid, uint nRids)
{
    for (uint i = 0; i < nRids; i++) {
        ridValues.push_back(startRid);
        startRid += LbmSegment::LbmOneByteSize*2;
    }
    nRidsPerBitmap.push_back(nRids);
}

void LbmEntryTest::testCaseSetUp()
{
    openStorage(DeviceMode::createNew);
}

void LbmEntryTest::testCaseTearDown()
{
    SegStorageTestBase::testCaseTearDown();
    entryTupleDesc.clear();
}

FENNEL_UNIT_TEST_SUITE(LbmEntryTest);


// End LbmEntryTest.cpp
