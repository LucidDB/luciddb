/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2006-2007 LucidEra, Inc.
// Copyright (C) 2006-2007 The Eigenbase Project
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
#include <hash_set>
#include <math.h>

#include <boost/scoped_array.hpp>
#include <boost/test/test_tools.hpp>

using namespace fennel;

struct LbmEntryInfo
{
    LbmEntry entry;
    PBuffer pBuf;
    TupleData entryTuple;
};

typedef boost::shared_ptr<LbmEntryInfo> SharedLbmEntryInfo;

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
     * @param ridValues vector of rid values to construct bitmaps for
     * @param nRidsPerBitmap vector of values indicating how many rids
     * to include in each bitmap
     * @param scratchBufferSize size of scratch buffer used to construct
     * bitmaps; size only indicates space to be used by bitmaps
     */
    void testMergeEntry(
        std::vector<LcsRid> const &ridValues,
        std::vector<uint> const &nRidsPerBitmap, uint scratchBufferSize);

    void newLbmEntry(
        std::vector<SharedLbmEntryInfo> &entryList, LcsRid startRid,
        SegPageLock &bufferLock);

    PBuffer allocateBuf(SegPageLock &bufferLock);

    bool compareExpected(
        LbmEntry &entry, std::vector<LcsRid> const &ridValues, uint &ridPos,
        bool testContains);

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

    /**
     * Main test routine for merging arbitrary singleton entries
     *
     * @param scratchBufferSize size of scratch buffer used to merge entry
     * @param ridValues initial rid values followed by the singleton rids that
     * will be merged
     * @param nSingletons number of singleton entries to be merged
     * @param split true if splits are allowed during merges
     */
    void testMergeSingleton(
        uint scratchBufferSize, const std::vector<LcsRid> &ridValues,
        uint nSingletons, bool split);

    void testMergeSingletonRandom(uint totalRids, uint ridRange);

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
        FENNEL_UNIT_TEST_CASE(LbmEntryTest, testler5920);
        FENNEL_UNIT_TEST_CASE(LbmEntryTest, testZeroBytes);

        FENNEL_UNIT_TEST_CASE(LbmEntryTest, testMergeSingletonInFront1);
        FENNEL_UNIT_TEST_CASE(LbmEntryTest, testMergeSingletonInFront2);
        FENNEL_UNIT_TEST_CASE(LbmEntryTest, testMergeSingletonMidSegment1);
        FENNEL_UNIT_TEST_CASE(LbmEntryTest, testMergeSingletonMidSegment2);
        FENNEL_UNIT_TEST_CASE(LbmEntryTest, testMergeSingletonMidSegment3);
        FENNEL_UNIT_TEST_CASE(LbmEntryTest, testMergeSingletonMidSegment4);
        FENNEL_UNIT_TEST_CASE(LbmEntryTest, testMergeSingletonEndSegment);
        FENNEL_UNIT_TEST_CASE(LbmEntryTest, testMergeSingletonStartSegment1);
        FENNEL_UNIT_TEST_CASE(LbmEntryTest, testMergeSingletonStartSegment2);
        FENNEL_UNIT_TEST_CASE(
            LbmEntryTest, testMergeSingletonInBetweenSegment);
        FENNEL_UNIT_TEST_CASE(LbmEntryTest, testMergeSingletonSplitLeft1);
        FENNEL_UNIT_TEST_CASE(LbmEntryTest, testMergeSingletonSplitLeft2);
        FENNEL_UNIT_TEST_CASE(LbmEntryTest, testMergeSingletonSplitRight1);
        FENNEL_UNIT_TEST_CASE(LbmEntryTest, testMergeSingletonSplitRight2);
        FENNEL_UNIT_TEST_CASE(LbmEntryTest, testMergeSingletonSplitHalf);
        FENNEL_UNIT_TEST_CASE(LbmEntryTest, testMergeSingletonSplitLast);
        FENNEL_UNIT_TEST_CASE(LbmEntryTest, testMergeSingletonSplitMaxSegments);
        FENNEL_UNIT_TEST_CASE(LbmEntryTest, testMergeSingletonZeros1);
        FENNEL_UNIT_TEST_CASE(LbmEntryTest, testMergeSingletonZeros2);
        FENNEL_UNIT_TEST_CASE(LbmEntryTest, testMergeSingletonZeros3);
        FENNEL_UNIT_TEST_CASE(LbmEntryTest, testMergeSingletonZeros4);
        FENNEL_UNIT_TEST_CASE(LbmEntryTest, testMergeSingletonAfterSplit);
        FENNEL_UNIT_TEST_CASE(LbmEntryTest, testMergeSingletonCombine);
        FENNEL_UNIT_TEST_CASE(LbmEntryTest, testMergeSingletonMaxSeg);
        FENNEL_UNIT_TEST_CASE(LbmEntryTest, testMergeSingletonRandom1);
        FENNEL_UNIT_TEST_CASE(LbmEntryTest, testMergeSingletonRandom2);
        FENNEL_UNIT_TEST_CASE(LbmEntryTest, testMergeSingletonWithSingleton1);
        FENNEL_UNIT_TEST_CASE(LbmEntryTest, testMergeSingletonWithSingleton2);
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
    void testler5920();
    void testZeroBytes();
    void testMergeSingletonInFront1();
    void testMergeSingletonInFront2();
    void testMergeSingletonMidSegment1();
    void testMergeSingletonMidSegment2();
    void testMergeSingletonMidSegment3();
    void testMergeSingletonMidSegment4();
    void testMergeSingletonEndSegment();
    void testMergeSingletonStartSegment1();
    void testMergeSingletonStartSegment2();
    void testMergeSingletonInBetweenSegment();
    void testMergeSingletonSplitLeft1();
    void testMergeSingletonSplitLeft2();
    void testMergeSingletonSplitRight1();
    void testMergeSingletonSplitRight2();
    void testMergeSingletonSplitHalf();
    void testMergeSingletonSplitLast();
    void testMergeSingletonSplitMaxSegments();
    void testMergeSingletonZeros1();
    void testMergeSingletonZeros2();
    void testMergeSingletonZeros3();
    void testMergeSingletonZeros4();
    void testMergeSingletonAfterSplit();
    void testMergeSingletonCombine();
    void testMergeSingletonMaxSeg();
    void testMergeSingletonRandom1();
    void testMergeSingletonRandom2();
    void testMergeSingletonWithSingleton1();
    void testMergeSingletonWithSingleton2();
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
    std::hash_set<uint64_t> ridsGenerated;
    for (uint i = 0; i < totalRids; ) {
        uint64_t rid = rand() % nRows;
        if (ridsGenerated.find(rid) == ridsGenerated.end()) {
            ridValues.push_back(LcsRid(rid));
            ridsGenerated.insert(rid);
            i++;
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
    std::vector<SharedLbmEntryInfo> entryList;

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
            if (!entryList.back()->entry.setRID(ridValues[i])) {
                newLbmEntry(entryList, ridValues[i], bufferLock);
            }
        }
        nRidPos++;
    }

    // produce the tuples corresponding to each LbmEntry
    for (uint i = 0; i < entryList.size(); i++) {
        entryList[i]->entryTuple = entryList[i]->entry.produceEntryTuple();
    }

#if 0
    std::cout << "Generated Entries Before Merge" << std::endl;
    for (uint i = 0; i < entryList.size(); i++) {
        std::cout << LbmEntry::toString(entryList[i]->entryTuple, true)
            << std::endl;
    }
#endif

    // merge the entries constructed
    uint ridPos = 0;

    PBuffer mergeBuf = allocateBuf(bufferLock);
    LbmEntry mEntry;
    mEntry.init(
        mergeBuf, NULL, LbmEntry::getScratchBufferSize(bitmapColSize),
        entryTupleDesc);
    mEntry.setEntryTuple(entryList[0]->entryTuple);
    for (uint i = 1; i < entryList.size(); i++) {
        if (mEntry.mergeEntry(entryList[i]->entryTuple)) {
            continue;
        }
        // not able to merge, so need to produce entry and compare against
        // expected rids before starting to merge on the next entry
        bool rc = compareExpected(mEntry, ridValues, ridPos, true);
        BOOST_REQUIRE(rc);
        mEntry.setEntryTuple(entryList[i]->entryTuple);
    }
    // if this is the last remaining entry, compare it against
    // expected rid values
    if (ridPos < totalRids) {
        bool rc = compareExpected(mEntry, ridValues, ridPos, true);
        BOOST_REQUIRE(rc);
    }

    BOOST_REQUIRE(ridPos == totalRids);

    scratchAccessor.pSegment->deallocatePageRange(NULL_PAGE_ID, NULL_PAGE_ID);
}

void LbmEntryTest::newLbmEntry(
    std::vector<SharedLbmEntryInfo> &entryList, LcsRid startRid,
    SegPageLock &bufferLock)
{
    SharedLbmEntryInfo pListElement = SharedLbmEntryInfo(new LbmEntryInfo());

    pListElement->pBuf = allocateBuf(bufferLock);
    pListElement->entry.init(
        pListElement->pBuf, NULL, LbmEntry::getScratchBufferSize(bitmapColSize),
        entryTupleDesc);
    entryList.push_back(pListElement);
    entryTuple[0].pData = (PConstBuffer) &startRid;
    entryList.back()->entry.setEntryTuple(entryTuple);
}

PBuffer LbmEntryTest::allocateBuf(SegPageLock &bufferLock)
{
    // allocate a new scratch page if not enough space is left on
    // the current
    uint scratchBufSize = LbmEntry::getScratchBufferSize(bitmapColSize);
    if (bufUsed + scratchBufSize > bufSize) {
        bufferLock.allocatePage();
        pBuf = bufferLock.getPage().getWritableData();
        bufferLock.unlock();
        bufUsed = 0;
    }
    PBuffer retBuf = pBuf + bufUsed;
    bufUsed += scratchBufSize;
    return retBuf;
}

bool LbmEntryTest::compareExpected(
    LbmEntry &generatedEntry, std::vector<LcsRid> const &expectedRids,
    uint &ridPos, bool testContains)
{
    TupleData const &generatedTuple = generatedEntry.produceEntryTuple();
    std::vector<LcsRid> actualRids;
    LbmEntry::generateRIDs(generatedTuple, actualRids);

    LcsRid endRid;
    uint rowCount = generatedEntry.getRowCount();
    if (rowCount == 1) {
        // singleton
        endRid = expectedRids[ridPos] + 1;
    } else {
        endRid =
            LbmSegment::roundToByteBoundary(expectedRids[ridPos]) + rowCount;
    }

    uint i;
    for (i = 0;
        ridPos < expectedRids.size() && expectedRids[ridPos] < endRid &&
            i < actualRids.size();
        i++, ridPos++)
    {
        if (expectedRids[ridPos] != actualRids[i]) {
            break;
        }
        // the two if blocks below are redundant but are there to test the
        // containsRid() method; the first tests the positive case and the
        // second the negative
        if (!generatedEntry.containsRid(expectedRids[ridPos])) {
            std::cout << "Positive containsRid check failed on rid = " <<
                expectedRids[ridPos] << std::endl;
            return false;
        }
        // search for the rids in between the current and next; these should
        // not be set in the entry
        if (testContains) {
            if (ridPos + 1 < expectedRids.size()) {
                for (LcsRid nextRid = expectedRids[ridPos] + 1;
                    nextRid < expectedRids[ridPos + 1]; nextRid++)
                {
                    if (generatedEntry.containsRid(nextRid)) {
                        std::cout << "Negative containsRid check failed" <<
                           " on rid = " << nextRid << std::endl;
                        return false;
                    }
                }
            }
        }
    }
#if 0
    std::cout << "Generated Entry:" << LbmEntry::toString(generatedTuple, true)
        << std::endl;
#endif
    if (i < actualRids.size()) {
        std::cout << "Mismatch in rid.  Actual = " << actualRids[i] <<
            ", Expected = " << expectedRids[ridPos] << std::endl;
        return false;
    }
    return true;
}

void LbmEntryTest::testldb35()
{
    std::vector<SharedLbmEntryInfo> entryList;
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
    bufUsed = bufSize;

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

    SharedLbmEntryInfo pListElement = SharedLbmEntryInfo(new LbmEntryInfo());

    // first entry -- single bitmap with rid 98601
    
    pListElement->pBuf = allocateBuf(bufferLock);
    pListElement->entry.init(
        pListElement->pBuf, NULL, LbmEntry::getScratchBufferSize(bitmapColSize),
        entryTupleDesc);
    entryList.push_back(pListElement);
    LcsRid rid = LcsRid(98061);
    ridValues.push_back(rid);
    LcsRid roundedRid = LbmSegment::roundToByteBoundary(rid);
    entryTuple[0].pData = (PConstBuffer) &roundedRid;
    entryTuple[1].pData = NULL;
    uint8_t byte =
        (uint8_t) (1 << (opaqueToInt(rid) % LbmSegment::LbmOneByteSize));
    entryTuple[2].pData = &byte;
    entryTuple[2].cbData = 1;
    entryList.back()->entry.setEntryTuple(entryTuple);

    // second entry -- compressed bitmap with only rid 98070 set

    pListElement = SharedLbmEntryInfo(new LbmEntryInfo());
    pListElement->pBuf = allocateBuf(bufferLock);
    pListElement->entry.init(
        pListElement->pBuf, NULL, LbmEntry::getScratchBufferSize(bitmapColSize),
        entryTupleDesc);
    entryList.push_back(pListElement);
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
    entryList.back()->entry.setEntryTuple(entryTuple);

    // third entry -- compressed bitmap with only rid 99992 set

    pListElement = SharedLbmEntryInfo(new LbmEntryInfo());
    pListElement->pBuf = allocateBuf(bufferLock);
    pListElement->entry.init(
        pListElement->pBuf, NULL, LbmEntry::getScratchBufferSize(bitmapColSize),
        entryTupleDesc);
    entryList.push_back(pListElement);
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
    entryList.back()->entry.setEntryTuple(entryTuple);

    // fourth entry -- singleton with rid 100133

    pListElement = SharedLbmEntryInfo(new LbmEntryInfo());
    pListElement->pBuf = allocateBuf(bufferLock);
    pListElement->entry.init(
        pListElement->pBuf, NULL, LbmEntry::getScratchBufferSize(bitmapColSize),
        entryTupleDesc);
    entryList.push_back(pListElement);
    rid = LcsRid(100133);
    ridValues.push_back(rid);
    entryTuple[0].pData = (PConstBuffer) &rid;
    entryTuple[1].pData = NULL;
    entryTuple[2].pData = NULL;
    entryList.back()->entry.setEntryTuple(entryTuple);

    // fifth entry -- singleton with rid 100792

    pListElement = SharedLbmEntryInfo(new LbmEntryInfo());
    pListElement->pBuf = allocateBuf(bufferLock);
    pListElement->entry.init(
        pListElement->pBuf, NULL, LbmEntry::getScratchBufferSize(bitmapColSize),
        entryTupleDesc);
    entryList.push_back(pListElement);
    rid = LcsRid(100792);
    ridValues.push_back(rid);
    entryTuple[0].pData = (PConstBuffer) &rid;
    entryTuple[1].pData = NULL;
    entryTuple[2].pData = NULL;
    entryList.back()->entry.setEntryTuple(entryTuple);

    // produce the tuples corresponding to each LbmEntry
    for (uint i = 0; i < entryList.size(); i++) {
        entryList[i]->entryTuple = entryList[i]->entry.produceEntryTuple();
    }

    uint ridPos = 0;
    PBuffer mergeBuf = allocateBuf(bufferLock);
    LbmEntry mEntry;
    mEntry.init(
        mergeBuf, NULL, LbmEntry::getScratchBufferSize(bitmapColSize),
        entryTupleDesc);

    mEntry.setEntryTuple(entryList[0]->entryTuple);
    for (uint i = 1; i < entryList.size(); i++) {
        if (mEntry.mergeEntry(entryList[i]->entryTuple)) {
            continue;
        }
        // not able to merge, so need to produce entry and compare against
        // expected rids before starting to merge on the next entry
        bool rc = compareExpected(mEntry, ridValues, ridPos, true);
        BOOST_REQUIRE(rc);
        mEntry.setEntryTuple(entryList[i]->entryTuple);
    }
    // if this is the last remaining entry, compare it against
    // expected rid values
    if (ridPos < ridValues.size()) {
        bool rc = compareExpected(mEntry, ridValues, ridPos, true);
        BOOST_REQUIRE(rc);
    }

    BOOST_REQUIRE(ridPos == ridValues.size());

    scratchAccessor.pSegment->deallocatePageRange(NULL_PAGE_ID, NULL_PAGE_ID);
}

void LbmEntryTest::testler5920()
{
    std::vector<SharedLbmEntryInfo> entryList;
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
    bufUsed = bufSize;

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

    SharedLbmEntryInfo pListElement = SharedLbmEntryInfo(new LbmEntryInfo());

    // first entry -- full bitmap
    
    pListElement->pBuf = allocateBuf(bufferLock);
    pListElement->entry.init(
        pListElement->pBuf, NULL, LbmEntry::getScratchBufferSize(bitmapColSize),
        entryTupleDesc);
    entryList.push_back(pListElement);
    LcsRid rid = LcsRid(0);
    ridValues.push_back(rid);
    entryTuple[0].pData = (PConstBuffer) &rid;
    entryTuple[1].cbData = 0;
    entryTuple[1].pData = NULL;
    entryTuple[2].cbData = 0;
    entryTuple[2].pData = NULL;
    entryList.back()->entry.setEntryTuple(entryTuple);
    for (uint i = 0; i < bitmapColSize - 2; i++) {
        rid = LcsRid((i + 1) * 8);
        entryList.back()->entry.setRID(rid);
        ridValues.push_back(rid);
    }
    LcsRid overlapStartRid = LcsRid((bitmapColSize - 2) * 8);

    // second entry -- compressed bitmap that overlaps the last byte of the
    // first entry

    pListElement = SharedLbmEntryInfo(new LbmEntryInfo());
    pListElement->pBuf = allocateBuf(bufferLock);
    pListElement->entry.init(
        pListElement->pBuf, NULL, LbmEntry::getScratchBufferSize(bitmapColSize),
        entryTupleDesc);
    entryList.push_back(pListElement);
    entryTuple[0].pData = (PConstBuffer) &overlapStartRid;
    entryTuple[1].pData = NULL;
    entryTuple[1].cbData = 0;
    rid = LcsRid(overlapStartRid + 4);
    ridValues.push_back(rid);
    uint8_t byte =
        (uint8_t) (1 << (opaqueToInt(rid) % LbmSegment::LbmOneByteSize));
    rid = LcsRid(overlapStartRid + 5);
    ridValues.push_back(rid);
    byte |= (uint8_t) (1 << (opaqueToInt(rid) % LbmSegment::LbmOneByteSize));
    entryTuple[2].pData = &byte;
    entryTuple[2].cbData = 1;
    entryList.back()->entry.setEntryTuple(entryTuple);

    // third entry -- singleton

    pListElement = SharedLbmEntryInfo(new LbmEntryInfo());
    pListElement->pBuf = allocateBuf(bufferLock);
    pListElement->entry.init(
        pListElement->pBuf, NULL, LbmEntry::getScratchBufferSize(bitmapColSize),
        entryTupleDesc);
    entryList.push_back(pListElement);
    rid = LcsRid(overlapStartRid + 8 + 2);
    ridValues.push_back(rid);
    entryTuple[0].pData = (PConstBuffer) &rid;
    entryTuple[1].pData = NULL;
    entryTuple[1].cbData = 0;
    entryTuple[2].pData = NULL;
    entryTuple[2].cbData = 0;
    entryList.back()->entry.setEntryTuple(entryTuple);

    // produce the tuples corresponding to each LbmEntry
    for (uint i = 0; i < entryList.size(); i++) {
        entryList[i]->entryTuple = entryList[i]->entry.produceEntryTuple();
    }

    uint ridPos = 0;
    PBuffer mergeBuf = allocateBuf(bufferLock);
    LbmEntry mEntry;
    mEntry.init(
        mergeBuf, NULL, LbmEntry::getScratchBufferSize(bitmapColSize),
        entryTupleDesc);

    mEntry.setEntryTuple(entryList[0]->entryTuple);

    // merge the the overlapping byte; mergeEntry should return true
    bool rc = mEntry.mergeEntry(entryList[1]->entryTuple);
    BOOST_REQUIRE(rc);

    // this merge should overflow the currentEntry
    rc = mEntry.mergeEntry(entryList[2]->entryTuple);
    BOOST_REQUIRE(!rc);
    rc = compareExpected(mEntry, ridValues, ridPos, true);
    BOOST_REQUIRE(rc);

    // make sure the previous merge didn't add any extra rids
    mEntry.setEntryTuple(entryList[2]->entryTuple);
    rc = compareExpected(mEntry, ridValues, ridPos, true);
    BOOST_REQUIRE(rc);

    scratchAccessor.pSegment->deallocatePageRange(NULL_PAGE_ID, NULL_PAGE_ID);
}

void LbmEntryTest::testZeroBytes()
{
    // Exercise creating bitmaps that contain zero bytes requiring various
    // lengths to encode.

    std::vector<SharedLbmEntryInfo> entryList;
    std::vector<LcsRid> ridValues;

    SegmentAccessor scratchAccessor =
        pSegmentFactory->newScratchSegment(pCache, 10);
    bufSize = scratchAccessor.pSegment->getUsablePageSize();
    SegPageLock bufferLock;
    bufferLock.accessSegment(scratchAccessor);
    bufUsed = bufSize;

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

    SharedLbmEntryInfo pListElement = SharedLbmEntryInfo(new LbmEntryInfo());

    // 1st entry -- start with rid 1
    
    pListElement->pBuf = allocateBuf(bufferLock);
    pListElement->entry.init(
        pListElement->pBuf, NULL, LbmEntry::getScratchBufferSize(bitmapColSize),
        entryTupleDesc);
    entryList.push_back(pListElement);
    LcsRid rid = LcsRid(1);
    ridValues.push_back(rid);
    LcsRid roundedRid = LbmSegment::roundToByteBoundary(rid);
    entryTuple[0].pData = (PConstBuffer) &roundedRid;
    entryTuple[1].pData = NULL;
    uint8_t byte =
        (uint8_t) (1 << (opaqueToInt(rid) % LbmSegment::LbmOneByteSize));
    entryTuple[2].pData = &byte;
    entryTuple[2].cbData = 1;
    entryList.back()->entry.setEntryTuple(entryTuple);

    // 2nd entry -- previous rid + 2^16*8.  This will require 2 bytes to store
    // the zero bytes in between this rid and the previous.

    pListElement = SharedLbmEntryInfo(new LbmEntryInfo());
    pListElement->pBuf = allocateBuf(bufferLock);
    pListElement->entry.init(
        pListElement->pBuf, NULL, LbmEntry::getScratchBufferSize(bitmapColSize),
        entryTupleDesc);
    entryList.push_back(pListElement);
    rid = LcsRid(rid + (int64_t) pow(2,16)*8);
    ridValues.push_back(rid);
    entryTuple[0].pData = (PConstBuffer) &rid;
    entryTuple[1].pData = NULL;
    entryTuple[2].pData = NULL;
    entryList.back()->entry.setEntryTuple(entryTuple);

    // 3rd entry -- previous rid + 2^24*8.  This will require 3 bytes to
    // store the zero bytes in between this rid and the previous.

    pListElement = SharedLbmEntryInfo(new LbmEntryInfo());
    pListElement->pBuf = allocateBuf(bufferLock);
    pListElement->entry.init(
        pListElement->pBuf, NULL, LbmEntry::getScratchBufferSize(bitmapColSize),
        entryTupleDesc);
    entryList.push_back(pListElement);
    rid = LcsRid(rid + (int64_t) pow(2,24)*8);
    ridValues.push_back(rid);
    entryTuple[0].pData = (PConstBuffer) &rid;
    entryTuple[1].pData = NULL;
    entryTuple[2].pData = NULL;
    entryList.back()->entry.setEntryTuple(entryTuple);

    // 4th entry -- previous rid + ((2^24)+1)*8.  The zero bytes in this case
    // cannot be encoded in 3 bytes, so a new bitmap entry will be created.

    pListElement = SharedLbmEntryInfo(new LbmEntryInfo());
    pListElement->pBuf = allocateBuf(bufferLock);
    pListElement->entry.init(
        pListElement->pBuf, NULL, LbmEntry::getScratchBufferSize(bitmapColSize),
        entryTupleDesc);
    entryList.push_back(pListElement);
    rid = LcsRid(rid + (int64_t) (pow(2,24)+1)*8);
    ridValues.push_back(rid);
    entryTuple[0].pData = (PConstBuffer) &rid;
    entryTuple[1].pData = NULL;
    entryTuple[2].pData = NULL;
    entryList.back()->entry.setEntryTuple(entryTuple);

    // produce the tuples corresponding to each LbmEntry
    for (uint i = 0; i < entryList.size(); i++) {
        entryList[i]->entryTuple = entryList[i]->entry.produceEntryTuple();
    }

    uint ridPos = 0;
    PBuffer mergeBuf = allocateBuf(bufferLock);
    LbmEntry mEntry;
    mEntry.init(
        mergeBuf, NULL, LbmEntry::getScratchBufferSize(bitmapColSize),
        entryTupleDesc);

    mEntry.setEntryTuple(entryList[0]->entryTuple);
    for (uint i = 1; i < entryList.size(); i++) {
        if (mEntry.mergeEntry(entryList[i]->entryTuple)) {
            continue;
        }
        // not able to merge, so need to produce entry and compare against
        // expected rids before starting to merge on the next entry; pass
        // in false for the testContains parameter to avoid excessive
        // containsRids() calls
        bool rc = compareExpected(mEntry, ridValues, ridPos, false);
        BOOST_REQUIRE(rc);
        mEntry.setEntryTuple(entryList[i]->entryTuple);
    }
    // if this is the last remaining entry, compare it against
    // expected rid values
    if (ridPos < ridValues.size()) {
        bool rc = compareExpected(mEntry, ridValues, ridPos, false);
        BOOST_REQUIRE(rc);
    }

    BOOST_REQUIRE(ridPos == ridValues.size());

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
        prev = LcsRid(1);
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

void LbmEntryTest::testMergeSingleton(
    uint scratchBufferSize, const std::vector<LcsRid> &ridValues,
    uint nSingletons, bool split)
{
    // bitmap entries will be created without a leading key, so the
    // first column is the startRid

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

    // generate the entry that the singleton will splice into
    LbmEntry lbmEntry;
    boost::scoped_array<FixedBuffer> buf, buf2;
    buf.reset(new FixedBuffer[scratchBufferSize]);
    buf2.reset(new FixedBuffer[scratchBufferSize]);
    lbmEntry.init(buf.get(), buf2.get(), scratchBufferSize, entryTupleDesc);
    entryTuple[0].pData = (PConstBuffer) &(ridValues[0]);
    lbmEntry.setEntryTuple(entryTuple);

    uint totalRids = ridValues.size();
    for (uint i = 1; i < totalRids - nSingletons; i++) {
        bool rc = lbmEntry.setRID(ridValues[i]);
        // make sure there is enough space to fit the initial set of rids
        BOOST_REQUIRE(rc);
    }

    std::vector<LcsRid> sortedRids;
    sortedRids.assign(ridValues.begin(), ridValues.end());
    std::sort(sortedRids.begin(), sortedRids.end());
#if 0
    std::cout << "Sorted Rid Values" << std::endl;
    for (uint i = 0; i < sortedRids.size(); i++) {
        std::cout << sortedRids[i] << std::endl;
    }
#endif

    // merge in the singletons and compare against the sorted original list
    // as entries fill up
    uint ridPos = 0;
    bool splitOccurred = false;
    for (uint i = 0; i < nSingletons; i++) {
        entryTuple[0].pData =
            (PConstBuffer) &(ridValues[totalRids - nSingletons + i]);
        entryTuple[1].pData = NULL;
        entryTuple[1].cbData = 0;
        entryTuple[2].pData = NULL;
        entryTuple[1].cbData = 0;
        if (!lbmEntry.mergeEntry(entryTuple)) {
            BOOST_REQUIRE(split);
            bool rc = compareExpected(lbmEntry, sortedRids, ridPos, true);
            BOOST_REQUIRE(rc);
            lbmEntry.setEntryTuple(entryTuple);
            splitOccurred = true;
        }
    } 

    // compare the rids in the last entry
    if (ridPos < totalRids) {
        bool rc = compareExpected(lbmEntry, sortedRids, ridPos, true);
        BOOST_REQUIRE(rc);
    }

    BOOST_REQUIRE(ridPos == totalRids);

    BOOST_REQUIRE(!(split && !splitOccurred));
}

void LbmEntryTest::testMergeSingletonInFront1()
{
    std::vector<LcsRid> ridValues;

    ridValues.push_back(LcsRid(9));
    ridValues.push_back(LcsRid(18));
    ridValues.push_back(LcsRid(27));
    ridValues.push_back(LcsRid(60));
    ridValues.push_back(LcsRid(77));

    // singleton rid needs to be in a byte in front of the current entry
    ridValues.push_back(LcsRid(1));
    testMergeSingleton(24, ridValues, 1, false);
}

void LbmEntryTest::testMergeSingletonInFront2()
{
    std::vector<LcsRid> ridValues;

    ridValues.push_back(LcsRid(20));

    // singleton rid needs to be in a byte in front of the current entry --
    // current entry is also a singleton
    ridValues.push_back(LcsRid(1));
    testMergeSingleton(24, ridValues, 1, false);
}

void LbmEntryTest::testMergeSingletonMidSegment1()
{
    std::vector<LcsRid> ridValues;

    ridValues.push_back(LcsRid(9));
    ridValues.push_back(LcsRid(18));
    ridValues.push_back(LcsRid(27));
    ridValues.push_back(LcsRid(60));
    ridValues.push_back(LcsRid(77));

    // singleton rid will be set in an existing rid range -- first rid in
    // segment
    ridValues.push_back(LcsRid(8));
    testMergeSingleton(24, ridValues, 1, false);
}

void LbmEntryTest::testMergeSingletonMidSegment2()
{
    std::vector<LcsRid> ridValues;

    ridValues.push_back(LcsRid(9));
    ridValues.push_back(LcsRid(18));
    ridValues.push_back(LcsRid(27));
    ridValues.push_back(LcsRid(60));
    ridValues.push_back(LcsRid(77));

    // singleton rid will be set in an existing rid range -- rid in the middle
    ridValues.push_back(LcsRid(17));
    testMergeSingleton(24, ridValues, 1, false);
}

void LbmEntryTest::testMergeSingletonMidSegment3()
{
    std::vector<LcsRid> ridValues;

    ridValues.push_back(LcsRid(9));
    ridValues.push_back(LcsRid(18));
    ridValues.push_back(LcsRid(27));
    ridValues.push_back(LcsRid(60));
    ridValues.push_back(LcsRid(77));

    // singleton rid will be set in an existing rid range -- rid at the end
    ridValues.push_back(LcsRid(31));
    testMergeSingleton(24, ridValues, 1, false);
}

void LbmEntryTest::testMergeSingletonMidSegment4()
{
    std::vector<LcsRid> ridValues;

    ridValues.push_back(LcsRid(9));
    ridValues.push_back(LcsRid(18));
    ridValues.push_back(LcsRid(27));
    ridValues.push_back(LcsRid(60));
    ridValues.push_back(LcsRid(77));

    // singleton rid will be set in an existing rid range -- rid in the 2nd
    // byte segment
    ridValues.push_back(LcsRid(56));
    testMergeSingleton(24, ridValues, 1, false);
}

void LbmEntryTest::testMergeSingletonEndSegment()
{
    std::vector<LcsRid> ridValues;

    ridValues.push_back(LcsRid(9));
    ridValues.push_back(LcsRid(18));
    ridValues.push_back(LcsRid(27));
    ridValues.push_back(LcsRid(60));
    ridValues.push_back(LcsRid(77));

    // singleton rid is in the byte adjacent to the last byte in a segment
    ridValues.push_back(LcsRid(33));
    testMergeSingleton(24, ridValues, 1, false);
}

void LbmEntryTest::testMergeSingletonStartSegment1()
{
    std::vector<LcsRid> ridValues;

    ridValues.push_back(LcsRid(9));
    ridValues.push_back(LcsRid(18));
    ridValues.push_back(LcsRid(27));
    ridValues.push_back(LcsRid(60));
    ridValues.push_back(LcsRid(77));

    // singleton rid is in the byte in front of the first byte in a segment
    // that's not the first byte in an entry -- last bit in new segment
    ridValues.push_back(LcsRid(55));
    testMergeSingleton(24, ridValues, 1, false);
}

void LbmEntryTest::testMergeSingletonStartSegment2()
{
    std::vector<LcsRid> ridValues;

    ridValues.push_back(LcsRid(9));
    ridValues.push_back(LcsRid(18));
    ridValues.push_back(LcsRid(27));
    ridValues.push_back(LcsRid(60));
    ridValues.push_back(LcsRid(77));

    // singleton rid is in the byte in front of the first byte in a segment
    // that's not the first byte in an entry -- first bit in new segment
    ridValues.push_back(LcsRid(64));
    testMergeSingleton(24, ridValues, 1, false);
}

void LbmEntryTest::testMergeSingletonInBetweenSegment()
{
    std::vector<LcsRid> ridValues;

    ridValues.push_back(LcsRid(9));
    ridValues.push_back(LcsRid(18));
    ridValues.push_back(LcsRid(27));
    ridValues.push_back(LcsRid(60));
    ridValues.push_back(LcsRid(77));

    // singleton rid is in a byte currently represented by trailing zeros, but
    // the new byte is not adjacent to an existing segment
    ridValues.push_back(LcsRid(40));
    testMergeSingleton(24, ridValues, 1, false);
}

void LbmEntryTest::testMergeSingletonSplitLeft1()
{
    std::vector<LcsRid> ridValues;

    ridValues.push_back(LcsRid(9));
    ridValues.push_back(LcsRid(18));
    ridValues.push_back(LcsRid(27));
    ridValues.push_back(LcsRid(60));
    ridValues.push_back(LcsRid(77));

    // singleton rid goes to the left side of the split entry -- at the end
    // of the new split entry
    ridValues.push_back(LcsRid(39));
    testMergeSingleton(19, ridValues, 1, true);
}

void LbmEntryTest::testMergeSingletonSplitLeft2()
{
    std::vector<LcsRid> ridValues;

    ridValues.push_back(LcsRid(9));
    ridValues.push_back(LcsRid(35));
    ridValues.push_back(LcsRid(60));
    ridValues.push_back(LcsRid(77));

    // singleton rid goes to the left side of the split entry -- move a
    // segment from the left to the right to minimize the space in the entry
    // that the new rid will be inserted into
    ridValues.push_back(LcsRid(18));
    testMergeSingleton(19, ridValues, 1, true);
}

void LbmEntryTest::testMergeSingletonSplitRight1()
{
    std::vector<LcsRid> ridValues;

    ridValues.push_back(LcsRid(9));
    ridValues.push_back(LcsRid(18));
    ridValues.push_back(LcsRid(27));
    ridValues.push_back(LcsRid(60));
    ridValues.push_back(LcsRid(85));

    // singleton rid goes to the right side of the split entry -- in the middle
    // of the split entry
    ridValues.push_back(LcsRid(71));
    testMergeSingleton(19, ridValues, 1, true);
}

void LbmEntryTest::testMergeSingletonSplitRight2()
{
    std::vector<LcsRid> ridValues;

    ridValues.push_back(LcsRid(544));
    ridValues.push_back(LcsRid(560));
    ridValues.push_back(LcsRid(576));
    ridValues.push_back(LcsRid(1088));

    // singleton rid goes to the right side of the split entry -- ensure
    // entry is split at the appropriate boundary; otherwise, there won't
    // be space in the split entry
    ridValues.push_back(LcsRid(832));
    testMergeSingleton(20, ridValues, 1, true);
}

void LbmEntryTest::testMergeSingletonSplitHalf()
{
    std::vector<LcsRid> ridValues;

    ridValues.push_back(LcsRid(9));
    ridValues.push_back(LcsRid(18));
    ridValues.push_back(LcsRid(27));
    ridValues.push_back(LcsRid(539));

    // only 2 segments with the left segment being bigger; ensure that the new
    // rid fits when merged into the left entry
    ridValues.push_back(LcsRid(283));
    testMergeSingleton(18, ridValues, 1, true);
}

void LbmEntryTest::testMergeSingletonSplitLast()
{
    std::vector<LcsRid> ridValues;

    ridValues.push_back(LcsRid(9));
    ridValues.push_back(LcsRid(18));
    ridValues.push_back(LcsRid(27));
    ridValues.push_back(LcsRid(48));
    ridValues.push_back(LcsRid(60));
    ridValues.push_back(LcsRid(64));
    ridValues.push_back(LcsRid(77));
    ridValues.push_back(LcsRid(87));
    ridValues.push_back(LcsRid(95));
    ridValues.push_back(LcsRid(96));

    // singleton rid goes to the left side of the split entry; only the last
    // segment in the original is split to the right
    ridValues.push_back(LcsRid(47));
    testMergeSingleton(23, ridValues, 1, true);
}

void LbmEntryTest::testMergeSingletonSplitMaxSegments()
{
    std::vector<LcsRid> ridValues;

    ridValues.push_back(LcsRid(0));
    // create several segments that max out at 16 bytes
    for (int i = 250; i < 600; i += 8) {
        ridValues.push_back(LcsRid(i));
    }
    ridValues.push_back(LcsRid(617));

    // inserting a new rid requires spltting in between two of the 16-byte
    // segments
    ridValues.push_back(LcsRid(125));
    testMergeSingleton(63, ridValues, 1, true);
}

void LbmEntryTest::testMergeSingletonZeros1()
{
    std::vector<LcsRid> ridValues;

    ridValues.push_back(LcsRid(4));
    // (12+2)*8 = 112
    ridValues.push_back(LcsRid(114));
    ridValues.push_back(LcsRid(123));

    // singleton rid replaces a trailing zero byte, which results in the 
    // number of trailing zero bytes decreasing by 1
    ridValues.push_back(LcsRid(47));
    testMergeSingleton(24, ridValues, 1, false);
}

void LbmEntryTest::testMergeSingletonZeros2()
{
    std::vector<LcsRid> ridValues;

    ridValues.push_back(LcsRid(4));
    // ((2^16)+1)*8 = 524288 (3 bytes required to encode zero length)
    ridValues.push_back(LcsRid(524296));
    ridValues.push_back(LcsRid(524305));

    // singleton rid replaces a trailing zero byte, which results in the 
    // number of trailing zero bytes decreasing by 1; new byte is adjacent
    // to first
    ridValues.push_back(LcsRid(13));
    testMergeSingleton(24, ridValues, 1, false);
}

void LbmEntryTest::testMergeSingletonZeros3()
{
    std::vector<LcsRid> ridValues;

    ridValues.push_back(LcsRid(4));
    // (12+2)*8 = 112
    ridValues.push_back(LcsRid(114));
    ridValues.push_back(LcsRid(123));

    // singleton rid replaces a trailing zero byte, which results in the 
    // number of trailing zero bytes decreasing by 1 but addition of new
    // segment requires a split of the current entry
    ridValues.push_back(LcsRid(60));
    testMergeSingleton(17, ridValues, 1, true);
}

void LbmEntryTest::testMergeSingletonZeros4()
{
    std::vector<LcsRid> ridValues;

    ridValues.push_back(LcsRid(4));
    // (12+2)*8 = 112
    ridValues.push_back(LcsRid(114));
    ridValues.push_back(LcsRid(123));

    // current entry is currently at max size; adding a new singleton rid
    // requires a new segment but doing so does not require a split because
    // the new segment replaces the trailing zero byte
    ridValues.push_back(LcsRid(13));
    testMergeSingleton(17, ridValues, 1, false);
}

void LbmEntryTest::testMergeSingletonCombine()
{
    std::vector<LcsRid> ridValues;

    ridValues.push_back(LcsRid(9));
    ridValues.push_back(LcsRid(18));
    ridValues.push_back(LcsRid(27));
    ridValues.push_back(LcsRid(60));
    ridValues.push_back(LcsRid(77));
    ridValues.push_back(LcsRid(86));
    ridValues.push_back(LcsRid(111));

    // singleton rid goes in the middle of 2 segments, which results in the 3
    // segments being combined into 1
    ridValues.push_back(LcsRid(71));
    testMergeSingleton(24, ridValues, 1, false);
}

void LbmEntryTest::testMergeSingletonAfterSplit()
{
    std::vector<LcsRid> ridValues;

    ridValues.push_back(LcsRid(9));
    ridValues.push_back(LcsRid(18));
    ridValues.push_back(LcsRid(27));
    ridValues.push_back(LcsRid(48));
    ridValues.push_back(LcsRid(60));
    ridValues.push_back(LcsRid(64));
    ridValues.push_back(LcsRid(77));
    ridValues.push_back(LcsRid(87));
    ridValues.push_back(LcsRid(95));
    ridValues.push_back(LcsRid(96));

    // singleton rid goes to the left side of the split entry; only the last
    // segment in the original is split to the right; add another singleton
    // after the split
    ridValues.push_back(LcsRid(33));
    ridValues.push_back(LcsRid(47));
    testMergeSingleton(23, ridValues, 2, true);
}

void LbmEntryTest::testMergeSingletonMaxSeg()
{
    std::vector<LcsRid> ridValues;

    ridValues.push_back(LcsRid(0));

    ridValues.push_back(LcsRid(27));
    ridValues.push_back(LcsRid(36));
    ridValues.push_back(LcsRid(45));
    ridValues.push_back(LcsRid(54));
    ridValues.push_back(LcsRid(63));
    ridValues.push_back(LcsRid(64));
    ridValues.push_back(LcsRid(72));
    ridValues.push_back(LcsRid(81));
    ridValues.push_back(LcsRid(90));
    ridValues.push_back(LcsRid(99));
    ridValues.push_back(LcsRid(108));
    ridValues.push_back(LcsRid(117));
    ridValues.push_back(LcsRid(126));
    ridValues.push_back(LcsRid(135));
    ridValues.push_back(LcsRid(136));
    ridValues.push_back(LcsRid(145));

    // attempt to insert singleton rid in front of a segment that currently
    // has a segment length of 16
    ridValues.push_back(LcsRid(18));
    testMergeSingleton(32, ridValues, 1, false);
}

void LbmEntryTest::testMergeSingletonRandom(uint totalRids, uint ridRange)
{
    std::vector<LcsRid> ridValues;

    // generate random rid values, ensuring that they are unique
    LcsRid rid = LcsRid(rand() % ridRange);
    ridValues.push_back(rid);
    for (uint i = 1; i < totalRids; ) {
        rid = LcsRid(rand() % ridRange);
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

#if 0
    std::cout << "Original Rid Values" << std::endl;
    for (uint i = 0; i < ridValues.size(); i++) {
        std::cout << ridValues[i] << std::endl;
    }
#endif

    // the first rid becomes the initial entry and the rest are the singleton
    // rids
    uint scratchBufferSize = totalRids * 5 + 8;
    testMergeSingleton(scratchBufferSize, ridValues, totalRids - 1, false);
}

void LbmEntryTest::testMergeSingletonRandom1()
{
    testMergeSingletonRandom(50, 1000);
}

void LbmEntryTest::testMergeSingletonRandom2()
{
    testMergeSingletonRandom(100, 10000);
}

void LbmEntryTest::testMergeSingletonWithSingleton1()
{
    std::vector<LcsRid> ridValues;

    ridValues.push_back(LcsRid(7));

    ridValues.push_back(LcsRid(1));
    testMergeSingleton(16, ridValues, 1, false);
}

void LbmEntryTest::testMergeSingletonWithSingleton2()
{
    std::vector<LcsRid> ridValues;

    ridValues.push_back(LcsRid(50));

    ridValues.push_back(LcsRid(1));
    // not enough space so the entries should get split into 2
    testMergeSingleton(10, ridValues, 1, true);
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
