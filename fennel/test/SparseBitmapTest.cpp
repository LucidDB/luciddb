/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2008-2009 The Eigenbase Project
// Copyright (C) 2008-2009 SQLstream, Inc.
// Copyright (C) 2008-2009 LucidEra, Inc.
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
#include "fennel/segment/SegPageLock.h"
#include "fennel/segment/SegmentFactory.h"
#include "fennel/segment/LinearDeviceSegment.h"
#include "fennel/device/RandomAccessFileDevice.h"
#include "fennel/cache/Cache.h"
#include "fennel/cache/CacheParams.h"
#include "fennel/test/TestBase.h"

#include <boost/test/test_tools.hpp>
#include <boost/bind.hpp>

// NOTE jvs 25-Nov-2008:  This file contains the code for the example
// in http://pub.eigenbase.org/wiki/FennelPageBasedDataStructureHowto;
// if you modify the wiki page, please be sure to modify the code as well,
// and vice versa.

/**
 * 0-based offset (bit address) within a sparse bitmap.
 */
typedef uint64_t SparseBitmapOffset;

/**
 * Entry in a sparse bitmap directory.
 */
struct SparseBitmapDirEntry
{
    /**
     * Start offsets of bits contained by the corresponding leaf node.
     */
    SparseBitmapOffset iLeafStartOffset;

    /**
     * Reference to leaf node.
     */
    fennel::PageId leafId;
};

/**
 * Page header data structure for a sparse bitmap directory node.
 */
struct SparseBitmapDirectory : public fennel::StoredNode
{
    static const fennel::MagicNumber MAGIC_NUMBER = 0x82009900461412f0LL;

    /**
     * Number of SparseBitmapDirEntry items currently filled on this page.
     */
    uint nEntries;

    /**
     * @return read-only reference to array of directory entries
     * on this page
     */
    SparseBitmapDirEntry const *getEntriesForRead() const
    {
        return reinterpret_cast<SparseBitmapDirEntry const *>(
            this + 1);
    }

    /**
     * @return read/write reference to array of directory entries
     * on this page
     */
    SparseBitmapDirEntry *getEntriesForWrite()
    {
        return reinterpret_cast<SparseBitmapDirEntry *>(
            this + 1);
    }
};

/**
 * Page lock guard for sparse bitmap directory nodes.
 */
typedef fennel::SegNodeLock<SparseBitmapDirectory> SparseBitmapDirLock;

/**
 * Page header data structure for a sparse bitmap leaf node.
 */
struct SparseBitmapLeaf : public fennel::StoredNode
{
    static const fennel::MagicNumber MAGIC_NUMBER = 0xba107d175b3338dcLL;

    /**
     * Start offset of bits contained by this leaf node.  This is redundant
     * (for sanity-checking and self-identification purposes); it should
     * match the corresponding directory entry referencing this leaf.
     */
    SparseBitmapOffset iStartOffset;

    /**
     * @return read-only reference to bytes containing bit array on
     * this page
     */
    fennel::PConstBuffer getBytesForRead() const
    {
        return reinterpret_cast<fennel::PConstBuffer>(this + 1);
    }

    /**
     * @return read/write reference to bytes containing bit array on
     * this page
     */
    fennel::PBuffer getBytesForWrite()
    {
        return reinterpret_cast<fennel::PBuffer>(this + 1);
    }
};

/**
 * Page lock guard for sparse bitmap leaf nodes.
 */
typedef fennel::SegNodeLock<SparseBitmapLeaf> SparseBitmapLeafLock;

/**
 * SparseBitmap is an example of how to create a page-based
 * persistent data structure using Fennel.  It is only intended
 * for educational purposes, not for real use.
 */
class SparseBitmap
{
    /**
     * Accessor for the segment storing the bitmap node pages.
     */
    fennel::SegmentAccessor segmentAccessor;

    /**
     * Location of bitmap directory node.
     */
    fennel::PageId dirPageId;

    /**
     * Number of bits contained by each leaf node; this is fixed
     * based on segment page size after headers and footers have been
     * subtracted off.
     */
    size_t nBitsPerLeaf;

    /**
     * Maximum number of entries which can be contained by
     * the directory node; this is fixed based on segment page size
     * after headers and footers have been subtracted off, and determines
     * the total number of leaf pages which can be allocated.
     */
    size_t nDirEntriesMax;

    /**
     * Looks up a leaf node (based on its start offset) in the directory.
     *
     * @param dirLock page guard for directory node, which must already
     * be locked in the desired mode
     *
     * @param iLeafStartOffset leaf node start offset to search for
     *
     * @return location of leaf node, or NULL_PAGE_ID if not found
     */
    fennel::PageId searchDirectory(
        SparseBitmapDirLock &dirLock,
        SparseBitmapOffset iLeafStartOffset);

public:
    /**
     * Creates a new empty sparse bitmap (or loads an existing one).
     *
     * @param segmentAccessor accessor for the segment storing the bitmap node
     * pages
     *
     * @param dirPageId location of existing directory node to load,
     * or NULL_PAGE_ID to create a new bitmap
     */
    explicit SparseBitmap(
        fennel::SegmentAccessor segmentAccessor,
        fennel::PageId dirPageId = fennel::NULL_PAGE_ID);

    /**
     * Sparse bitmap destructor.
     */
    virtual ~SparseBitmap();

    /**
     * Reads a bit from the bitmap.
     *
     * @param iOffset address of bit to read
     *
     * @return true iff bit is set
     */
    bool getBit(SparseBitmapOffset iOffset);

    /**
     * Writes a bit in the bitmap.
     *
     * @param iOffset address of bit to write
     *
     * @param value true to set bit; false to clear bit
     */
    void setBit(SparseBitmapOffset iOffset, bool value);

    /**
     * @return the location of the directory node for this bitmap
     */
    fennel::PageId getDirPageId() const;

    /**
     * @return number of bits contained by each leaf node
     */
    size_t getBitsPerLeaf() const;

    /**
     * @return maximum number of entries which can be contained
     * by the directory node
     */
    size_t getMaxDirectoryEntries() const;
};

SparseBitmap::SparseBitmap(
    fennel::SegmentAccessor segmentAccessorInit,
    fennel::PageId dirPageIdInit)
{
    segmentAccessor = segmentAccessorInit;
    dirPageId = dirPageIdInit;

    if (dirPageId == fennel::NULL_PAGE_ID) {
        // create new empty directory
        SparseBitmapDirLock dirLock;
        dirLock.accessSegment(segmentAccessor);
        dirPageId = dirLock.allocatePage(fennel::ANON_PAGE_OWNER_ID);
        SparseBitmapDirectory &dir = dirLock.getNodeForWrite();
        dir.nEntries = 0;
    }

    // precompute some limits based on page and header sizes
    size_t cbPage = segmentAccessor.pSegment->getUsablePageSize();
    size_t nBytesPerLeaf = cbPage - sizeof(SparseBitmapLeaf);
    nBitsPerLeaf = nBytesPerLeaf * 8;
    assert(nBitsPerLeaf > 0);

    nDirEntriesMax =
        (cbPage - sizeof(SparseBitmapDirectory)) / sizeof(SparseBitmapDirEntry);
    assert(nDirEntriesMax > 0);
}

SparseBitmap::~SparseBitmap()
{
}

fennel::PageId SparseBitmap::searchDirectory(
    SparseBitmapDirLock &dirLock,
    SparseBitmapOffset iLeafStartOffset)
{
    SparseBitmapDirectory const &dir = dirLock.getNodeForRead();

    SparseBitmapDirEntry const *pFirst = dir.getEntriesForRead();
    SparseBitmapDirEntry const *pLast = pFirst + dir.nEntries;
    SparseBitmapDirEntry const *pFound =
        std::find_if(
            pFirst,
            pLast,
            boost::bind(&SparseBitmapDirEntry::iLeafStartOffset, _1)
            == iLeafStartOffset);
    if (pFound == pLast) {
        return fennel::NULL_PAGE_ID;
    }
    return pFound->leafId;
}

bool SparseBitmap::getBit(SparseBitmapOffset iOffset)
{
    // Lock directory page in shared mode
    SparseBitmapDirLock dirLock;
    dirLock.accessSegment(segmentAccessor);
    dirLock.lockShared(dirPageId);

    // Compute start offset of leaf page containing iOffset
    SparseBitmapOffset iLeafStartOffset =
        (iOffset / nBitsPerLeaf) * nBitsPerLeaf;

    // Look for it in the directory
    fennel::PageId leafId = searchDirectory(dirLock, iLeafStartOffset);
    if (leafId == fennel::NULL_PAGE_ID) {
        // Not in directory, so the bit is not set
        return false;
    }

    // Unlock directory node early since we don't need it any more
    dirLock.unlock();

    // Lock leaf page and perform sanity check
    SparseBitmapLeafLock leafLock;
    leafLock.accessSegment(segmentAccessor);
    leafLock.lockShared(leafId);
    SparseBitmapLeaf const &leaf = leafLock.getNodeForRead();
    assert(leaf.iStartOffset == iLeafStartOffset);

    // Read bit value from leaf
    fennel::PConstBuffer pBytes = leaf.getBytesForRead();
    size_t iBit = iOffset - iLeafStartOffset;
    size_t iByte = iBit / 8;
    size_t iBitInByte = iBit - 8 * iByte;
    if (pBytes[iByte] & (1 << iBitInByte)) {
        return true;
    } else {
        return false;
    }
}

void SparseBitmap::setBit(SparseBitmapOffset iOffset, bool value)
{
    // Lock directory page in exclusive mode
    SparseBitmapDirLock dirLock;
    dirLock.accessSegment(segmentAccessor);
    dirLock.lockExclusive(dirPageId);

    // Search directory; if it already has a corresponding leaf entry,
    // we won't need to modify the directory
    SparseBitmapOffset iLeafStartOffset =
        (iOffset / nBitsPerLeaf) * nBitsPerLeaf;
    fennel::PageId leafId = searchDirectory(dirLock, iLeafStartOffset);

    SparseBitmapLeafLock leafLock;
    leafLock.accessSegment(segmentAccessor);
    bool clearLeaf = false;
    if (leafId == fennel::NULL_PAGE_ID) {
        // Leaf doesn't exist yet:  we'll need a new one
        SparseBitmapDirectory &dir = dirLock.getNodeForWrite();
        if (dir.nEntries >= nDirEntriesMax) {
            // Oops, directory is full and we have no provisions
            // for splitting it; we haven't modified the directory yet,
            // so the bitmap remains intact
            throw std::runtime_error("SparseBitmap directory full");
        }
        // Allocate new leaf and add it to the directory
        SparseBitmapDirEntry *pLast =
            dir.getEntriesForWrite() + dir.nEntries;
        leafId = leafLock.allocatePage(fennel::ANON_PAGE_OWNER_ID);
        pLast->iLeafStartOffset = iLeafStartOffset;
        pLast->leafId = leafId;
        dir.nEntries++;
        clearLeaf = true;
        dirLock.unlock();
    } else {
        // Leaf already exists, so no need to modify directory;
        // we can unlock the directory early
        dirLock.unlock();
        leafLock.lockExclusive(leafId);
    }

    // Write bit value to leaf
    SparseBitmapLeaf &leaf = leafLock.getNodeForWrite();
    fennel::PBuffer pBytes = leaf.getBytesForWrite();
    if (clearLeaf) {
        // We're initializing a new leaf, so clear all the bits first
        leaf.iStartOffset = iLeafStartOffset;
        memset(pBytes, 0, nBitsPerLeaf / 8);
    }
    size_t iBit = iOffset - iLeafStartOffset;
    size_t iByte = iBit / 8;
    size_t iBitInByte = iBit - 8 * iByte;
    if (value) {
        pBytes[iByte] |= (1 << iBitInByte);
    } else {
        pBytes[iByte] &= ~(1 << iBitInByte);
    }
}

fennel::PageId SparseBitmap::getDirPageId() const
{
    return dirPageId;
}

size_t SparseBitmap::getBitsPerLeaf() const
{
    return nBitsPerLeaf;
}

size_t SparseBitmap::getMaxDirectoryEntries() const
{
    return nDirEntriesMax;
}

using namespace fennel;

/**
 * Unit tests for SparseBitmap.
 */
class SparseBitmapTest : virtual public TestBase
{
    SharedRandomAccessDevice pDevice;
    SharedCache pCache;
    SharedSegmentFactory pSegmentFactory;
    SharedSegment pSegment;
    SegmentAccessor segmentAccessor;

    static const DeviceId BITMAP_DEVICE_ID;

    void openStorage(DeviceMode deviceMode);
    void closeStorage();

public:
    explicit SparseBitmapTest()
    {
        FENNEL_UNIT_TEST_CASE(SparseBitmapTest, testBasic);
        FENNEL_UNIT_TEST_CASE(SparseBitmapTest, testSpread);
        FENNEL_UNIT_TEST_CASE(SparseBitmapTest, testSizes);
        FENNEL_UNIT_TEST_CASE(SparseBitmapTest, testFullDirectory);
    }

    virtual void testCaseTearDown();

    void testBasic();
    void testSpread();
    void testSizes();
    void testFullDirectory();
};

/**
 * Designated device ID for SparseBitmap unit tests.
 */
const DeviceId SparseBitmapTest::BITMAP_DEVICE_ID = DeviceId(1);

void SparseBitmapTest::openStorage(DeviceMode deviceMode)
{
    // Create or load a file device
    pDevice.reset(
        new RandomAccessFileDevice(
            "bitmap.dat",
            deviceMode,
            0));

    // Set up the cache
    CacheParams cacheParams;
    cacheParams.readConfig(configMap);
    pCache = Cache::newCache(cacheParams);
    pCache->registerDevice(BITMAP_DEVICE_ID, pDevice);

    // Map a segment onto the file
    pSegmentFactory =
        SegmentFactory::newSegmentFactory(configMap, shared_from_this());
    LinearDeviceSegmentParams segParams;
    CompoundId::setDeviceId(segParams.firstBlockId, BITMAP_DEVICE_ID);
    CompoundId::setBlockNum(segParams.firstBlockId, 0);
    if (!deviceMode.create) {
        segParams.nPagesAllocated = MAXU;
    }
    pSegment = pSegmentFactory->newLinearDeviceSegment(
        pCache,
        segParams);
    segmentAccessor.pSegment = pSegment;
    segmentAccessor.pCacheAccessor = pCache;
}

void SparseBitmapTest::testCaseTearDown()
{
    // Regardless of how the test ends, be sure to close all resources
    closeStorage();
    TestBase::testCaseTearDown();
}

void SparseBitmapTest::closeStorage()
{
    segmentAccessor.reset();
    if (pSegment) {
        assert(pSegment.unique());
        pSegment.reset();
    }
    if (pSegmentFactory) {
        assert(pSegmentFactory.unique());
        pSegmentFactory.reset();
    }
    if (pCache) {
        pCache->unregisterDevice(BITMAP_DEVICE_ID);
        assert(pCache.unique());
        pCache.reset();
    }
    if (pDevice) {
        assert(pDevice.unique());
        pDevice.reset();
    }
}

void SparseBitmapTest::testBasic()
{
    // Create a new bitmap and set a single bit at offset 0
    PageId dirPageId;
    openStorage(DeviceMode::createNew);

    {
        SparseBitmap bitmap(segmentAccessor);
        dirPageId = bitmap.getDirPageId();
        bitmap.setBit(0, 1);

        // Make sure we can read the bit back
        bool x = bitmap.getBit(0);
        BOOST_CHECK_EQUAL(1, x);
    }

    // Now close and re-open storage
    closeStorage();
    openStorage(DeviceMode::load);

    {
        // Verify that we can still read the bit back
        SparseBitmap bitmap(segmentAccessor, dirPageId);
        bool x = bitmap.getBit(0);
        BOOST_CHECK_EQUAL(1, x);
    }
}

void SparseBitmapTest::testSpread()
{
    // Similar to testBasic, but use a bunch of predefined bit offsets

    std::vector<SparseBitmapOffset> filledOffsets;
    filledOffsets.push_back(5);
    filledOffsets.push_back(6);
    filledOffsets.push_back(8);
    filledOffsets.push_back(100);
    filledOffsets.push_back(50000);
    filledOffsets.push_back(50001);
    filledOffsets.push_back(50004);
    filledOffsets.push_back(55000);

    std::vector<SparseBitmapOffset> emptyOffsets;
    emptyOffsets.push_back(0);
    emptyOffsets.push_back(7);
    emptyOffsets.push_back(1000);
    emptyOffsets.push_back(50003);
    emptyOffsets.push_back(1000000);

    PageId dirPageId;
    openStorage(DeviceMode::createNew);

    {
        SparseBitmap bitmap(segmentAccessor);
        dirPageId = bitmap.getDirPageId();
        for (int i = 0; i < filledOffsets.size(); ++i) {
            bitmap.setBit(filledOffsets[i], 1);
        }
    }

    closeStorage();

    openStorage(DeviceMode::load);

    {
        SparseBitmap bitmap(segmentAccessor, dirPageId);
        for (int i = 0; i < filledOffsets.size(); ++i) {
            bool x = bitmap.getBit(filledOffsets[i]);
            BOOST_CHECK_EQUAL(1, x);
        }

        // Also verify that some bits didn't get accidentally set
        for (int i = 0; i < emptyOffsets.size(); ++i) {
            bool x = bitmap.getBit(emptyOffsets[i]);
            BOOST_CHECK_EQUAL(0, x);
        }
    }
}

void SparseBitmapTest::testSizes()
{
    openStorage(DeviceMode::createNew);
    if (pCache->getPageSize() != 4096) {
        // Expected values below are only valid for 4K page size
        return;
    }
    SparseBitmap bitmap(segmentAccessor);
    BOOST_CHECK_EQUAL(32640, bitmap.getBitsPerLeaf());
    BOOST_CHECK_EQUAL(255, bitmap.getMaxDirectoryEntries());
}

void SparseBitmapTest::testFullDirectory()
{
    // Negative test to make sure we get the expected failure
    // when the directory fills up completely
    openStorage(DeviceMode::createNew);
    SparseBitmap bitmap(segmentAccessor);
    int iOffset = 10;
    for (int i = 0; i < bitmap.getMaxDirectoryEntries(); ++i) {
        bitmap.setBit(iOffset, 1);
        // stride forward to next leaf
        iOffset += bitmap.getBitsPerLeaf();
    }
    try {
        bitmap.setBit(iOffset, 1);
        BOOST_FAIL("directory full exception expected");
    } catch (std::exception ex) {
        // failure expected
    }
}

FENNEL_UNIT_TEST_SUITE(SparseBitmapTest);

// End SparseBitmapTest.cpp
