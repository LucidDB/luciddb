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
#include "fennel/test/SnapshotSegmentTestBase.h"
#include "fennel/segment/VersionedRandomAllocationSegment.h"
#include "fennel/cache/CacheStats.h"

#include <boost/test/test_tools.hpp>

using namespace fennel;

class SnapshotSegmentTest : virtual public SnapshotSegmentTestBase
{
public:
    explicit SnapshotSegmentTest()
    {
        FENNEL_UNIT_TEST_CASE(SegmentTestBase, testSingleThread);
        FENNEL_UNIT_TEST_CASE(PagingTestBase, testMultipleThreads);
        FENNEL_UNIT_TEST_CASE(SnapshotSegmentTest, testSnapshotReads);
        FENNEL_UNIT_TEST_CASE(SnapshotSegmentTest, testRollback);
        FENNEL_UNIT_TEST_CASE(SnapshotSegmentTest, testUncommittedReads);
        FENNEL_UNIT_TEST_CASE(SnapshotSegmentTest, testDeallocateOld);
    }

    void testSnapshotReads()
    {
        // Create and initialize a VersionedRandomAllocationSegment using
        // a snapshot with TxnId(0)
        currCsn = TxnId(0);
        openStorage(DeviceMode::createNew);
        testAllocateAll();
        closeStorage();

        // Create a snapshot at TxnId(5) where every 5 pages are updated
        updatedCsns.push_back(TxnId(5));
        currCsn = TxnId(5);
        openStorage(DeviceMode::load);
        testSkipWrite(5);
        closeStorage();

        // Create a snapshot at TxnId(7) where every 7 pages are updated
        updatedCsns.push_back(TxnId(7));
        currCsn = TxnId(7);
        openStorage(DeviceMode::load);
        testSkipWrite(7);
        closeStorage();

        // Read all pages as of txnIds 0-9
        for (uint i = 0; i < 10; i++) {
            currCsn = TxnId(i);
            openStorage(DeviceMode::load);
            testSequentialRead();
            closeStorage();
        }
    }

    void testRollback()
    {
        // Create and initialize a VersionedRandomAllocationSegment using
        // a snapshot with TxnId(0)
        currCsn = TxnId(0);
        openStorage(DeviceMode::createNew);
        testAllocateAll();
        closeStorage();

        // Create a snapshot at TxnId(1) where every 5 pages are updated,
        // but rollback the changes
        currCsn = TxnId(1);
        openStorage(DeviceMode::load);
        testSkipWrite(5);
        // Make sure new pages have been allocated before we roll them back
        assert(
            pVersionedRandomSegment->getAllocatedSizeInPages() ==
            nDiskPages + nDiskPages / 5 + ((nDiskPages % 5) ? 1 : 0));
        commit = false;
        closeStorage();
        commit = true;

        // Read all pages as of TxnId(2).  Changes from TxnId(1) should not
        // be seen.
        currCsn = TxnId(2);
        openStorage(DeviceMode::load);
        testSequentialRead();
        closeStorage();
    }

    void testUncommittedReads()
    {
        // Create and initialize a VersionedRandomAllocationSegment using
        // a snapshot with TxnId(0)
        currCsn = TxnId(0);
        openStorage(DeviceMode::createNew);
        testAllocateAll();
        closeStorage();

        // Create a snapshot at TxnId(5) where every 5 pages are updated,
        // but don't commit the changes yet.
        currCsn = TxnId(5);
        openStorage(DeviceMode::load);
        testSkipWrite(5);

        // Setup a new snapshot segment since we still want to keep around
        // the uncommitted TxnId(5)
        closeLinearSegment();
        pSnapshotRandomSegment2 =
            pSegmentFactory->newSnapshotRandomAllocationSegment(
                pVersionedRandomSegment,
                pVersionedRandomSegment,
                TxnId(6));
        setForceCacheUnmap(pSnapshotRandomSegment2);
        SharedSegment pLinearViewSegment =
            pSegmentFactory->newLinearViewSegment(
                pSnapshotRandomSegment2,
                firstPageId);
        pLinearSegment = pLinearViewSegment;

        // Read all pages as of TxnId(6).  Changes from TxnId(5) should not
        // be seen since it's not committed yet.
        testSequentialRead();
        pLinearViewSegment.reset();

        // Setup a snapshot segment that reads pages as of TxnId(5), but
        // ignores uncommitted updates.
        pSnapshotRandomSegment2.reset();
        closeLinearSegment();
        pSnapshotRandomSegment2 =
            pSegmentFactory->newSnapshotRandomAllocationSegment(
                pVersionedRandomSegment,
                pVersionedRandomSegment,
                TxnId(5),
                true);
        setForceCacheUnmap(pSnapshotRandomSegment2);
        pLinearViewSegment =
            pSegmentFactory->newLinearViewSegment(
                pSnapshotRandomSegment2,
                firstPageId);
        pLinearSegment = pLinearViewSegment;
        testSequentialRead();
        pLinearViewSegment.reset();

        // Commit TxnId(5)
        closeStorage();

        // Now we should see TxnId(5)
        updatedCsns.push_back(TxnId(5));
        currCsn = TxnId(6);
        openStorage(DeviceMode::load);
        testSequentialRead();
        closeStorage();
    }

    void testDeallocateOld()
    {
        // Create and initialize a VersionedRandomAllocationSegment using
        // a snapshot with TxnId(0)
        currCsn = TxnId(0);
        openStorage(DeviceMode::createNew);
        testAllocateAll();
        closeStorage();

        // Create a snapshot at TxnId(3) where every 3 pages are updated
        updatedCsns.push_back(TxnId(3));
        currCsn = TxnId(3);
        openStorage(DeviceMode::load);
        testSkipWrite(3);
        closeStorage();

        // Create a snapshot at TxnId(5) where every 5 pages are updated
        updatedCsns.push_back(TxnId(5));
        currCsn = TxnId(5);
        openStorage(DeviceMode::load);
        testSkipWrite(5);
        closeStorage();

        // Create a snapshot at TxnId(7) where every 7 pages are updated
        updatedCsns.push_back(TxnId(7));
        currCsn = TxnId(7);
        openStorage(DeviceMode::load);
        testSkipWrite(7);
        closeStorage();

        uint totalPages =
            nDiskPages +
            nDiskPages / 3 + ((nDiskPages % 3) ? 1 : 0) +
            nDiskPages / 5 + ((nDiskPages % 5) ? 1 : 0) +
            nDiskPages / 7 + ((nDiskPages % 7) ? 1 : 0);

        // Deallocate pages -- set the oldestActiveTxnId at TxnId(3).  No
        // pages should be deallocated.
        deallocateOldPages(TxnId(3), totalPages, totalPages, 3, 8);

        // Deallocate pages -- set the oldestActiveTxnId at TxnId(4).  No
        // pages should be deallocated.
        deallocateOldPages(TxnId(4), totalPages, totalPages, 4, 8);

        // Deallocate pages -- set the oldestActiveTxnId at TxnId(6).  Only
        // pages with both TxnId(3) and TxnId(5) in their page chain should
        // be deallocated, with only the TxnId(3) pages being deallocated.
        uint nPages =
            totalPages - (nDiskPages/(3*5) + ((nDiskPages % (3*5)) ? 1 : 0));
        deallocateOldPages(TxnId(6), totalPages, nPages, 6, 8);

        // Deallocate pages -- set the oldestActiveTxnId at TxnId(8).  Pages
        // with only 2 old pages in the chain can be deallocated, with only
        // the older of the two being deallocated.
        totalPages = nPages;
        nPages =
            totalPages -
                (nDiskPages/(5*7) + ((nDiskPages % (5*7)) ? 1 : 0)) -
                (nDiskPages/(3*7) + ((nDiskPages % (3*7)) ? 1 : 0)) +
                (nDiskPages/(3*5*7) + ((nDiskPages % (3*5*7)) ? 1 : 0));
        deallocateOldPages(TxnId(8), totalPages, nPages, 8, 8);

        // Deallocate the first 100 pages.  They won't actually be freed but
        // will be marked as deallocation-deferred.
        currCsn = TxnId(9);
        openStorage(DeviceMode::load);

        // Read all pages into cache to test discard behavior.
        testSequentialRead();

        // Save cache stats before deallocation.
        CacheStats statsBefore;
        pCache->collectStats(statsBefore);

        for (int i = opaqueToInt(firstPageId);
            i < 100 + opaqueToInt(firstPageId); i++)
        {
            pSnapshotRandomSegment->deallocatePageRange(PageId(i), PageId(i));
        }

        // Get cache stats after deallocation and compare.
        CacheStats statsAfter;
        pCache->collectStats(statsAfter);

        // Count of unused pages should not go up since deallocation is
        // deferred.
        BOOST_CHECK(statsAfter.nMemPagesUnused <= statsBefore.nMemPagesUnused);

        closeStorage();

        // Deallocate old pages, but set the oldestActiveTxnId to TxnId(0) so
        // no pages should actually be freed
        deallocateOldPages(TxnId(0), nPages, nPages, 8, 8);

        // Set the oldestActiveTnxId to TxnId(10) so all deallocation-deferred
        // pages will be freed.  When computing the number of freed pages,
        // take into account old pages that have already been deallocated
        // above.
        int nPagesFreed =
            100 + 100/3 + 100/5 + 100/7 - 100/(3*5) - 100/(3*7) - 100/(5*7)
            - 100/(3*5*7);
        deallocateOldPages(TxnId(10), nPages, nPages - nPagesFreed, 1, 0);
    }

    void deallocateOldPages(
        TxnId oldestActiveTxnId,
        uint numPagesBefore,
        uint numPagesAfter,
        uint readStart,
        uint readEnd)
    {
        currCsn = TxnId(10);
        openStorage(DeviceMode::load);
        VersionedRandomAllocationSegment *pVRSegment =
            SegmentFactory::dynamicCast<VersionedRandomAllocationSegment *>(
                pVersionedRandomSegment);
        uint nPages = pVRSegment->getAllocatedSizeInPages();
        assert(nPages == numPagesBefore);
        uint iSegAlloc = 0;
        ExtentNum extentNum = 0;
        uint numPages = 100;
        PageSet oldPageSet;
        bool morePages = true;
        do {
            morePages =
                pVRSegment->getOldPageIds(
                    iSegAlloc,
                    extentNum,
                    oldestActiveTxnId,
                    numPages,
                    oldPageSet);
            pVRSegment->deallocateOldPages(oldPageSet, oldestActiveTxnId);
            oldPageSet.clear();
        } while (morePages);
        nPages = pVRSegment->getAllocatedSizeInPages();
        assert(nPages == numPagesAfter);
        closeStorage();

        // Read all pages for the specified TxnId range
        for (uint i = readStart; i <= readEnd; i++) {
            currCsn = TxnId(i);
            openStorage(DeviceMode::load);
            testSequentialRead();
            closeStorage();
        }
    }
};

FENNEL_UNIT_TEST_SUITE(SnapshotSegmentTest);

// End SnapshotSegmentTest.cpp
