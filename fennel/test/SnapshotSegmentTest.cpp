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
#include "fennel/test/SegmentTestBase.h"
#include "fennel/segment/SnapshotRandomAllocationSegment.h"
#include "fennel/segment/VersionedRandomAllocationSegment.h"
#include "fennel/segment/LinearViewSegment.h"
#include "fennel/segment/SegPageLock.h"
#include "fennel/cache/CacheStats.h"

#include <boost/test/test_tools.hpp>

using namespace fennel;

class SnapshotSegmentTest : virtual public SegmentTestBase
{
    uint nDiskPagesTotal;
    PageId firstPageId;
    DeviceId tempDeviceId;
    SharedRandomAccessDevice pTempDevice;
    SharedSegment pTempSegment;
    SharedSegment pSnapshotRandomSegment2;
    std::vector<TxnId> updatedCsns;
    TxnId currCsn;
    bool commit;
    
public:
    explicit SnapshotSegmentTest()
    {
        nDiskPagesTotal = nDiskPages;
        tempDeviceId = DeviceId(42);

        FENNEL_UNIT_TEST_CASE(SegmentTestBase, testSingleThread);
        FENNEL_UNIT_TEST_CASE(PagingTestBase, testMultipleThreads);
        FENNEL_UNIT_TEST_CASE(SnapshotSegmentTest, testSnapshotReads);
        FENNEL_UNIT_TEST_CASE(SnapshotSegmentTest, testRollback);
        FENNEL_UNIT_TEST_CASE(SnapshotSegmentTest, testUncommittedReads);
        FENNEL_UNIT_TEST_CASE(SnapshotSegmentTest, testDeallocateOld);
    }
  
    virtual void testCaseSetUp()
    {
        currCsn = TxnId(0);
        commit = true;
        updatedCsns.clear();
    }

    virtual void openSegmentStorage(DeviceMode openMode)
    {
        nDiskPages = nDiskPagesTotal;
        if (openMode.create) {
            firstPageId = NULL_PAGE_ID;
        }

        // Use a random segment for the temp segment so we can verify that
        // temp pages used are freed.
        pTempDevice =
            openDevice("temp.dat", openMode, nDiskPages/50, tempDeviceId);
        SharedSegment pTempDeviceSegment =
            createLinearDeviceSegment(tempDeviceId, nDiskPages/50);
        pTempSegment =
            pSegmentFactory->newRandomAllocationSegment(
                pTempDeviceSegment,
                openMode.create);

        SharedSegment pDeviceSegment =
            createLinearDeviceSegment(dataDeviceId, nDiskPages);
        pVersionedRandomSegment =
            pSegmentFactory->newVersionedRandomAllocationSegment(
                pDeviceSegment,
                pTempSegment,
                openMode.create);
        pSnapshotRandomSegment =
            pSegmentFactory->newSnapshotRandomAllocationSegment(
                pVersionedRandomSegment,
                pVersionedRandomSegment,
                currCsn);
        setForceCacheUnmap(pSnapshotRandomSegment);

        // Set pRandomSegment so we can use PagingTestBase::testMultipleThreads
        // to exercise concurrent allocations/deallocations
        pRandomSegment = pSnapshotRandomSegment;

        nDiskPages /= 2;
        SharedSegment pLinearViewSegment =
            pSegmentFactory->newLinearViewSegment(
                pSnapshotRandomSegment,
                firstPageId);
        pLinearSegment = pLinearViewSegment;
    }

    void setForceCacheUnmap(SharedSegment pSegment)
    {
        // Force the snapshot segment to always execute its checkpoints during
        // a cache flush and unmap, in order to unmap these page from the cache
        if (pSegment) {
            SnapshotRandomAllocationSegment *pSnapshotSegment =
                SegmentFactory::dynamicCast<SnapshotRandomAllocationSegment *>(
                    pSegment);
            pSnapshotSegment->setForceCacheUnmap();
        }
    }

    virtual void closeStorage()
    {
        commitChanges(currCsn);
        closeLinearSegment();
        pRandomSegment.reset();
        closeSnapshotRandomSegment();
        if (pSnapshotRandomSegment2) {
            assert(pSnapshotRandomSegment2.unique());
            pSnapshotRandomSegment2.reset();
        }
        // Free leftover temp pages used during page versioning
        if (pVersionedRandomSegment) {
            VersionedRandomAllocationSegment *pVRSegment =
                SegmentFactory::dynamicCast<VersionedRandomAllocationSegment *>(
                    pVersionedRandomSegment);
            pVRSegment->freeTempPages();
        }
        closeVersionedRandomSegment();
        if (pTempSegment) {
            // Confirm that all temp pages have been freed.
            BOOST_REQUIRE(pTempSegment->getAllocatedSizeInPages() == 0);
            assert(pTempSegment.unique());
            pTempSegment.reset();
        }
        if (pTempDevice) {
            closeDevice(tempDeviceId, pTempDevice);
        }
        SegmentTestBase::closeStorage();
    }

    virtual void testAllocateAll()
    {
        SegmentTestBase::testAllocateAll();
        assert(firstPageId == NULL_PAGE_ID);
        LinearViewSegment *pLinearViewSegment =
            SegmentFactory::dynamicCast<LinearViewSegment *>(pLinearSegment);
        assert(pLinearViewSegment);
        firstPageId = pLinearViewSegment->getFirstPageId();
    }

    virtual void verifyPage(CachePage &page, uint x)
    {
        // If the pageId is a multiple of one of the csn's smaller than the
        // current csn, then the page should reflect the update made by
        // that smaller csn
        uint update = 0;
        for (int i = updatedCsns.size() - 1; i >= 0; i--) {
            if (updatedCsns[i] <= currCsn &&
                x % opaqueToInt(updatedCsns[i]) == 0)
            {
                update = opaqueToInt(updatedCsns[i]);
                break;
            }
        }
        SegmentTestBase::verifyPage(page, x + update);
    }

    virtual void fillPage(CachePage &page, uint x)
    {
        SegmentTestBase::fillPage(page, x + opaqueToInt(currCsn));
    }

    void commitChanges(TxnId commitCsn)
    {
        if (pSnapshotRandomSegment) {
            SnapshotRandomAllocationSegment *pSnapshotSegment =
                SegmentFactory::dynamicCast<SnapshotRandomAllocationSegment *>(
                    pSnapshotRandomSegment);
            if (commit) {
                pSnapshotSegment->commitChanges(commitCsn);
            } else {
                pSnapshotSegment->rollbackChanges();
            }
        }
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
            nDiskPages + nDiskPages/5 + ((nDiskPages % 5) ? 1 : 0));
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
            nDiskPages/3 + ((nDiskPages % 3) ? 1 : 0) +
            nDiskPages/5 + ((nDiskPages % 5) ? 1 : 0) +
            nDiskPages/7 + ((nDiskPages % 7) ? 1 : 0);

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
