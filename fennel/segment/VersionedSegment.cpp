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
#include "fennel/segment/VersionedSegment.h"
#include "fennel/segment/WALSegment.h"
#include "fennel/segment/SegPageLock.h"
#include "fennel/segment/SegmentFactory.h"
#include "fennel/cache/CachePage.h"

FENNEL_BEGIN_CPPFILE("$Id$");

// NOTE:  read comments on struct StoredNode before modifying
// VersionedPageFooter

/**
 * Information stored in the footer of each page of a VersionedSegment.  Note
 * that some of this information is only relevant in old page versions stored
 * in the log segment, but the footer size has to be reserved for logging.
 */
struct VersionedPageFooter
{
    /**
     * For an old version stored in the log, this is the PageId of the latest
     * version in the data segment.  For the latest version in the data
     * segment, this is NULL_PAGE_ID.
     */
    PageId dataPageId;

    /**
     * The version number of this page.
     */
    SegVersionNum versionNumber;

    /**
     * The UUID of the online instance that logged this version.
     */
    PseudoUuid onlineUuid;

    /**
     * For an old version stored in the log, this is a checksum of the logged
     * contents, used to find the last good page during recovery.  For the
     * latest version in the data segment, this is 0.
     */
    uint64_t checksum;
};

VersionedSegment::VersionedSegment(
    SharedSegment dataSegmentInit,
    SharedSegment logSegmentInit,
    PseudoUuid const &onlineUuidInit,
    SegVersionNum versionNumberInit)
    : DelegatingSegment(dataSegmentInit)
{
    logSegment = logSegmentInit;
    pWALSegment = SegmentFactory::dynamicCast<WALSegment *>(logSegment);
    assert(pWALSegment);

    setUsablePageSize(
        DelegatingSegment::getUsablePageSize()
        - sizeof(VersionedPageFooter));

    onlineUuid = onlineUuidInit;
    versionNumber = versionNumberInit;
    oldestLogPageId = NULL_PAGE_ID;
    newestLogPageId = NULL_PAGE_ID;
    lastCheckpointLogPageId = NULL_PAGE_ID;
    inRecovery = false;
}

VersionedSegment::~VersionedSegment()
{
    pWALSegment = NULL;
    assert(dataToLogMap.empty());
}

// TODO: comments on checkpoint concurrency requirements

void VersionedSegment::delegatedCheckpoint(
    Segment &delegatingSegment,CheckpointType checkpointType)
{
    if (checkpointType != CHECKPOINT_DISCARD) {
        // TODO:  for a fuzzy checkpoint, only need to force the log pages for
        // data pages that are going to be flushed
        logSegment->checkpoint(checkpointType);
        assert(pWALSegment->getMinDirtyPageId() == NULL_PAGE_ID);
    }
    if (checkpointType == CHECKPOINT_FLUSH_FUZZY) {
        MappedPageListenerPredicate pagePredicate(delegatingSegment);
        fuzzyCheckpointSet.setDelegatePagePredicate(pagePredicate);
        pCache->checkpointPages(fuzzyCheckpointSet,checkpointType);
        fuzzyCheckpointSet.finishCheckpoint();
        if (lastCheckpointLogPageId != NULL_PAGE_ID) {
            oldestLogPageId = logSegment->getPageSuccessor(
                lastCheckpointLogPageId);
        } else {
            oldestLogPageId = NULL_PAGE_ID;
        }
    } else {
        DelegatingSegment::delegatedCheckpoint(
            delegatingSegment,checkpointType);
        fuzzyCheckpointSet.clear();
        oldestLogPageId = NULL_PAGE_ID;
    }

    if (checkpointType == CHECKPOINT_DISCARD) {
        logSegment->checkpoint(checkpointType);
    }

    StrictMutexGuard mutexGuard(mutex);
    ++versionNumber;
    dataToLogMap.clear();
}

void VersionedSegment::deallocateCheckpointedLog(CheckpointType checkpointType)
{
    if (checkpointType == CHECKPOINT_FLUSH_FUZZY) {
        if (lastCheckpointLogPageId != NULL_PAGE_ID) {
            logSegment->deallocatePageRange(
                NULL_PAGE_ID,lastCheckpointLogPageId);
            if (lastCheckpointLogPageId == newestLogPageId) {
                newestLogPageId = NULL_PAGE_ID;
            }
        }
    } else {
        logSegment->deallocatePageRange(NULL_PAGE_ID,NULL_PAGE_ID);
        newestLogPageId = NULL_PAGE_ID;
    }
    lastCheckpointLogPageId = newestLogPageId;
}

void VersionedSegment::deallocatePageRange(
    PageId startPageId,PageId endPageId)
{
    // TODO:  support real truncations?
    assert(startPageId == endPageId);
    assert(startPageId != NULL_PAGE_ID);

    // TODO:  need to log copy of deallocated page
    DelegatingSegment::deallocatePageRange(startPageId,endPageId);
}

void VersionedSegment::notifyPageDirty(CachePage &page,bool bDataValid)
{
    DelegatingSegment::notifyPageDirty(page,bDataValid);

    if (inRecovery) {
        // REVIEW jvs 8-Aug-2006: It would be nice to assert instead.  But we
        // can get here in online recovery when we replace pages which were
        // abandoned but not discarded.
        return;
    }

    VersionedPageFooter *pDataFooter = reinterpret_cast<VersionedPageFooter *>(
        getWritableFooter(page));

    if (!bDataValid) {
        // newly allocated page
        pDataFooter->dataPageId = NULL_PAGE_ID;
        pDataFooter->onlineUuid.generateInvalid();
        pDataFooter->versionNumber = versionNumber;
        pDataFooter->checksum = 0;
        return;
    }

    assert(pDataFooter->versionNumber <= versionNumber);
    if (pDataFooter->versionNumber == versionNumber) {
        // already logged this page
        return;
    }

    // write before-image to the log
    SegmentAccessor logSegmentAccessor(logSegment,pCache);
    SegPageLock logPageLock(logSegmentAccessor);
    PageId logPageId = logPageLock.allocatePage();

    // REVIEW:  what if there's other footer information to copy?

    // TODO:  remember logPageId in version map
    PBuffer pLogPageBuffer = logPageLock.getPage().getWritableData();
    memcpy(
        pLogPageBuffer,
        page.getReadableData(),
        getUsablePageSize());
    VersionedPageFooter *pLogFooter = reinterpret_cast<VersionedPageFooter *>(
        getWritableFooter(logPageLock.getPage()));
    pLogFooter->versionNumber = versionNumber - 1;
    pLogFooter->onlineUuid = onlineUuid;
    PageId dataPageId = DelegatingSegment::translateBlockId(
        page.getBlockId());
    pLogFooter->dataPageId = dataPageId;

    pLogFooter->checksum = computeChecksum(pLogPageBuffer);

    // record new version number for soon-to-be-modified data page
    pDataFooter->versionNumber = versionNumber;

    // tell the cache that the log page is a good candidate for victimization
    pCache->nicePage(logPageLock.getPage());

    StrictMutexGuard mutexGuard(mutex);
    dataToLogMap[dataPageId] = logPageId;
    if ((newestLogPageId == NULL_PAGE_ID) || (logPageId > newestLogPageId)) {
        newestLogPageId = logPageId;
    }
    if ((oldestLogPageId == NULL_PAGE_ID) || (logPageId < oldestLogPageId)) {
        oldestLogPageId = logPageId;
    }
}

SegVersionNum VersionedSegment::computeChecksum(void const *pPageData)
{
    crcComputer.reset();
    crcComputer.process_bytes(pPageData,getUsablePageSize());
    return crcComputer.checksum();
}

bool VersionedSegment::canFlushPage(CachePage &page)
{
    // this implements the WAL constraint

    PageId minLogPageId = pWALSegment->getMinDirtyPageId();
    if (minLogPageId == NULL_PAGE_ID) {
        return DelegatingSegment::canFlushPage(page);
    }

    StrictMutexGuard mutexGuard(mutex);
    PageId dataPageId = DelegatingSegment::translateBlockId(
        page.getBlockId());
    PageMapConstIter pLogPageId = dataToLogMap.find(dataPageId);
    if (pLogPageId == dataToLogMap.end()) {
        // newly allocated page
        return DelegatingSegment::canFlushPage(page);
    }
    PageId logPageId = pLogPageId->second;
    if (logPageId >= minLogPageId) {
        return false;
    }
    return DelegatingSegment::canFlushPage(page);
}

void VersionedSegment::prepareOnlineRecovery()
{

    // For simplicity, force entire log out to disk first, but don't discard
    // it, since we're about to read it during recovery.
    logSegment->checkpoint(CHECKPOINT_FLUSH_ALL);

    StrictMutexGuard mutexGuard(mutex);

    dataToLogMap.clear();
    oldestLogPageId = NULL_PAGE_ID;
}

void VersionedSegment::recover(
    SharedSegment pDelegatingSegment,
    PageId firstLogPageId,
    SegVersionNum versionNumberInit,
    PseudoUuid const &onlineUuidInit)
{
    onlineUuid = onlineUuidInit;
    recover(pDelegatingSegment, firstLogPageId, versionNumberInit);
}

void VersionedSegment::recover(
    SharedSegment pDelegatingSegment,
    PageId firstLogPageId,
    SegVersionNum versionNumberInit)
{
    assert(dataToLogMap.empty());
    assert(pWALSegment->getMinDirtyPageId() == NULL_PAGE_ID);

    inRecovery = true;

    if (!isMAXU(versionNumberInit)) {
        versionNumber = versionNumberInit;
    }

    // The conventional thing to do is to scan forward to find the log end, and
    // then recover backwards, guaranteeing that earlier shadows replace later
    // shadows (in case of a fuzzy checkpoint).  Instead, we keep track of
    // which pages have already been recovered and skip them if they are
    // encountered again.
    std::hash_set<PageId> recoveredPageSet;

    // TODO:  use PageIters

    // TODO:  what about when one shadow log stores pages for multiple
    // VersionedSegments?
    SegmentAccessor logSegmentAccessor(logSegment,pCache);
    SegmentAccessor dataSegmentAccessor(pDelegatingSegment,pCache);
    for (; firstLogPageId != NULL_PAGE_ID;
         firstLogPageId = logSegment->getPageSuccessor(firstLogPageId))
    {
        SegPageLock logPageLock(logSegmentAccessor);
        logPageLock.lockShared(firstLogPageId);
        if (!logPageLock.getPage().isDataValid()) {
            break;
        }
        PConstBuffer pLogPageBuffer = logPageLock.getPage().getReadableData();
        VersionedPageFooter const *pLogFooter =
            reinterpret_cast<VersionedPageFooter const *>(
                getReadableFooter(logPageLock.getPage()));
        if (pLogFooter->checksum != computeChecksum(pLogPageBuffer)) {
            break;
        }
        if (pLogFooter->onlineUuid != onlineUuid) {
            break;
        }
        assert(pLogFooter->versionNumber < (versionNumber + 2));
        if (pLogFooter->versionNumber < versionNumber) {
            continue;
        }
        if (recoveredPageSet.find(pLogFooter->dataPageId)
            != recoveredPageSet.end())
        {
            assert(pLogFooter->versionNumber > versionNumber);
            continue;
        }

        SegPageLock dataPageLock(dataSegmentAccessor);
        dataPageLock.lockExclusive(pLogFooter->dataPageId);
        memcpy(
            dataPageLock.getPage().getWritableData(),
            pLogPageBuffer,
            getFullPageSize());
        recoveredPageSet.insert(pLogFooter->dataPageId);
    }

    inRecovery = false;
}

SegVersionNum VersionedSegment::getPageVersion(CachePage &page)
{
    VersionedPageFooter const *pFooter =
        reinterpret_cast<VersionedPageFooter const *>(
            getReadableFooter(page));
    return pFooter->versionNumber;
}

SegVersionNum VersionedSegment::getVersionNumber() const
{
    return versionNumber;
}

SharedSegment VersionedSegment::getLogSegment() const
{
    return logSegment;
}

PageId VersionedSegment::getOnlineRecoveryPageId() const
{
    return oldestLogPageId;
}

PageId VersionedSegment::getRecoveryPageId() const
{
    if (oldestLogPageId == NULL_PAGE_ID) {
        // if we've truncated the log, then recovery should start from the
        // first new shadow page after a crash
        return FIRST_LINEAR_PAGE_ID;
    } else {
        return oldestLogPageId;
    }
}

FENNEL_END_CPPFILE("$Id$");

// End VersionedSegment.cpp
