/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/

#ifndef Fennel_VersionedSegment_Included
#define Fennel_VersionedSegment_Included

#include "fennel/segment/DelegatingSegment.h"
#include "fennel/synch/SynchObj.h"
#include "fennel/cache/FuzzyCheckpointSet.h"
#include "fennel/common/PseudoUuid.h"

#include <boost/crc.hpp>

FENNEL_BEGIN_NAMESPACE

class WALSegment;

/**
 * VersionedSegment provides versioned updates to an underlying data segment by
 * keeping before-images of modified pages in a separate write-ahead log
 * segment.  See <a href="structSegmentDesign.html#VersionedSegment">the design
 * docs</a> for more detail.
 */
class FENNEL_SEGMENT_EXPORT VersionedSegment
    : public DelegatingSegment
{
    friend class SegmentFactory;

    bool inRecovery;
    PseudoUuid onlineUuid;
    SegVersionNum versionNumber;
    PageId oldestLogPageId;
    PageId newestLogPageId;
    PageId lastCheckpointLogPageId;
    StrictMutex mutex;
    SharedSegment logSegment;
    WALSegment *pWALSegment;
    FuzzyCheckpointSet fuzzyCheckpointSet;

    // TODO:  use a 64-bit crc instead
    boost::crc_32_type crcComputer;

    PageMap dataToLogMap;

    explicit VersionedSegment(
        SharedSegment dataSegment,
        SharedSegment logSegment,
        PseudoUuid const &onlineUuid,
        SegVersionNum versionNumber);

    uint64_t computeChecksum(void const *pPageData);

public:
    virtual ~VersionedSegment();

    /**
     * Recovers to a specific version from the log.
     *
     * @param pDelegatingSegment segment from which pages to recover originate
     *
     * @param firstLogPageId starting PageId in log segment
     *
     * @param versionNumber version number to recover to, or MAXU
     * to use current version number
     */
    void recover(
        SharedSegment pDelegatingSegment,
        PageId firstLogPageId,
        SegVersionNum versionNumber = MAXU);

    /**
     * Recovers to a specific version from the log and resets the online uuid.
     *
     * @param pDelegatingSegment segment from which pages to recover originate
     *
     * @param firstLogPageId starting PageId in log segment
     *
     * @param versionNumber version number to recover to
     *
     * @param onlineUuid online uuid corresponding to the recovered instance
     */
    void recover(
        SharedSegment pDelegatingSegment,
        PageId firstLogPageId,
        SegVersionNum versionNumber,
        PseudoUuid const &onlineUuid);

    /**
     * Prepares for "online" recovery, meaning a revert back to the last
     * checkpointed version.  Call getOnlineRecoveryPageId() first.
     */
    void prepareOnlineRecovery();

    /**
     * @return the PageId of the oldest log page still needed for recovery
     * after a crash
     */
    PageId getRecoveryPageId() const;

    /**
     * @return the PageId of the oldest log page still needed for recovery
     * while online
     */
    PageId getOnlineRecoveryPageId() const;

    /**
     * Gets the version number of a locked page.
     *
     * @param page the locked page
     *
     * @return the page version number
     */
    SegVersionNum getPageVersion(CachePage &page);

    /**
     * @return the current version number for this segment
     */
    SegVersionNum getVersionNumber() const;

    /**
     * @return the WAL segment
     */
    SharedSegment getLogSegment() const;

    /**
     * Deallocates any old log pages which have become irrelevant after a
     * checkpoint.  Divorced from the checkpoint operation itself to
     * make possible atomicity as part of compound checkpoint sequences.
     *
     * @param checkpointType the CheckpointType passed to the last checkpoint()
     * call
     */
    void deallocateCheckpointedLog(CheckpointType checkpointType);

    // implement the Segment interface
    virtual void deallocatePageRange(PageId startPageId, PageId endPageId);
    virtual void delegatedCheckpoint(
        Segment &delegatingSegment,CheckpointType checkpointType);

    // implement the MappedPageListener interface
    virtual void notifyPageDirty(CachePage &page,bool bDataValid);
    virtual bool canFlushPage(CachePage &page);
};

FENNEL_END_NAMESPACE

#endif

// End VersionedSegment.h
