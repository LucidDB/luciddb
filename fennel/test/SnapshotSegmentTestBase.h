/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
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

#ifndef Fennel_SnapshotSegmentTestBase_Included
#define Fennel_SnapshotSegmentTestBase_Included

#include "fennel/test/SegmentTestBase.h"

FENNEL_BEGIN_NAMESPACE

/**
 * SnapshotSegmentTestBase is a common base for any test that uses a
 * SnapshotRandomAllocationSegment for storage.
 */
class SnapshotSegmentTestBase : virtual public SegmentTestBase
{
protected:
    uint nDiskPagesTotal;
    PageId firstPageId;
    DeviceId tempDeviceId;
    SharedRandomAccessDevice pTempDevice;
    SharedSegment pTempSegment;
    TxnId currCsn;
    std::vector<TxnId> updatedCsns;
    bool commit;
    SharedSegment pSnapshotRandomSegment2;

    /**
     * Forces the underlying snapshot segment to always execute its checkpoints
     * during a cache flush and unmap.
     */
    void setForceCacheUnmap(SharedSegment pSegment);

    /**
     * Commits transactions associated with a specified csn.
     *
     * @param commitCsn the specified csn
     */
    void commitChanges(TxnId commitCsn);

public:
    explicit SnapshotSegmentTestBase();

    virtual void testCaseSetUp();
    virtual void openSegmentStorage(DeviceMode openMode);
    virtual void closeStorage();
    virtual void testAllocateAll();
    virtual void verifyPage(CachePage &page, uint x);
    virtual void fillPage(CachePage &page, uint x);
};

FENNEL_END_NAMESPACE

#endif

// End SnapshotSegmentTestBase.h
