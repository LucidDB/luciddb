/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 1999-2009 John V. Sichi
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

#ifndef Fennel_SegmentTestBase_Included
#define Fennel_SegmentTestBase_Included

#include "fennel/test/SegStorageTestBase.h"
#include "fennel/test/PagingTestBase.h"

FENNEL_BEGIN_NAMESPACE

/**
 * SegmentTestBase is a common base for any test of an implementation of the
 * Segment interface.
 */
class FENNEL_TEST_EXPORT SegmentTestBase
    : virtual public SegStorageTestBase,
        virtual public PagingTestBase
{
protected:
    /**
     * Queue of allocated pages waiting to be freed.
     */
    std::vector<PageId> freeablePages;

    /**
     * Mutex protecting freeablePages.
     */
    StrictMutex freeablePagesMutex;

    /**
     * PageOwnerId to use when allocating pages.
     */
    PageOwnerId objId;

public:
    virtual void openStorage(DeviceMode openMode);

    virtual CachePage *lockPage(OpType opType,uint iPage);

    virtual void unlockPage(CachePage &page,LockMode lockMode);

    virtual void prefetchPage(uint iPage);

    virtual void prefetchBatch(uint,uint);

    virtual void testAllocate();

    virtual void testDeallocate();

    virtual void testCheckpoint();

    explicit SegmentTestBase();

    void testSingleThread();
};

FENNEL_END_NAMESPACE

#endif

// End SegmentTestBase.h

