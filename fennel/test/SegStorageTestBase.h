/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
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

#ifndef Fennel_SegStorageTestBase_Included
#define Fennel_SegStorageTestBase_Included

#include "fennel/test/CacheTestBase.h"
#include "fennel/segment/SegmentFactory.h"

FENNEL_BEGIN_NAMESPACE

/**
 * SegStorageTestBase is a common base for any test which depends on the
 * Segment interface.
 */
class SegStorageTestBase : virtual public CacheTestBase
{
protected:
    SharedSegmentFactory pSegmentFactory;
    
    /**
     * Segment supporting linear page allocation.
     */
    SharedSegment pLinearSegment;

    /**
     * (Optional) segment supporting random page allocation.
     */
    SharedSegment pRandomSegment;

    /**
     * (Optional) segment supporting versioned random page allocation.
     */
    SharedSegment pVersionedRandomSegment;

    /**
     * (Optional) segment supporting snapshot random page allocation.
     */
    SharedSegment pSnapshotRandomSegment;

public:
    virtual void openStorage(DeviceMode openMode);

    virtual void openSegmentStorage(DeviceMode openMode);

    virtual void openRandomSegment();

    SharedSegment createLinearDeviceSegment(DeviceId deviceId,uint nPages);
    
    void closeLinearSegment();

    void closeRandomSegment();

    void closeVersionedRandomSegment();

    void closeSnapshotRandomSegment();

    virtual void closeStorage();
    
    explicit SegStorageTestBase();
};

FENNEL_END_NAMESPACE

#endif

// End SegStorageTestBase.h

