/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
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
     * Segment supporting linear page allocation.  This is used for testing
     * the non-allocation portion of the Segment interface.
     */
    SharedSegment pLinearSegment;

    /**
     * (Optional) segment supporting random page allocation.  If present, this
     * is used for testing the allocation portion of the Segment interface.
     */
    SharedSegment pRandomSegment;

public:
    virtual void openStorage(DeviceMode openMode);

    virtual void openSegmentStorage(DeviceMode openMode);

    SharedSegment createLinearDeviceSegment(DeviceId deviceId,uint nPages);
    
    void closeLinearSegment();

    void closeRandomSegment();
    
    virtual void closeStorage();
    
    explicit SegStorageTestBase();
};

FENNEL_END_NAMESPACE

#endif

// End SegStorageTestBase.h

