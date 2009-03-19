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

#ifndef Fennel_RandomAccessNullDevice_Included
#define Fennel_RandomAccessNullDevice_Included

#include "fennel/device/RandomAccessDevice.h"

FENNEL_BEGIN_NAMESPACE

class RandomAccessRequest;

/**
 * RandomAccessNullDevice is an implementation of RandomAccessDevice which acts
 * something like /dev/null, except that it does not allow any transfers at
 * all.
 */
class RandomAccessNullDevice : public RandomAccessDevice
{
public:
    /**
     * Creates a new null device.
     */
    explicit RandomAccessNullDevice();

// ----------------------------------------------------------------------
// Implementation of RandomAccessDevice interface (q.v.)
// ----------------------------------------------------------------------
    FileSize getSizeInBytes();
    void setSizeInBytes(FileSize cbNew);
    void transfer(RandomAccessRequest const &request);
    void prepareTransfer(RandomAccessRequest &request);
    void flush();
    int getHandle();
};

FENNEL_END_NAMESPACE

#endif

// End RandomAccessNullDevice.h
