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

#ifndef Fennel_RandomAccessFileDevice_Included
#define Fennel_RandomAccessFileDevice_Included

#include "fennel/device/RandomAccessDevice.h"
#include "fennel/device/FileDevice.h"

FENNEL_BEGIN_NAMESPACE

class RandomAccessRequest;

/**
 * RandomAccessFileDevice is an implementation of RandomAccessDevice in terms
 * of a FileDevice.
 */
class RandomAccessFileDevice : public RandomAccessDevice, public FileDevice
{
public:
    /**
     * Opens a file device for random access, specifying an initial size on
     * creation.
     *
     * @param filename path to file
     * @param mode modifiers for how to open file
     * @param initialSize the initial size (in bytes) of the device, if
     * creating a new file
     */
    explicit RandomAccessFileDevice(
        std::string filename,
        DeviceMode mode,
        FileSize initialSize);

    /**
     * Opens a file device for random access.
     *
     * @param filename path to file
     * @param mode modifiers for how to open file
     */
    explicit RandomAccessFileDevice(
        std::string filename,
        DeviceMode mode);

// ----------------------------------------------------------------------
// Implementation of RandomAccessDevice interface (q.v.)
// ----------------------------------------------------------------------
    virtual FileSize getSizeInBytes();
    virtual void setSizeInBytes(FileSize cbNew);
    virtual void transfer(RandomAccessRequest const &request);
    virtual void prepareTransfer(RandomAccessRequest &request);
    virtual void flush();
    virtual int getHandle();
};

FENNEL_END_NAMESPACE

#endif

// End RandomAccessFileDevice.h
