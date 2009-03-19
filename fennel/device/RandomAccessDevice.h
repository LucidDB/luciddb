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

#ifndef Fennel_RandomAccessDevice_Included
#define Fennel_RandomAccessDevice_Included

#include <boost/utility.hpp>

FENNEL_BEGIN_NAMESPACE

class RandomAccessRequest;
class RandomAccessRequestBinding;

/**
 * RandomAccessDevice is an interface representing any Device whose
 * stored bytes can be accessed at random in constant time.
 * For more information, see DeviceDesign.
 */
class RandomAccessDevice : boost::noncopyable
{
public:
    virtual ~RandomAccessDevice();

    /**
     * Gets the current size of this device.
     *
     * @return device size, in bytes
     */
    virtual FileSize getSizeInBytes() = 0;

    /**
     * Sets the size of this device, truncating or extending the device as
     * necessary.  Contents of extended portion are undefined.
     *
     * @param cbNew new device size in bytes
     */
    virtual void setSizeInBytes(FileSize cbNew) = 0;

    /**
     * Synchronously reads or writes a range of bytes from the device.
     * Never returns an error state; instead, a completion
     * notification method is called (via
     * RandomAccessRequestBinding::notifyTransferCompletion).
     *
     * @param request the encapsulated request parameters
     */
    virtual void transfer(
        RandomAccessRequest const &request) = 0;

    /**
     * Prepares for an asynchronous transfer by associating required information
     * about this device (e.g. file handle) with the given request.
     * The actual asynchronous transfer is initiated by a calling
     * DeviceAccessScheduler rather than this RandomAccessDevice itself.
     *
     * @param request the encapsulated request parameters
     */
    virtual void prepareTransfer(
        RandomAccessRequest &request) = 0;

    /**
     * Forces any buffered writes to permanent storage (e.g. fsync for a file
     * device).
     */
    virtual void flush() = 0;

    /**
     * @return OS-defined handle representing this device, or -1 for none
     */
    virtual int getHandle() = 0;
};

FENNEL_END_NAMESPACE

#endif

// End RandomAccessDevice.h
