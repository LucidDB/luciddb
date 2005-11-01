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

#ifndef Fennel_RandomAccessRequest_Included
#define Fennel_RandomAccessRequest_Included

#include "fennel/common/IntrusiveList.h"

#ifdef HAVE_AIO_H
#include <aio.h>
struct aiocb;
#endif

#ifdef HAVE_LIBAIO_H
#include <libaio.h>
struct iocb;
#endif

#ifdef __MINGW32__
#include <windows.h>
#endif

FENNEL_BEGIN_NAMESPACE

class RandomAccessDevice;

/**
 * RandomAccessRequestBinding binds a RandomAccessRequest to a particular
 * memory location being read from or written to.
 */
class RandomAccessRequestBinding : public IntrusiveListNode
#ifdef USE_AIO_H
, public aiocb
#endif
#ifdef USE_LIBAIO_H
, public iocb
#endif
#ifdef __MINGW32__
, public OVERLAPPED
#endif
{
public:
    explicit RandomAccessRequestBinding();
    virtual ~RandomAccessRequestBinding();
    
    /**
     * @return memory address where transfer should start.
     */
    virtual PBuffer getBuffer() const = 0;

    /**
     * @return number of contiguous bytes from getBuffer() to be used for
     * transfer.
     */
    virtual uint getBufferSize() const = 0;
    
    /**
     * Receives notification when a transfer completes.
     *
     * @param bSuccess true if the full buffer size was successfully
     * transferred for this binding
     */
    virtual void notifyTransferCompletion(bool bSuccess) = 0;
};

/**
 * RandomAccessRequest represents one logical unit of I/O against a
 * RandomAccessDevice.  Currently supported operations are reads and writes
 * spanning a contiguous range of byte offsets within the device.  The
 * RandomAccessRequestBinding memory locations need not be contiguous
 * (scatter/gather).
 */
class RandomAccessRequest 
{
public:
    enum Type { READ, WRITE};
    typedef IntrusiveList<RandomAccessRequestBinding> BindingList;
    typedef IntrusiveListIter<RandomAccessRequestBinding> BindingListIter;
    typedef IntrusiveListMutator<RandomAccessRequestBinding>
    BindingListMutator;

    /**
     * The device to be accessed.  It's a pointer rather than a reference so
     * that RandomAccessRequest behaves as a concrete class.
     */
    RandomAccessDevice *pDevice;

    /**
     * Byte offset within device at which access should start.
     */
    FileSize cbOffset;

    /**
     * Number of bytes to be transferred.
     */
    FileSize cbTransfer;

    /**
     * Access type:  READ for transfer from device to memory; WRITE for
     * transfer from memory to device.
     */
    Type type;

    /**
     * Bindings for memory source or destination and notifications.
     */
    BindingList bindingList;

    /**
     * Executes this request.  (Satisfies the ThreadPool Task signature,
     * allowing instances of this class to be submitted directly as a Task by
     * ThreadPoolScheduler).
     */
    void execute();
};

FENNEL_END_NAMESPACE

#endif

// End RandomAccessRequest.h
