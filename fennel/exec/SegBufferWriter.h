/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2004-2009 John V. Sichi
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

#ifndef Fennel_SegBufferWriter_Included
#define Fennel_SegBufferWriter_Included

#include "fennel/common/ClosableObject.h"
#include "fennel/segment/SegStream.h"
#include "fennel/segment/SegmentAccessor.h"
#include "fennel/exec/ExecStreamDefs.h"

FENNEL_BEGIN_NAMESPACE

/**
 * SegBufferWriter is a helper class that reads an input stream and writes
 * the data into a buffer.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class SegBufferWriter : public ClosableObject
{
    SharedExecStreamBufAccessor pInAccessor;
    SegmentAccessor bufferSegmentAccessor;
    SharedSegOutputStream pByteOutputStream;
    PageId firstPageId;

    /**
     * True if the buffer pages written should be destroyed on close.
     * Otherwise, it's the responsibility of the caller to destroy the
     * buffer pages.
     */
    bool destroyOnClose;

public:
    /**
     * Creates a shared pointer to a new SegBufferWriter object.
     *
     * @param pInAccessor the input stream that will be read
     * @param bufferSegmentAccessor the segment accessor that will be used
     * to write the buffered data
     * @param destroyOnClose true if the data written needs to be destroyed
     * when the object is destroyed; otherwise, it's the responsibility of
     * the caller to destroy the buffer pages
     */
    static SharedSegBufferWriter newSegBufferWriter(
        SharedExecStreamBufAccessor &pInAccessor,
        SegmentAccessor const &bufferSegmentAccessor,
        bool destroyOnClose);

    /**
     * Creates a new SegBufferWriter object.
     *
     * @param pInAccessorInit the input stream that will be read
     * @param bufferSegmentAccessorInit the segment accessor that will be used
     * to write the buffered data
     * @param destroyOnCloseInit true if the data written needs to be destroyed
     * when the object is destroyed; otherwise, it's the responsibility of
     * the caller to destroy the buffer pages
     */
    explicit SegBufferWriter(
        SharedExecStreamBufAccessor &pInAccessorInit,
        SegmentAccessor const &bufferSegmentAccessorInit,
        bool destroyOnCloseInit);

    /**
     * Reads data from the input stream and buffers it.
     */
    ExecStreamResult write();

    /*
     * @return the pageId of the first buffer page, once writes have been
     * initiated
     */
    PageId getFirstPageId();

    // implement ClosableObject
    virtual void closeImpl();
};

FENNEL_END_NAMESPACE

#endif

// End SegBufferWriter.h
