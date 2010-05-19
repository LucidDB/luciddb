/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2004 John V. Sichi
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

#ifndef Fennel_SegBufferReader_Included
#define Fennel_SegBufferReader_Included

#include "fennel/common/ClosableObject.h"
#include "fennel/segment/SegStream.h"
#include "fennel/segment/SegmentAccessor.h"
#include "fennel/exec/ExecStreamDefs.h"

FENNEL_BEGIN_NAMESPACE

/**
 * SegBufferReader is a helper class that reads data that has previously been
 * buffered, and writes the data to its output stream.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FENNEL_EXEC_EXPORT SegBufferReader
    : public ClosableObject
{
    SharedExecStreamBufAccessor pOutAccessor;
    SegmentAccessor bufferSegmentAccessor;
    SharedSegInputStream pByteInputStream;
    PageId firstPageId;
    SegStreamPosition restartPos;
    uint cbLastRead;

public:
    /**
     * Creates a shared pointer to a new SegBufferReader object.
     *
     * @param pOutAccessor the output stream that will be written
     * @param bufferSegmentAccessor the segment accessor that will be used
     * to read the buffered data
     * @param firstPageId the pageId of the first buffer page
     */
    static SharedSegBufferReader newSegBufferReader(
        SharedExecStreamBufAccessor &pOutAccessor,
        SegmentAccessor const &bufferSegmentAccessor,
        PageId firstPageId);

    /**
     * Creates a new SegBufferReader object.
     *
     * @param pOutAccessorInit the output stream that will be written
     * @param bufferSegmentAccessorInit the segment accessor that will be used
     * to read the buffered data
     * @param firstPageIdInit the pageId of the first buffer page
     */
    explicit SegBufferReader(
        SharedExecStreamBufAccessor &pOutAccessorInit,
        SegmentAccessor const &bufferSegmentAccessorInit,
        PageId firstPageIdInit);

    /**
     * Initiates reads of the buffered data, beginning at the first page.
     *
     * @param destroy if true, destroy the buffered pages after they've been
     * read
     */
    void open(bool destroy);

    /**
     * Reads the buffered data.
     *
     * @return the execution stream result code indicating the state of the
     * data written to the output stream; EXECRC_BUF_OVERFLOW if data was
     * successfully written or EXECRC_EOS if all data has been written
     */
    ExecStreamResult read();

    // implement ClosableObject
    virtual void closeImpl();
};

FENNEL_END_NAMESPACE

#endif

// End SegBufferReader.h
