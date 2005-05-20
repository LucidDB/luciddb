/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2004-2005 John V. Sichi
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

#ifndef Fennel_SegBufferExecStream_Included
#define Fennel_SegBufferExecStream_Included

#include "fennel/exec/ConduitExecStream.h"
#include "fennel/segment/SegStream.h"

FENNEL_BEGIN_NAMESPACE

/**
 * SegBufferExecStreamParams defines parameters for instantiating a
 * SegBufferExecStream.
 *
 *<p>
 *
 * TODO:  support usage of a SpillOutputStream.
 */
struct SegBufferExecStreamParams : public ConduitExecStreamParams
{
    /**
     * If true, buffer contents are preserved rather than deleted as they are
     * read.  This allows open(restart=true) to be used to perform multiple
     * iterations over the buffer.
     *
     *<p>
     *
     * TODO: support "tee" on the first pass.
     */
    bool multipass;
};
    
/**
 * SegBufferExecStream fully buffers its input (using segment storage as
 * specified in its parameters).  The first execute request causes all input
 * data to be stored, after which the original input stream is ignored and data
 * is returned from the stored buffer instead.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class SegBufferExecStream : public ConduitExecStream
{
    SegmentAccessor bufferSegmentAccessor;
    SharedSegOutputStream pByteOutputStream;
    SharedSegInputStream pByteInputStream;
    PageId firstPageId;
    bool multipass;
    SegStreamPosition restartPos;
    uint cbLastRead;

    void destroyBuffer();
    void openBufferForRead(bool destroy);
    
public:
    virtual void prepare(SegBufferExecStreamParams const &params);
    virtual void getResourceRequirements(
        ExecStreamResourceQuantity &minQuantity,
        ExecStreamResourceQuantity &optQuantity);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
    virtual void closeImpl();
    virtual ExecStreamBufProvision getOutputBufProvision() const;
    virtual ExecStreamBufProvision getInputBufProvision() const;
};

FENNEL_END_NAMESPACE

#endif

// End SegBufferExecStream.h
