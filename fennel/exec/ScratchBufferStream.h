/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 John V. Sichi.
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

#ifndef Fennel_ScratchBufferStream_Included
#define Fennel_ScratchBufferStream_Included

#include "fennel/exec/ConduitExecStream.h"
#include "fennel/segment/SegPageLock.h"

FENNEL_BEGIN_NAMESPACE

/**
 * ScratchBufferStream is an adapter for converting the output of an upstream
 * BUFPROV_CONSUMER producer for use by a downstream BUFPROV_PRODUCER consumer.
 * The implementation works by allocating a scratch buffer; the
 * upstream producer writes its results into this buffer, and the
 * downstream consumer reads input from this buffer.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class ScratchBufferStream : public ConduitExecStream
{
    SegmentAccessor scratchAccessor;
    SegPageLock bufferLock;
    
public:
    // implement ExecStream
    virtual void prepare(ExecStreamParams const &params);
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

// End ScratchBufferStream.h
