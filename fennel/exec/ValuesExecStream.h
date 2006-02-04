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

#ifndef Fennel_ValuesExecStream_Included
#define Fennel_ValuesExecStream_Included

#include "fennel/exec/SingleOutputExecStream.h"

FENNEL_BEGIN_NAMESPACE

/**
 * ValuesExecStreamParams defines parameters for ValuesExecStream.
 */
struct ValuesExecStreamParams : public SingleOutputExecStreamParams
{
    /**
     * Number of bytes in buffer
     */
    uint bufSize;

    /**
     * Buffer containing tuples that stream will produce
     */
    PBuffer pTupleBuffer;
};

/**
 * ValuesExecStream passes a buffer of tuples passed in as a parameter into
 * the stream on to its consumer to process.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
class ValuesExecStream : public SingleOutputExecStream
{
    /**
     * Number of bytes in input buffer
     */
    uint bufSize;
    
    /**
     * Pointer to start of input tuple buffer
     */
    PBuffer pTupleBuffer;
    
    /**
     * True if stream has passed on its buffer to its consumer
     */
    bool produced;

public:
    // implement ExecStream
    virtual void prepare(ValuesExecStreamParams const &params);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
    virtual ExecStreamBufProvision getOutputBufProvision() const;
};

FENNEL_END_NAMESPACE

#endif

// End ValuesExecStream.h
