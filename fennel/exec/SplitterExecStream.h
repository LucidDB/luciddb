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

#ifndef Fennel_SplitterExecStream_Included
#define Fennel_SplitterExecStream_Included

#include "fennel/exec/DiffluenceExecStream.h"

FENNEL_BEGIN_NAMESPACE

/**
 * SplitterExecStreamParams defines parameters for SplitterExecStream.
 */
struct SplitterExecStreamParams : public DiffluenceExecStreamParams
{
};
    
/**
 * SplitterExecStream is an adapter for converting the output of an
 * upstream producer for use by several downstream consumers.
 * SplitterExecStream itself does not provide any buffer to either
 * the producer or the consumer. The upstream producer writes its results into
 * its output buffer, and the downstream consumers read input from the same
 * buffer.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class SplitterExecStream : public DiffluenceExecStream
{
    /**
     * 0-based ordinal of next output from which to retrieve state
     */
    uint iOutput;
    
    /**
     * End of input buffer currently being consumed.
     */
    PConstBuffer pLastConsumptionEnd;
    
public:
    // implement ExecStream
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
    /**
     * Indicate to the consumer if the buffer is provided by this exec stream
     * which is the producer.
     */
    virtual ExecStreamBufProvision getOutputBufProvision() const;
};

FENNEL_END_NAMESPACE

#endif

// End SplitterExecStream.h
