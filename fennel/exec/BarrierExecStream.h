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

#ifndef Fennel_BarrierExecStream_Included
#define Fennel_BarrierExecStream_Included

#include "fennel/exec/ConfluenceExecStream.h"
#include "fennel/tuple/TupleData.h"

FENNEL_BEGIN_NAMESPACE

/**
 * BarrierExecStreamParams defines parameters for BarrierExecStream.
 */
struct BarrierExecStreamParams : public ConfluenceExecStreamParams
{
    
};
    
/**
 * BarrierExecStream is a synchronizing barrier to wait for the completion of
 * several upstream producers and generate a status output for the downstream
 * consumer.
 * Note 2005-12-14:
 * BarrierExecStream can provide a buffer for its producers to write into.
 * Currently a ScratchBuffer will be added between its producers and
 * BarrierExecStream.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class BarrierExecStream : public ConfluenceExecStream
{
    TupleData inputTuple;
    TupleData outputTuple;
    /**
     * 0-based ordinal of next input from which to read
     */
    uint iInput;
    
    /**
     * Number of rows processed.
     */
    RecordNum rowCount;
    
    
public:
    // implement ExecStream
    virtual void prepare(BarrierExecStreamParams const &params);    
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
};

FENNEL_END_NAMESPACE

#endif

// End BarrierExecStream.h
