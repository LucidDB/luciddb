/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2004-2007 John V. Sichi
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

#ifndef Fennel_ConduitExecStream_Included
#define Fennel_ConduitExecStream_Included

#include "fennel/exec/SingleOutputExecStream.h"
#include "fennel/exec/SingleInputExecStream.h"

FENNEL_BEGIN_NAMESPACE

/**
 * ConduitExecStreamParams defines parameters for ConduitExecStream.
 */
struct ConduitExecStreamParams
    : virtual public SingleInputExecStreamParams, 
        virtual public SingleOutputExecStreamParams
{
};
    
/**
 * ConduitExecStream is an abstract base for any ExecStream with exactly
 * one input and one output.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class ConduitExecStream
    : virtual public SingleInputExecStream, 
        virtual public SingleOutputExecStream
{
protected:
    /**
     * Checks the state of the input and output buffers.  If input empty,
     * requests production.  If input EOS, propagates that to output buffer.
     * If output full, returns EXECRC_OVERFLOW.
     *
     * @return result of precheck; anything but EXECRC_YIELD indicates
     * that execution should terminate immediately with returned code
     */
    ExecStreamResult precheckConduitBuffers();
    
public:
    // implement ExecStream
    virtual void setOutputBufAccessors(
        std::vector<SharedExecStreamBufAccessor> const &outAccessors);
    virtual void setInputBufAccessors(
        std::vector<SharedExecStreamBufAccessor> const &inAccessors);
    virtual void prepare(ConduitExecStreamParams const &params);
    virtual void open(bool restart);
};

FENNEL_END_NAMESPACE

#endif

// End ConduitExecStream.h
