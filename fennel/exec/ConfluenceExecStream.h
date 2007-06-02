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

#ifndef Fennel_ConfluenceExecStream_Included
#define Fennel_ConfluenceExecStream_Included

#include "fennel/exec/SingleOutputExecStream.h"

FENNEL_BEGIN_NAMESPACE

/**
 * ConfluenceExecStreamParams defines parameters for ConfluenceExecStream.
 */
struct ConfluenceExecStreamParams : virtual public SingleOutputExecStreamParams
{
};
    
/**
 * ConfluenceExecStream is an abstract base for any ExecStream with
 * multiple inputs and exactly one output.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class ConfluenceExecStream : virtual public SingleOutputExecStream
{
protected:
    std::vector<SharedExecStreamBufAccessor> inAccessors;

public:
    // implement ExecStream
    virtual void prepare(ConfluenceExecStreamParams const &params);
    virtual void setInputBufAccessors(
        std::vector<SharedExecStreamBufAccessor> const &inAccessors);
    virtual void open(bool restart);
    virtual ExecStreamBufProvision getInputBufProvision() const;
};

FENNEL_END_NAMESPACE

#endif

// End ConfluenceExecStream.h
