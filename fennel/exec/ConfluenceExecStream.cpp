/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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

#include "fennel/common/CommonPreamble.h"
#include "fennel/exec/ConfluenceExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/exec/ExecStreamGraph.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void ConfluenceExecStream::setInputBufAccessors(
    std::vector<SharedExecStreamBufAccessor> const &inAccessorsInit)
{
    inAccessors = inAccessorsInit;
}

void ConfluenceExecStream::prepare(ConfluenceExecStreamParams const &params)
{
    SingleOutputExecStream::prepare(params);
    
    for (uint i = 0; i < inAccessors.size(); ++i) {
        assert(inAccessors[i]->getProvision() == getInputBufProvision());
    }
}

void ConfluenceExecStream::open(bool restart)
{
    SingleOutputExecStream::open(restart);
    if (restart) {
        // restart inputs
        for (uint i = 0; i < inAccessors.size(); ++i) {
            inAccessors[i]->clear();
            pGraph->getStreamInput(getStreamId(),i)->open(true);
        }
    }
}

ExecStreamBufProvision ConfluenceExecStream::getInputBufProvision() const
{
    return BUFPROV_PRODUCER;
}

FENNEL_END_CPPFILE("$Id$");

// End ConfluenceExecStream.cpp
