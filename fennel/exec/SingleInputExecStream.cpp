/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
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

#include "fennel/common/CommonPreamble.h"
#include "fennel/exec/SingleInputExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/exec/ExecStreamGraph.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void SingleInputExecStream::setOutputBufAccessors(
    std::vector<SharedExecStreamBufAccessor> const &outAccessors)
{
    assert(outAccessors.size() == 0);
}

void SingleInputExecStream::setInputBufAccessors(
    std::vector<SharedExecStreamBufAccessor> const &inAccessors)
{
    assert(inAccessors.size() == 1);
    pInAccessor = inAccessors[0];
}

void SingleInputExecStream::prepare(SingleInputExecStreamParams const &params)
{
    ExecStream::prepare(params);

    assert(pInAccessor);
    assert(pInAccessor->getProvision() == getInputBufProvision());
}

void SingleInputExecStream::open(bool restart)
{
    ExecStream::open(restart);
    if (restart) {
        // restart input
        pInAccessor->clear();
        pGraph->getStreamInput(getStreamId(),0)->open(true);
    }
}

ExecStreamBufProvision SingleInputExecStream::getInputBufProvision() const
{
    return BUFPROV_PRODUCER;
}

FENNEL_END_CPPFILE("$Id$");

// End SingleInputExecStream.cpp
