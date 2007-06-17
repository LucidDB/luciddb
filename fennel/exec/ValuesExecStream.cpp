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

#include "fennel/common/CommonPreamble.h"
#include "fennel/exec/ValuesExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void ValuesExecStream::prepare(ValuesExecStreamParams const &params)
{
    SingleOutputExecStream::prepare(params);
    pTupleBuffer = params.pTupleBuffer;
    bufSize = params.bufSize;
}

void ValuesExecStream::open(bool restart)
{
    SingleOutputExecStream::open(restart);
    produced = false;
}

ExecStreamResult ValuesExecStream::execute(
    ExecStreamQuantum const &quantum)
{
    if (produced || bufSize == 0) {
        pOutAccessor->markEOS();
        return EXECRC_EOS;
    }

    pOutAccessor->provideBufferForConsumption(
        pTupleBuffer.get(), pTupleBuffer.get() + bufSize);
    produced = true;
    return EXECRC_BUF_OVERFLOW;
}

ExecStreamBufProvision ValuesExecStream::getOutputBufProvision() const
{
    return BUFPROV_PRODUCER;
}

FENNEL_END_CPPFILE("$Id$");

// End ValuesExecStream.cpp
