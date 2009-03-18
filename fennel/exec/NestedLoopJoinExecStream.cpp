/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2007-2007 The Eigenbase Project
// Copyright (C) 2007-2007 Disruptive Tech
// Copyright (C) 2007-2007 LucidEra, Inc.
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
#include "fennel/exec/NestedLoopJoinExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void NestedLoopJoinExecStream::prepare(
    NestedLoopJoinExecStreamParams const &params)
{
    CartesianJoinExecStream::prepare(params);

    leftJoinKeys.assign(params.leftJoinKeys.begin(), params.leftJoinKeys.end());
    assert(leftJoinKeys.size() <= nLeftAttributes);
}

bool NestedLoopJoinExecStream::checkNumInputs()
{
    return (inAccessors.size() >= 2 && inAccessors.size() <= 3);
}

void NestedLoopJoinExecStream::open(bool restart)
{
    CartesianJoinExecStream::open(restart);

    if (!restart) {
        std::vector<NestedLoopJoinKey>::iterator it;
        for (it = leftJoinKeys.begin(); it != leftJoinKeys.end(); it++) {
            pDynamicParamManager->createParam(
                it->dynamicParamId,
                pLeftBufAccessor->getTupleDesc()[it->leftAttributeOrdinal]);
        }

        // Initialize this here and don't reset on restarts, since the
        // defined behavior is that the pre-processing is only done once
        // per stream graph execution, even if the stream is re-opened in
        // restart mode
        preProcessingDone = false;
    }
}

ExecStreamResult NestedLoopJoinExecStream::preProcessRightInput()
{
    // Create the temporary index by requesting production on the 3rd input
    if (!preProcessingDone && inAccessors.size() == 3) {
        if (inAccessors[2]->getState() != EXECBUF_EOS) {
            inAccessors[2]->requestProduction();
            return EXECRC_BUF_UNDERFLOW;
        }
    }
    preProcessingDone = true;
    return EXECRC_YIELD;
}

void NestedLoopJoinExecStream::processLeftInput()
{
    std::vector<NestedLoopJoinKey>::iterator it;
    for (it = leftJoinKeys.begin(); it != leftJoinKeys.end(); it++) {
        pDynamicParamManager->writeParam(
            it->dynamicParamId,
            outputData[it->leftAttributeOrdinal]);
    }
}

FENNEL_END_CPPFILE("$Id$");

// End NestedLoopJoinExecStream.cpp
