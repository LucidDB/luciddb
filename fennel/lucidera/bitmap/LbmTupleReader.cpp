/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Copyright (C) 2005-2009 The Eigenbase Project
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
#include "fennel/lucidera/bitmap/LbmTupleReader.h"
#include "fennel/exec/ExecStreamBufAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

LbmTupleReader::~LbmTupleReader()
{
}

void LbmStreamTupleReader::init(
    SharedExecStreamBufAccessor &pInAccessorInit,
    TupleData &bitmapSegTuple)
{
    pInAccessor = pInAccessorInit;
    pInputTuple = &bitmapSegTuple;
}

ExecStreamResult LbmStreamTupleReader::read(PTupleData &pTupleData)
{
    if (pInAccessor->getState() == EXECBUF_EOS) {
        return EXECRC_EOS;
    }

    // consume the previous input if there was one
    if (pInAccessor->isTupleConsumptionPending()) {
        pInAccessor->consumeTuple();
    }
    if (!pInAccessor->demandData()) {
        return EXECRC_BUF_UNDERFLOW;
    }

    pInAccessor->unmarshalTuple(*pInputTuple);
    return EXECRC_YIELD;
}

void LbmSingleTupleReader::init(TupleData &bitmapSegTuple)
{
    hasTuple = true;
    pInputTuple = &bitmapSegTuple;
}

ExecStreamResult LbmSingleTupleReader::read(PTupleData &pTupleData)
{
    if (!hasTuple) {
        return EXECRC_EOS;
    }
    pTupleData = pInputTuple;
    hasTuple = false;
    return EXECRC_YIELD;
}

FENNEL_END_CPPFILE("$Id$");

// End LbmTupleReader.cpp
