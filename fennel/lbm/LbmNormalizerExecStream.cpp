/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2010-2010 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
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
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/lbm/LbmNormalizerExecStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void LbmNormalizerExecStream::prepare(
    LbmNormalizerExecStreamParams const &params)
{
    ConduitExecStream::prepare(params);

    keyBitmapDesc = pInAccessor->getTupleDesc();
    keyBitmapAccessor.compute(keyBitmapDesc);
    keyBitmapData.compute(keyBitmapDesc);

    // temporarily fake a key projection
    keyProj = params.keyProj;
    keyDesc.projectFrom(keyBitmapDesc, keyProj);
    keyData.compute(keyDesc);
}

void LbmNormalizerExecStream::open(bool restart)
{
    ConduitExecStream::open(restart);
    segmentReader.init(pInAccessor, keyBitmapData);
    producePending = false;
}

ExecStreamResult LbmNormalizerExecStream::execute(
    ExecStreamQuantum const &quantum)
{
    ExecStreamResult rc;

    for (uint i = 0; i < quantum.nTuplesMax; i++) {
        while (! producePending) {
            rc = readSegment();
            if (rc == EXECRC_EOS) {
                pOutAccessor->markEOS();
                return rc;
            } else if (rc != EXECRC_YIELD) {
                return rc;
            }
            assert(producePending);
        }
        if (! produceTuple()) {
            return EXECRC_BUF_OVERFLOW;
        }
    }
    return EXECRC_QUANTUM_EXPIRED;
}

ExecStreamResult LbmNormalizerExecStream::readSegment()
{
    assert(!producePending);

    ExecStreamResult rc = segmentReader.readSegmentAndAdvance(
        segment.byteNum, segment.byteSeg, segment.len);
    if (rc == EXECRC_YIELD) {
        producePending = true;
        nTuplesPending = segment.countBits();
        assert(nTuplesPending > 0);
    }
    return rc;
}

bool LbmNormalizerExecStream::produceTuple()
{
    assert(producePending);

    // manually project output keys from the current tuple
    if (segmentReader.getTupleChange()) {
        for (uint i = 0; i < keyProj.size(); i++) {
            keyData[i] = keyBitmapData[keyProj[i]];
        }
        segmentReader.resetChangeListener();
    }

    if (pOutAccessor->produceTuple(keyData)) {
        nTuplesPending--;
        if (nTuplesPending == 0) {
            producePending = false;
        }
        return true;
    }
    return false;
}

FENNEL_END_CPPFILE("$Id$");

// End LbmNormalizerExecStream.cpp
