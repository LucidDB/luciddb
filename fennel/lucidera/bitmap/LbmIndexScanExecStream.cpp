/*
// $Id$ 
// Fennel is a library of data storage and processing components.
// Copyright (C) 2006-2006 LucidEra, Inc.
// Copyright (C) 2006-2006 The Eigenbase Project
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
#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/lucidera/colstore/LcsClusterNode.h"
#include "fennel/lucidera/bitmap/LbmIndexScanExecStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void LbmIndexScanExecStream::prepare(LbmIndexScanExecStreamParams const &params)
{
    BTreeSearchExecStream::prepare(params);

    rowLimitParamId = params.rowLimitParamId;
    startRidParamId = params.startRidParamId;
    ignoreRowLimit =
        params.ignoreRowLimit || rowLimitParamId == DynamicParamId(0);

    // setup tupledatums for copying dynamic parameters
    rowLimitDatum.pData = (PConstBuffer) &rowLimit;
    rowLimitDatum.cbData = sizeof(rowLimit);
    startRidDatum.pData = (PConstBuffer) &startRid;
    startRidDatum.cbData = sizeof(startRid);

    // if the full key is being used, then the rid is part of the key
    ridInKey =
        treeDescriptor.keyProjection.size() == inputKeyDesc.size() &&
            startRidParamId > DynamicParamId(0);
    // if the full key without the rid is used as the search key, make
    // sure there is no start rid parameter
    assert(
        !(treeDescriptor.keyProjection.size() - 1 == inputKeyDesc.size() &&
            startRidParamId > DynamicParamId(0)));

    if (ridInKey) {
        // if the rid is part of the key, make sure rids are expected to be
        // outputted in the first key and a rid is the last btree
        // search key
        StandardTypeDescriptorFactory stdTypeFactory;
        TupleAttributeDescriptor expectedRidDesc(
            stdTypeFactory.newDataType(STANDARD_TYPE_RECORDNUM));
        assert(params.outputTupleDesc[0] == expectedRidDesc);
        assert(inputKeyDesc[inputKeyDesc.size() - 1] == expectedRidDesc);
        assert(lowerBoundDirective == SEARCH_OPEN_LOWER);
        assert(upperBoundDirective == SEARCH_CLOSED_UPPER);

        // need to look for greatest lower bound if searching on rid
        leastUpper = false;

        // remove the rid from the upper bound key, since the rid is only
        // needed to initiate the lower bound search
        upperBoundProj.pop_back();
        upperBoundAccessor.bind(
            pInAccessor->getConsumptionTupleAccessor(), upperBoundProj);
        upperBoundDesc.projectFrom(pInAccessor->getTupleDesc(), upperBoundProj);
        upperBoundData.compute(upperBoundDesc);
    }
}

bool LbmIndexScanExecStream::reachedTupleLimit(uint nTuples)
{
    if (ignoreRowLimit) {
        return false;
    }

    // read the parameter the first time through
    if (nTuples == 1) {
        pDynamicParamManager->readParam(rowLimitParamId, rowLimitDatum);
    }
    return (nTuples >= rowLimit);
}

void LbmIndexScanExecStream::setAdditionalKeys()
{
    if (ridInKey) {
        pDynamicParamManager->readParam(startRidParamId, startRidDatum);

        // rid is the last key
        inputKeyData[inputKeyData.size() - 1].pData = (PConstBuffer) &startRid;
    }
}

FENNEL_END_CPPFILE("$Id: //open/dt/dev/fennel/lucidera/bitmap/LbmIndexScanExecStream.cpp#1 $");

// End LbmIndexScanExecStream.cpp
