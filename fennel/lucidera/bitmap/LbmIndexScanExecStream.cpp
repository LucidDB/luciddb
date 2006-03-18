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
    ignoreRowLimit = (rowLimitParamId == DynamicParamId(0));
    if (!ignoreRowLimit) {
        // tupledatum for dynamic parameter
        rowLimitDatum.pData = (PConstBuffer) &rowLimit;
        rowLimitDatum.cbData = sizeof(rowLimit);
    }

    startRidParamId = params.startRidParamId;
    ridInKey = (startRidParamId > DynamicParamId(0));
    if (ridInKey) {
        // make sure full key minus rid is specified as search key
        assert(inputKeyDesc.size() == treeDescriptor.keyProjection.size() -1);

        startRidDatum.pData = (PConstBuffer) &startRid;
        startRidDatum.cbData = sizeof(startRid);

        // add on the rid to the btree search key
        TupleDescriptor ridKeyDesc = inputKeyDesc;

        StandardTypeDescriptorFactory stdTypeFactory;
        TupleAttributeDescriptor attrDesc(
            stdTypeFactory.newDataType(STANDARD_TYPE_RECORDNUM));
        ridKeyDesc.push_back(attrDesc);
        ridSearchKeyData.compute(ridKeyDesc);
        // rid is last key
        ridSearchKeyData[ridSearchKeyData.size() - 1].pData =
            (PConstBuffer) &startRid;

        // need to look for greatest lower bound if searching on rid
        leastUpper = false;
    }
}

bool LbmIndexScanExecStream::reachedTupleLimit(uint nTuples)
{
    if (ignoreRowLimit) {
        return false;
    }

    // read the parameter the first time through
    if (nTuples == 0) {
        pDynamicParamManager->readParam(rowLimitParamId, rowLimitDatum);
    }
    return (nTuples >= rowLimit);
}

void LbmIndexScanExecStream::setAdditionalKeys()
{
    if (ridInKey) {
        // make sure we really are doing an equality search
        assert(lowerBoundDirective == SEARCH_CLOSED_LOWER);
        assert(upperBoundDirective == SEARCH_CLOSED_UPPER);

        // Copy the inputKeyData into the tupledata that contains the 
        // rid key.
        for (uint i = 0; i < inputKeyData.size(); i++) {
            ridSearchKeyData[i] = inputKeyData[i];
        }
        pDynamicParamManager->readParam(startRidParamId, startRidDatum);
        pSearchKey = &ridSearchKeyData;

    } else {
        pSearchKey = &inputKeyData;
    }
}

FENNEL_END_CPPFILE("$Id$");

// End LbmIndexScanExecStream.cpp
