/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
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
#include "fennel/lucidera/test/LbmExecStreamTestBase.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void LbmExecStreamTestBase::initBitmapInput(
    BitmapInput &bmInput, uint nRows, InputData const &inputData)
{
    LbmNumberStreamInput input;
    SharedNumberStream pNumberStream(
        new SkipNumberStream(
            opaqueToInt(inputData.startRid),
            NumberStream::BIG_NUMBER, inputData.skipRows));
    input.pStream = pNumberStream;
    input.bitmapSize = inputData.bitmapSize;

    initBitmapInput(bmInput, nRows, input);
}

void LbmExecStreamTestBase::initBitmapInput(
    BitmapInput &bmInput, uint nRows, LbmNumberStreamInput input)
{
    bmInput.fullBufSize = input.pStream->getMaxRowCount(nRows) * 16;
    bmInput.bufArray.reset(new FixedBuffer[bmInput.fullBufSize]);
    bmInput.nBitmaps = 0;
    bmInput.currBufSize = 0;
    generateBitmaps(nRows, input, bmInput);
}

void LbmExecStreamTestBase::generateBitmaps(
    uint nRows, LbmNumberStreamInput input, BitmapInput &bmInput)
{
    LbmEntry lbmEntry;
    boost::scoped_array<FixedBuffer> entryBuf;
    LcsRid rid = LcsRid(input.pStream->getNext());

    // setup an LbmEntry with the initial rid value
    uint scratchBufSize = LbmEntry::getScratchBufferSize(bitmapColSize);
    entryBuf.reset(new FixedBuffer[scratchBufSize]);
    lbmEntry.init(entryBuf.get(), scratchBufSize, bitmapTupleDesc);
    bitmapTupleData[0].pData = (PConstBuffer) &rid;
    lbmEntry.setEntryTuple(bitmapTupleData);
    TraceLevel level = getSourceTraceLevel(getTraceName());
    if (level <= TRACE_FINER) {
        std::cout << "Set root: " << rid << std::endl;
    }

    // add on the remaining rids
    while (input.pStream->hasNext())
    {
        rid = LcsRid(input.pStream->getNext());
        if (rid >= LcsRid(nRows)) {
            break;
        }
        if (level <= TRACE_FINER) {
            std::cout << "Set value: " << rid << std::endl;
        }
        if ((rid > LcsRid(0) &&
                opaqueToInt(rid % (input.bitmapSize*8)) == 0) ||
            !lbmEntry.setRID(LcsRid(rid)))
        {
            // either hit desired number of rids per bitmap segment or
            // exhausted buffer space, so write the tuple to the output
            // buffer and reset LbmEntry
            produceEntry(lbmEntry, bitmapTupleAccessor, bmInput);
            lbmEntry.setEntryTuple(bitmapTupleData);
        }
    }
    // write out the last LbmEntry
    produceEntry(lbmEntry, bitmapTupleAccessor, bmInput);
    
    assert(bmInput.currBufSize <= bmInput.fullBufSize);
}

void LbmExecStreamTestBase::produceEntry(
    LbmEntry &lbmEntry, TupleAccessor &bitmapTupleAccessor, 
    BitmapInput &bmInput)
{
    TupleData bitmapTuple = lbmEntry.produceEntryTuple();
    bitmapTupleAccessor.marshal(
        bitmapTuple, bmInput.bufArray.get() + bmInput.currBufSize);
    bmInput.currBufSize += bitmapTupleAccessor.getCurrentByteCount();
    ++bmInput.nBitmaps;
}

void LbmExecStreamTestBase::initValuesExecStream(
    uint idx, ValuesExecStreamParams &valuesParams,
    ExecStreamEmbryo &valuesStreamEmbryo, BitmapInput &bmInput)
{
    valuesParams.outputTupleDesc = bitmapTupleDesc;
    valuesParams.pTupleBuffer = bmInput.bufArray;
    valuesParams.bufSize = bmInput.currBufSize;

    valuesStreamEmbryo.init(new ValuesExecStream(), valuesParams);
    std::ostringstream oss;
    oss << "InputValuesExecStream" << "#" << idx;
    valuesStreamEmbryo.getStream()->setName(oss.str());
}

void LbmExecStreamTestBase::initSorterExecStream(
    ExternalSortExecStreamParams &params,
    ExecStreamEmbryo &embryo)
{
    params.outputTupleDesc = bitmapTupleDesc;
    params.distinctness = DUP_ALLOW;
    params.pTempSegment = pRandomSegment;
    params.pCacheAccessor = pCache;
    params.scratchAccessor =
        pSegmentFactory->newScratchSegment(pCache, 10);
    params.keyProj.clear();
    params.keyProj.push_back(0);
    params.storeFinalRun = false;

    embryo.init(ExternalSortExecStream::newExternalSortExecStream(), params);
    embryo.getStream()->setName("SorterExecStream");
}

void LbmExecStreamTestBase::testCaseSetUp()
{    
    ExecStreamUnitTestBase::testCaseSetUp();

    attrDesc_int64 = TupleAttributeDescriptor(
        stdTypeFactory.newDataType(STANDARD_TYPE_INT_64));
    bitmapColSize = pRandomSegment->getUsablePageSize()/8;
    attrDesc_bitmap = TupleAttributeDescriptor(
        stdTypeFactory.newDataType(STANDARD_TYPE_VARBINARY),
        true, bitmapColSize);

    bitmapTupleDesc.push_back(attrDesc_int64);
    bitmapTupleDesc.push_back(attrDesc_bitmap);
    bitmapTupleDesc.push_back(attrDesc_bitmap);

    bitmapTupleData.compute(bitmapTupleDesc);
    bitmapTupleData[1].pData = NULL;
    bitmapTupleData[1].cbData = 0;
    bitmapTupleData[2].pData = NULL;
    bitmapTupleData[2].cbData = 0;
        
    bitmapTupleAccessor.compute(bitmapTupleDesc);
}

void LbmExecStreamTestBase::testCaseTearDown()
{
    ExecStreamUnitTestBase::testCaseTearDown();
    bitmapTupleDesc.clear();
}

FENNEL_END_CPPFILE("$Id$");

// End LbmExecStreamTestBase.cpp
