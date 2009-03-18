/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Copyright (C) 2005-2007 The Eigenbase Project
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
    lbmEntry.init(entryBuf.get(), NULL, scratchBufSize, bitmapTupleDesc);
    bitmapTupleData[0].pData = (PConstBuffer) &rid;
    lbmEntry.setEntryTuple(bitmapTupleData);
    TraceLevel level = getSourceTraceLevel(getTraceName());
    if (level <= TRACE_FINER) {
        std::cout << "Set root: " << rid << std::endl;
    }

    // add on the remaining rids
    while (input.pStream->hasNext()) {
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
    ExecStreamEmbryo &embryo,
    TupleDescriptor const &outputDesc,
    uint nKeys)
{
    params.outputTupleDesc = outputDesc;
    params.distinctness = DUP_ALLOW;
    params.pTempSegment = pRandomSegment;
    params.pCacheAccessor = pCache;
    params.scratchAccessor =
        pSegmentFactory->newScratchSegment(pCache, 10);
    params.keyProj.clear();
    for (uint i = 0; i < nKeys; i++) {
        params.keyProj.push_back(i);
    }
    params.storeFinalRun = false;
    params.estimatedNumRows = MAXU;
    params.earlyClose = false;

    embryo.init(ExternalSortExecStream::newExternalSortExecStream(), params);
    embryo.getStream()->setName("SorterExecStream");
}

void LbmExecStreamTestBase::initNormalizerExecStream(
    LbmNormalizerExecStreamParams &params,
    ExecStreamEmbryo &embryo,
    uint nKeys)
{
    TupleProjection keyProj;
    for (int i = 0; i < nKeys; i++) {
        keyProj.push_back(i);
    }
    params.keyProj = keyProj;

    TupleDescriptor keyDesc;
    keyDesc.projectFrom(keyBitmapTupleDesc, keyProj);
    params.outputTupleDesc = keyDesc;

    embryo.init(new LbmNormalizerExecStream(), params);
    embryo.getStream()->setName("Normalizer");
}

void LbmExecStreamTestBase::generateBitmaps(
    uint nRows, uint start, uint skipRows, PBuffer pBuf, uint &bufSize,
    uint fullBufSize, uint &nBitmaps, bool includeKeys)
{
    LbmEntry lbmEntry;
    boost::scoped_array<FixedBuffer> entryBuf;
    LcsRid rid = LcsRid(start);

    // setup an LbmEntry with the initial rid value
    uint scratchBufSize = LbmEntry::getScratchBufferSize(bitmapColSize);
    entryBuf.reset(new FixedBuffer[scratchBufSize]);
    lbmEntry.init(entryBuf.get(), NULL, scratchBufSize, bitmapTupleDesc);
    bitmapTupleData[0].pData = (PConstBuffer) &rid;
    lbmEntry.setEntryTuple(bitmapTupleData);

    // add on the remaining rids
    for (rid = LcsRid(start + skipRows); rid < LcsRid(nRows); rid += skipRows) {
        if (!lbmEntry.setRID(LcsRid(rid))) {
            // exhausted buffer space, so write the tuple to the output
            // buffer and reset LbmEntry
            produceEntry(
                lbmEntry, bitmapTupleAccessor,
                pBuf, bufSize, nBitmaps, includeKeys);
            lbmEntry.setEntryTuple(bitmapTupleData);
        }
    }
    // write out the last LbmEntry
    produceEntry(
        lbmEntry, bitmapTupleAccessor, pBuf, bufSize, nBitmaps, includeKeys);

    assert(bufSize <= fullBufSize);
}

void LbmExecStreamTestBase::produceEntry(
    LbmEntry &lbmEntry, TupleAccessor &bitmapTupleAccessor, PBuffer pBuf,
    uint &bufSize, uint &nBitmaps, bool includeKeys)
{
    TupleData bitmapTuple = lbmEntry.produceEntryTuple();
    if (includeKeys) {
        int nKeys = keyBitmapTupleData.size() - bitmapTuple.size();
        assert(nKeys > 0);
        for (uint i = 0; i < bitmapTupleData.size(); i++) {
            keyBitmapTupleData[nKeys + i] = bitmapTuple[i];
        }
        keyBitmapTupleAccessor.marshal(keyBitmapTupleData, pBuf + bufSize);
        bufSize += keyBitmapTupleAccessor.getCurrentByteCount();
    } else {
        bitmapTupleAccessor.marshal(bitmapTuple, pBuf + bufSize);
        bufSize += bitmapTupleAccessor.getCurrentByteCount();
    }
    ++nBitmaps;
}

void LbmExecStreamTestBase::initKeyBitmap(
    uint nRows,
    std::vector<int> const &repeatSeqValues)
{
    // find the interval for which the entire tuple's sequence repeats
    uint skipRows = getTupleInterval(repeatSeqValues);

    // generate a key bitmap for each distinct input value...
    // configure descriptor
    uint nInputKeys = repeatSeqValues.size();
    keyBitmapTupleDesc.clear();
    for (uint i = 0; i < nInputKeys; i++) {
        keyBitmapTupleDesc.push_back(attrDesc_int64);
    }
    for (uint i = 0; i < bitmapTupleDesc.size(); i++) {
        keyBitmapTupleDesc.push_back(bitmapTupleDesc[i]);
    }

    // configure accessor and key data (bitmap data handled elsewhere)
    keyBitmapTupleAccessor.compute(keyBitmapTupleDesc);
    keyBitmapTupleData.compute(keyBitmapTupleDesc);
    boost::scoped_array<uint64_t> vals(new uint64_t[nInputKeys]);
    for (uint i = 0; i < nInputKeys; i++) {
        keyBitmapTupleData[i].pData = (PConstBuffer) &vals[i];
    }

    uint fullBufSize = nRows * keyBitmapTupleAccessor.getMaxByteCount();
    keyBitmapBuf.reset(new FixedBuffer[fullBufSize]);
    PBuffer pBuf = keyBitmapBuf.get();
    keyBitmapBufSize = 0;
    uint nBitmaps = 0;
    for (uint i = 0; i < skipRows; i++) {
        // generate input keys
        for (uint j = 0; j < nInputKeys; j++) {
            vals[j] = i % repeatSeqValues[j];
        }
        generateBitmaps(
            nRows, i, skipRows, pBuf, keyBitmapBufSize,
            fullBufSize, nBitmaps, true);
    }
}

/**
 * Find the interval for which an entire tuple's sequence repeats
 */
uint LbmExecStreamTestBase::getTupleInterval(
    std::vector<int> const &repeatSeqValues, uint nKeys)
{
    if (nKeys == 0) {
        nKeys = repeatSeqValues.size();
    }
    uint interval = 1;
    for (uint i = 0; i < nKeys; i++) {
        interval *= repeatSeqValues[i];
    }
    return interval;
}

void LbmExecStreamTestBase::testCaseSetUp()
{
    ExecStreamUnitTestBase::testCaseSetUp();

    bitmapColSize = pRandomSegment->getUsablePageSize() / 8;
    attrDesc_bitmap = TupleAttributeDescriptor(
        stdTypeFactory.newDataType(STANDARD_TYPE_VARBINARY),
        true, bitmapColSize);
    attrDesc_int64 = TupleAttributeDescriptor(
        stdTypeFactory.newDataType(STANDARD_TYPE_INT_64));

    bitmapTupleDesc.clear();
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

FENNEL_END_CPPFILE("$Id$");

// End LbmExecStreamTestBase.cpp
