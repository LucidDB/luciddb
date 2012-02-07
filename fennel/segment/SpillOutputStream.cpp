/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/

#include "fennel/common/CommonPreamble.h"
#include "fennel/segment/SpillOutputStream.h"
#include "fennel/segment/SegOutputStream.h"
#include "fennel/segment/SegInputStream.h"
#include "fennel/segment/SegmentFactory.h"
#include "fennel/common/ByteArrayInputStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

// TODO:  support multiple scratch pages

SharedSpillOutputStream SpillOutputStream::newSpillOutputStream(
    SharedSegmentFactory pSegmentFactory,
    SharedCacheAccessor pCacheAccessor,
    std::string spillFileName)
{
    return SharedSpillOutputStream(
        new SpillOutputStream(pSegmentFactory, pCacheAccessor, spillFileName),
        ClosableObjectDestructor());
}

SpillOutputStream::SpillOutputStream(
    SharedSegmentFactory pSegmentFactoryInit,
    SharedCacheAccessor pCacheAccessorInit,
    std::string spillFileNameInit)
    : pSegmentFactory(pSegmentFactoryInit),
      pCacheAccessor(pCacheAccessorInit),
      spillFileName(spillFileNameInit)
{
    // REVIEW:  this causes pCacheAccessor to lose its chance to intercept any
    // of the scratch calls
    scratchAccessor = pSegmentFactory->newScratchSegment(
        pCacheAccessor->getCache(),
        1);
    scratchPageLock.accessSegment(scratchAccessor);
    scratchPageLock.allocatePage();
    cbBuffer = scratchAccessor.pSegment->getUsablePageSize();
    setBuffer(
        scratchPageLock.getPage().getWritableData(),
        cbBuffer);
}

SpillOutputStream::~SpillOutputStream()
{
}

void SpillOutputStream::flushBuffer(uint cbRequested)
{
    if (scratchPageLock.isLocked()) {
        assert(!pSegOutputStream);
        // grow from short to long
        spill();
    } else {
        assert(pSegOutputStream);
        assert(!scratchPageLock.isLocked());
        // already long
        assert(cbBuffer >= getBytesAvailable());
        pSegOutputStream->consumeWritePointer(cbBuffer - getBytesAvailable());
    }
    assert(pSegOutputStream);
    if (cbRequested) {
        PBuffer pBuffer =
            pSegOutputStream->getWritePointer(cbRequested,&cbBuffer);
        setBuffer(pBuffer, cbBuffer);
    } else {
        pSegOutputStream->hardPageBreak();
        cbBuffer = 0;
    }
}

void SpillOutputStream::closeImpl()
{
    if (scratchPageLock.isLocked()) {
        // discard contents
        scratchPageLock.unlock();
    } else {
        assert(pSegOutputStream);
        assert(!scratchPageLock.isLocked());
        // flush long log
        pSegOutputStream->consumeWritePointer(cbBuffer - getBytesAvailable());
        cbBuffer = 0;
        pSegOutputStream.reset();
    }
}

void SpillOutputStream::setWriteLatency(WriteLatency writeLatency)
{
    ByteOutputStream::setWriteLatency(writeLatency);
    if (pSegOutputStream) {
        pSegOutputStream->setWriteLatency(writeLatency);
    }
}

// REVIEW:  due to page size discrepancies, spill will screw up any
// LogicalTxnParticipant relying on getWritePointer/getReadPointer.  Should
// either warn about this or fix it.  Also, after spill, could cut out the
// middleman by having LogicalTxn reference pSegOutputStream directly and
// discard the SpillOutputStream.

void SpillOutputStream::spill()
{
    DeviceMode devMode = DeviceMode::createNew;
    // TODO:  make this a parameter; for now it's always direct since this is
    // only used for log streams
    devMode.direct = true;
    SharedSegment pLongLogSegment =
        pSegmentFactory->newTempDeviceSegment(
            scratchPageLock.getCacheAccessor()->getCache(),
            devMode,
            spillFileName);
    SegmentAccessor segmentAccessor(pLongLogSegment, pCacheAccessor);
    pSegOutputStream = SegOutputStream::newSegOutputStream(segmentAccessor);
    pSegOutputStream->setWriteLatency(writeLatency);
    pSegOutputStream->writeBytes(
        scratchPageLock.getPage().getReadableData(),
        cbBuffer - getBytesAvailable());
    scratchPageLock.unlock();
}

SharedByteInputStream SpillOutputStream::getInputStream(
    SeekPosition seekPosition)
{
    if (scratchPageLock.isLocked()) {
        SharedByteInputStream pInputStream =
            ByteArrayInputStream::newByteArrayInputStream(
                scratchPageLock.getPage().getReadableData(),
                getOffset());
        if (seekPosition == SEEK_STREAM_END) {
            pInputStream->seekForward(getOffset());
        }
        return pInputStream;
    } else {
        assert(pSegOutputStream);
        updatePage();
        SharedSegment pSegment = pSegOutputStream->getSegment();
        SegStreamPosition endPos;
        if (seekPosition == SEEK_STREAM_END) {
            pSegOutputStream->getSegPos(endPos);
        }
        SegmentAccessor segmentAccessor(pSegment, pCacheAccessor);
        SharedSegInputStream pInputStream =
            SegInputStream::newSegInputStream(segmentAccessor);
        if (seekPosition == SEEK_STREAM_END) {
            pInputStream->seekSegPos(endPos);
        }
        return pInputStream;
    }
}

void SpillOutputStream::updatePage()
{
    if (!cbBuffer) {
        return;
    }
    assert(cbBuffer > getBytesAvailable());
    uint cbConsumed = cbBuffer - getBytesAvailable();
    pSegOutputStream->consumeWritePointer(cbConsumed);
    cbBuffer -= cbConsumed;
    pSegOutputStream->updatePage();
}

SharedSegment SpillOutputStream::getSegment()
{
    if (pSegOutputStream) {
        return pSegOutputStream->getSegment();
    } else {
        return SharedSegment();
    }
}

SharedSegOutputStream SpillOutputStream::getSegOutputStream()
{
    return pSegOutputStream;
}

FENNEL_END_CPPFILE("$Id$");

// End SpillOutputStream.cpp
