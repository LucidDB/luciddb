/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later Eigenbase-approved version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307  USA
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
        new SpillOutputStream(pSegmentFactory,pCacheAccessor,spillFileName),
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
        setBuffer(pBuffer,cbBuffer);
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
    SegmentAccessor segmentAccessor(pLongLogSegment,pCacheAccessor);
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
        SegmentAccessor segmentAccessor(pSegment,pCacheAccessor);
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
