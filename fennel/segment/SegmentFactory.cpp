/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 1999 John V. Sichi
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
#include "fennel/segment/SegmentFactory.h"
#include "fennel/segment/LinearDeviceSegment.h"
#include "fennel/segment/LinearViewSegment.h"
#include "fennel/segment/WALSegment.h"
#include "fennel/segment/VersionedSegment.h"
#include "fennel/segment/ScratchSegment.h"
#include "fennel/segment/RandomAllocationSegment.h"
#include "fennel/segment/SnapshotRandomAllocationSegment.h"
#include "fennel/segment/VersionedRandomAllocationSegment.h"
#include "fennel/segment/DynamicDelegatingSegment.h"
#include "fennel/segment/CircularSegment.h"
#include "fennel/segment/SegmentAccessor.h"
#include "fennel/common/ConfigMap.h"
#include "fennel/common/FileSystem.h"
#include "fennel/device/RandomAccessFileDevice.h"

FENNEL_BEGIN_CPPFILE("$Id$");

ParamName SegmentFactory::paramTraceSegments = "traceSegments";

SharedSegmentFactory SegmentFactory::newSegmentFactory(
    ConfigMap const &configMap,
    SharedTraceTarget pTraceTarget)
{
    return SharedSegmentFactory(
        new SegmentFactory(configMap, pTraceTarget));
}

SegmentFactory::SegmentFactory(
    ConfigMap const &configMapInit,
    SharedTraceTarget pTraceTargetInit)
    : configMap(configMapInit)
{
    pTraceTarget = pTraceTargetInit;

    // TODO:  parameterize
    firstTempDeviceId = DeviceId(512);
    tempDeviceIdBitset.resize(512);
}

SegmentFactory::~SegmentFactory()
{
}

ConfigMap const &SegmentFactory::getConfigMap() const
{
    return configMap;
}

SharedSegment SegmentFactory::newLinearDeviceSegment(
    SharedCache cache,
    LinearDeviceSegmentParams const &params)
{
    SharedSegment pSegment(
        new LinearDeviceSegment(cache, params),
        ClosableObjectDestructor());
    SharedSegment tracingSegment =
        newTracingSegment(pSegment, "LinearDeviceSegment");
    tracingSegment->initForUse();
    return tracingSegment;
}

SharedSegment SegmentFactory::newRandomAllocationSegment(
    SharedSegment delegateSegment,
    bool bFormat,
    bool deferInit)
{
    RandomAllocationSegment *pRandomSegment =
        new RandomAllocationSegment(delegateSegment);
    SharedSegment pSegment(pRandomSegment, ClosableObjectDestructor());
    SharedSegment tracingSegment =
        newTracingSegment(pSegment, "RandomAllocationSegment");
    // Format the segment through the tracing segment so the operation
    // is traced
    if (bFormat) {
        tracingSegment->deallocatePageRange(NULL_PAGE_ID, NULL_PAGE_ID);
    }
    if (!deferInit) {
        tracingSegment->initForUse();
    }
    return tracingSegment;
}

SharedSegment SegmentFactory::newVersionedRandomAllocationSegment(
    SharedSegment delegateSegment,
    SharedSegment pTempSegment,
    bool bFormat,
    bool deferInit)
{
    VersionedRandomAllocationSegment *pVersionedRandomSegment =
        new VersionedRandomAllocationSegment(delegateSegment, pTempSegment);
    SharedSegment pSegment(pVersionedRandomSegment, ClosableObjectDestructor());
    SharedSegment tracingSegment =
        newTracingSegment(pSegment, "VersionedRandomAllocationSegment");
    // Format the segment through the tracing segment so the operation
    // is traced
    if (bFormat) {
        tracingSegment->deallocatePageRange(NULL_PAGE_ID, NULL_PAGE_ID);
    }
    if (!deferInit) {
        tracingSegment->initForUse();
    }
    return tracingSegment;
}

SharedSegment SegmentFactory::newSnapshotRandomAllocationSegment(
    SharedSegment delegateSegment,
    SharedSegment versionedSegment,
    TxnId snapshotCsn,
    bool readOnlyCommittedData)
{
    SnapshotRandomAllocationSegment *pSnapshotSegment =
        new SnapshotRandomAllocationSegment(
            delegateSegment,
            versionedSegment,
            snapshotCsn,
            readOnlyCommittedData);
    SharedSegment pSegment(pSnapshotSegment, ClosableObjectDestructor());
    SharedSegment tracingSegment =
        newTracingSegment(pSegment, "SnapshotRandomAllocationSegment");
    tracingSegment->initForUse();
    return tracingSegment;
}

SharedSegment SegmentFactory::newDynamicDelegatingSegment(
    SharedSegment delegateSegment)
{
    DynamicDelegatingSegment *pDelegatingSegment =
        new DynamicDelegatingSegment(WeakSegment(delegateSegment));
    SharedSegment pSegment(pDelegatingSegment, ClosableObjectDestructor());
    SharedSegment tracingSegment =
        newTracingSegment(pSegment, "DynamicDelegatingSegment");
    tracingSegment->initForUse();
    return tracingSegment;
}

SharedSegment SegmentFactory::newWALSegment(
    SharedSegment logSegment)
{
    SharedSegment pSegment(
        new WALSegment(logSegment),
        ClosableObjectDestructor());
    SharedSegment tracingSegment =
        newTracingSegment(pSegment, "WALSegment");
    tracingSegment->initForUse();
    return tracingSegment;
}

SharedSegment SegmentFactory::newLinearViewSegment(
    SharedSegment delegateSegment,
    PageId firstPageId)
{
    SharedSegment pSegment(
        new LinearViewSegment(delegateSegment, firstPageId),
        ClosableObjectDestructor());
    SharedSegment tracingSegment =
        newTracingSegment(pSegment, "LinearViewSegment");
    tracingSegment->initForUse();
    return tracingSegment;
}

SharedSegment SegmentFactory::newVersionedSegment(
    SharedSegment dataSegment,
    SharedSegment logSegment,
    PseudoUuid const &onlineUuid,
    SegVersionNum versionNumber)
{
    SharedSegment pSegment(
        new VersionedSegment(
            dataSegment, logSegment, onlineUuid, versionNumber),
        ClosableObjectDestructor());
    SharedSegment tracingSegment =
        newTracingSegment(pSegment, "VersionedSegment");
    tracingSegment->initForUse();
    return tracingSegment;
}

SegmentAccessor SegmentFactory::newScratchSegment(
    SharedCache pCache,
    uint nPagesMax)
{
    boost::shared_ptr<ScratchSegment> pSegment(
        new ScratchSegment(pCache, nPagesMax),
        ClosableObjectDestructor());
    SegmentAccessor segmentAccessor;
    segmentAccessor.pSegment = newTracingSegment(pSegment, "ScratchSegment");
    segmentAccessor.pSegment->initForUse();
    segmentAccessor.pCacheAccessor = pSegment;
    return segmentAccessor;
}

SharedSegment SegmentFactory::newCircularSegment(
    SharedSegment delegateSegment,
    SharedCheckpointProvider pCheckpointProvider,
    PageId oldestPageId,
    PageId newestPageId)
{
    SharedSegment pSegment(
        new CircularSegment(
            delegateSegment,
            pCheckpointProvider,
            oldestPageId, newestPageId),
        ClosableObjectDestructor());
    SharedSegment tracingSegment =
        newTracingSegment(pSegment, "CircularSegment");
    tracingSegment->initForUse();
    return tracingSegment;
}

SharedSegment SegmentFactory::newTracingSegment(
    SharedSegment pSegment,
    std::string sourceName,
    bool qualifySourceName)
{
    if (!pTraceTarget.get()) {
        return pSegment;
    }
    if (qualifySourceName) {
        std::ostringstream oss;
        oss << "segment." << sourceName << "." << pSegment.get();
        sourceName = oss.str();
    }
    if (pTraceTarget->getSourceTraceLevel(sourceName) > TRACE_FINE) {
        // all segment tracing is TRACE_FINE or lower, so don't bother
        return pSegment;
    }
    SharedSegment pTracingSegment(
        new TracingSegment(pSegment, pTraceTarget, sourceName),
        ClosableObjectDestructor());

    pSegment->setTracingSegment(WeakSegment(pTracingSegment));

    return pTracingSegment;
}

// TODO:  parameters
SharedSegment SegmentFactory::newTempDeviceSegment(
    SharedCache pCache,
    DeviceMode deviceMode,
    std::string deviceFileName)
{
    // TODO:  guard to automatically deallocateTempDeviceId on failure?
    DeviceId deviceId = allocateTempDeviceId();
    if (deviceMode.create) {
        FileSystem::remove(deviceFileName.c_str());
    }
    // TODO: depending on config params?
    // deviceMode.temporary = true;
    SharedRandomAccessDevice pDevice(
        new RandomAccessFileDevice(deviceFileName, deviceMode));
    pCache->registerDevice(deviceId, pDevice);
    LinearDeviceSegmentParams deviceParams;
    CompoundId::setDeviceId(deviceParams.firstBlockId, deviceId);
    CompoundId::setBlockNum(deviceParams.firstBlockId, 0);
    deviceParams.nPagesAllocated = 0;
    if (!deviceMode.create) {
        deviceParams.nPagesAllocated = MAXU;
    }
    SharedSegment pSegment(
        new LinearDeviceSegment(pCache, deviceParams),
        TempSegDestructor(shared_from_this()));
    SharedSegment tracingSegment =
        newTracingSegment(pSegment, "TempLinearDeviceSegment");
    tracingSegment->initForUse();
    return tracingSegment;
}

DeviceId SegmentFactory::allocateTempDeviceId()
{
    StrictMutexGuard mutexGuard(mutex);
    // TODO:  submit fast find-clear-bit to boost
    for (uint i = 0; i < tempDeviceIdBitset.size(); ++i) {
        if (!tempDeviceIdBitset[i]) {
            tempDeviceIdBitset[i] = true;
            return firstTempDeviceId + i;
        }
    }
    permAssert(false);
}

void SegmentFactory::deallocateTempDeviceId(DeviceId deviceId)
{
    StrictMutexGuard mutexGuard(mutex);
    uint i = opaqueToInt(deviceId - firstTempDeviceId);
    assert(tempDeviceIdBitset[i]);
    tempDeviceIdBitset[i] = false;
}

TempSegDestructor::TempSegDestructor(
    SharedSegmentFactory pSegmentFactoryInit)
    : pSegmentFactory(pSegmentFactoryInit)
{
}

void TempSegDestructor::operator()(Segment *pSegment)
{
    LinearDeviceSegment *pLinearDeviceSegment =
        dynamic_cast<LinearDeviceSegment *>(pSegment);
    SharedCache pCache = pSegment->getCache();
    DeviceId deviceId = pLinearDeviceSegment->getDeviceId();
    // NOTE:  pSegment and pLinearDeviceSegment are invalidated here
    ClosableObjectDestructor::operator()(pSegment);
    pCache->unregisterDevice(deviceId);
    pSegmentFactory->deallocateTempDeviceId(deviceId);
}

SnapshotRandomAllocationSegment *SegmentFactory::getSnapshotSegment(
    SharedSegment pSegment)
{
    SnapshotRandomAllocationSegment *pSnapshotSegment =
        SegmentFactory::dynamicCast<SnapshotRandomAllocationSegment *>(
            pSegment);
    if (pSnapshotSegment == NULL) {
        DynamicDelegatingSegment *pDynamicSegment =
            SegmentFactory::dynamicCast<DynamicDelegatingSegment *>(
                pSegment);
        if (pDynamicSegment != NULL) {
            pSnapshotSegment =
                SegmentFactory::dynamicCast<SnapshotRandomAllocationSegment *>(
                    pDynamicSegment->getDelegateSegment());
        }
    }
    return pSnapshotSegment;
}

FENNEL_END_CPPFILE("$Id$");

// End SegmentFactory.cpp
