/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

#ifndef Fennel_SegmentFactory_Included
#define Fennel_SegmentFactory_Included

#include "fennel/common/CompoundId.h"
#include "fennel/common/ClosableObject.h"
#include "fennel/synch/SynchObj.h"
#include "fennel/segment/TracingSegment.h"
#include "fennel/device/DeviceMode.h"

#include <boost/dynamic_bitset.hpp>
#include <boost/utility.hpp>
#include <boost/enable_shared_from_this.hpp>

FENNEL_BEGIN_NAMESPACE

class ConfigMap;
class TraceTarget;
class LinearDeviceSegmentParams;
class PseudoUuid;

/**
 * SegmentFactory is a factory class for creating instances of the Segment
 * interface.  See <a href="structSegmentDesign.html#SegmentFactory">the design
 * docs</a> for more detail.
 */
class SegmentFactory
    : public boost::noncopyable,
        public boost::enable_shared_from_this<SegmentFactory>
{
    friend class TempSegDestructor;
    
    static ParamName paramTraceSegments;

    /**
     * Trace target if tracing enable.
     */
    TraceTarget *pTraceTarget;

    boost::dynamic_bitset<> tempDeviceIdBitset;

    DeviceId firstTempDeviceId;

    StrictMutex mutex;

    DeviceId allocateTempDeviceId();

    void deallocateTempDeviceId(DeviceId);

    explicit SegmentFactory(
        ConfigMap const &configMap,
        TraceTarget *pTraceTarget);
public:
    /**
     * Constructs a new SegmentFactory.
     *
     * @param configMap ConfigMap defining properties to use when instantiating
     * segments
     *
     * @param pTraceTarget target for trace messages; if NULL, tracing is
     * never performed (otherwise it depends on configMap)
     *
     * @return new factory
     */
    static SharedSegmentFactory newSegmentFactory(
        ConfigMap const &configMap,
        TraceTarget *pTraceTarget);
    
    virtual ~SegmentFactory();

    /**
     * Opens a LinearDeviceSegment.
     *
     * @param cache the cache to use for this segment
     *
     * @param params initialization parameters
     *
     * @return new segment
     */
    SharedSegment newLinearDeviceSegment(
        SharedCache cache,
        LinearDeviceSegmentParams const &params);

    /**
     * Opens a RandomAllocationSegment.
     *
     * @param delegateSegment the underlying segment providing storage; this
     * segment must return LINEAR_ALLOCATION from getAllocationOrder(), and
     * should already be allocated to the desired size
     *
     * @param bFormat if true, the RandomAllocationSegment is formatted as
     * empty; otherwise, the existing formatting is read
     *
     * @return new segment
     */
    SharedSegment newRandomAllocationSegment(
        SharedSegment delegateSegment,
        bool bFormat);

    /**
     * Opens a WALSegment.
     *
     * @param logSegment the Segment in which log pages are stored; this
     * segment must guarantee at least ASCENDING_ALLOCATION (TBD: plus some
     * other restrictions on the way in which page allocation is implemented)
     *
     * @return new segment
     */
    SharedSegment newWALSegment(
        SharedSegment logSegment);

    /**
     * Opens a LinearViewSegment.
     *
     * @param delegateSegment the underlying segment
     *
     * @param firstPageId PageId of starting page in underlying segment, or
     * NULL_PAGE_ID to create a new LinearViewSegment
     *
     * @return new segment
     */
    SharedSegment newLinearViewSegment(
        SharedSegment delegateSegment,
        PageId firstPageId);

    /**
     * Opens a CircularSegment.
     *
     * @param delegateSegment the underlying segment
     *
     * @param pCheckpointProvider the CheckpointProvider to call
     * when segment space is getting low; if this is singular,
     * the caller must take care of checkpointing to prevent
     * space from running out
     *
     * @param oldestPageId restored oldest PageId for recovery, or
     * NULL_PAGE_ID for empty segment
     *
     * @param newestPageId restored newest PageId for recovery, or
     * NULL_PAGE_ID for empty segment or unknown
     *
     * @return new segment
     */
    SharedSegment newCircularSegment(
        SharedSegment delegateSegment,
        SharedCheckpointProvider pCheckpointProvider,
        PageId oldestPageId = NULL_PAGE_ID,
        PageId newestPageId = NULL_PAGE_ID);

    /**
     * Opens a VersionedSegment.
     *
     * @param dataSegment the segment storing the latest page versions
     *
     * @param logSegment the log segment used for storing
     * old page versions; an assertion violation will result if this
     * is not an instance of WALSegment
     *
     * @param versionNumber TODO: doc
     *
     * @param onlineUuid TODO: doc
     *
     * @return new segment
     */
    SharedSegment newVersionedSegment(
        SharedSegment dataSegment,
        SharedSegment logSegment,
        PseudoUuid const &onlineUuid,
        SegVersionNum versionNumber);

    /**
     * Creates a ScratchSegment.
     *
     * @param pCache cache from which to allocate scratch pages
     *
     * @param nPagesMax maximum number of scratch pages to allocate from cache,
     * or MAXU for unlimited
     *
     * @return SegmentAccessor for returned segment; all access must
     * be through this SegmentAccessor's pCacheAccessor
     */
    SegmentAccessor newScratchSegment(
        SharedCache pCache,
        uint nPagesMax = MAXU);

    /**
     * If necessary, wraps a TracingSegment around another segment.
     *
     * @param pSegment the underlying segment
     *
     * @param sourceName the trace source name for this segment
     *
     * @param qualifySourceName if true, pSegment's pointer address is appended
     * to sourceName to make it unique
     *
     * @return the wrapped segment
     */
    SharedSegment newTracingSegment(
        SharedSegment pSegment,
        std::string sourceName,
        bool qualifySourceName = true);

    /**
     * Creates a new temporary device paired with a LinearDeviceSegment.  The
     * device will be automatically deleted when the segment is destroyed.
     *
     * @param pCache the cache to use for this segment
     *
     * @param deviceMode mode in which to open the device
     *
     * @param deviceFileName filename to use for device; if relative,
     * location is dependent on SegmentFactory configuration parameters
     *
     * @return new segment
     */
    SharedSegment newTempDeviceSegment(
        SharedCache pCache,
        DeviceMode deviceMode,
        std::string deviceFileName);
    
    /**
     * Some implementations of the Segment interface extend the interface with
     * implementation-specific features.  dynamicCast provides access to a
     * derived interface from the generic SharedSegment.  It is the caller's
     * responsibility to ensure that the returned (non-shared) pointer remains
     * protected by the original shared_ptr.  All generic access to the segment
     * should still go through the original SharedSegment for proper tracing.
     *
     *<p>
     *
     * Example usage given a SharedSegment pSegment:
     *
     *<code>
     *
     * VersionedSegment *pVersionedSegment =
     *     SegmentFactory::dynamicCast<VersionedSegment *>(pSegment);
     *
     *</code>
     *
     * @param pSegment the SharedSegment to downcast
     *
     * @return result of the downcast as a reference, or NULL if
     * pSegment was singular or of a different type
     */
    template <class PDerivedSegment>
    static PDerivedSegment dynamicCast(SharedSegment pSegment)
    {
        PDerivedSegment pDerived = dynamic_cast<PDerivedSegment>(
            pSegment.get());
        if (pDerived) {
            return pDerived;
        }
        TracingSegment *pTracing = dynamic_cast<TracingSegment *>(
            pSegment.get());
        if (pTracing) {
            pDerived = dynamic_cast<PDerivedSegment>(
                pTracing->getDelegateSegment().get());
        }
        return pDerived;
    }
};

class TempSegDestructor : public ClosableObjectDestructor
{
    SharedSegmentFactory pSegmentFactory;
    
public:
    explicit TempSegDestructor(SharedSegmentFactory);
    void operator()(Segment *pSegment);
};
    
FENNEL_END_NAMESPACE

#endif

// End SegmentFactory.h
