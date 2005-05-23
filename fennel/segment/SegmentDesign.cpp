/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
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

FENNEL_BEGIN_CPPFILE("$Id$");

/**

<a name="Overview"></a>
<h3>Overview</h3>

The Fennel segment library defines mechanisms for mapping from logical storage
spaces to the physical storage interfaces defined by the device layer (as
described in DeviceDesign).  Here's a rough analogy to non-persistent memory:

<ul>

<li>The device layer is like persistent physical memory.

<li>BlockIds are like physical memory addresses.

<li>The segment layer is like persistent virtual memory.

<li>A segment is like a single virtual address space.

<li>PageIds are like virtual memory addresses.  Unlike a
BlockId, a PageId is only meaningful in the context of a particular segment.

</ul>

However, the analogy is not exact, since the mapping from virtual to physical
can be arbitrarily defined by each segment implementation, along with extra
semantics such as versioning.  Also, there may be multiple layers of segment
virtualization rather than a direct logical-to-physical mapping.

<p>

The segment library also relies on the cache library (TBD: link to cache
design) for all device access, and for imposing rules such as write ordering.

<hr>

<h3>Segment Interface</h3>

The Segment interface provides the basic definition of a logical storage space.
Some aspects of this interface are optional (meaning implementations are free
to raise exceptions when an unsupported or irrelevant capability is requested).
Derived classes augment the Segment interface with additional methods.
The base public Segment interface methods can be categorized as follows:

<ul>

<li> <em>Page allocation</em>.  Each segment has an associated set of allocated
pages; each page of a segment has a different PageId.  Segment::allocatePageId
allocates a new page of storage and returns its PageId.
Segment::getAllocationOrder returns information about the nature of the
PageId's returned by allocatePageId, as defined by the Segment::AllocationOrder
enumeration.  Most common are <em>linear</em> PageId's, which are allocated
consecutively starting from 0.  Segment::deallocatePageRange frees allocated
pages.  Segment::isPageIdAllocated can be used to test the allocation status
of particular PageId's.

<li> <em>Segment size</em>.  Segment::getAllocatedSizeInPages returns the
cardinality of a segment's set of allocated pages.
Segment::ensureAllocatedSize can be used to allocate a large number of pages at
once.

<li> <em>Page size</em>.  Segments may reserve footer space on each page for
their own bookkeeping purposes.  Segment::getUsablePageSize allows a caller
to determine how much of a page can safely be used.  Segment::getFullPageSize
returns the full page size including all footers (this is currently the same as
the cache page size).

<li>
<em>PageId/BlockId translation</em>.  Segment::translatePageId maps a logical
PageId to a physical BlockId.  Segment::translateBlockId maps a physical
BlockId back to a logical PageId.

<li> <em>Checkpoint</em>.  The cached status of pages of a segment can be
controlled with Segment::checkpoint.

<li> <em>Page successors</em>.  For some segments, there is a meaningful
successorship function over the set of PageIds (e.g. for linear allocation
segments, the successor is determined by interpreting the PageId as an
integer).  Segment::getPageSuccessor can be used to find the successor of a
PageId.  For segments where the ordering can be imposed (rather than being
determined implicitly by allocation), Segment::setPageSuccessor can be used to
change the successor relationship.

<li> Segment derives from the MappedPageListener interface so that it can
receive cache notifications for any of its pages whenever they are mapped into
the cache.  The default behavior is to ignore the notifications, but segment
implementations can override this with arbitrary behavior.

<li> Segment also implements the ClosableObject interface by calling
checkpoint(CHECKPOINT_FLUSH_AND_UNMAP) when the segment is closed.  This
ensures that cache pages do not remain mapped to a segment which has already
been closed.

</ul>

<hr>

<a name="SegmentFactory"></a>
<h3>SegmentFactory</h3>

Subsequent sections describe the available implementations of Segment.  These
classes cannot be instantiated directly; instead, SegmentFactory provides
methods for this purpose, and for some related functionality:

<ul>

<li> Trace support is provided implicitly for all of the public Segment
interface methods.  SegmentFactory accomplishes this by wrapping Segment
instances with TracingSegment (but only when tracing is turned on).

<li> When SegmentFactory returns a shared_ptr to a new Segment, it ensures that
the shared_ptr is constructed in such a way that the Segment's close method
will be called automatically before destruction.

<li> SegmentFactory manages DeviceId allocation for file devices associated with
temporary segments, and ensures that the corresponding temporary files are
deleted when a temporary segment is closed.

</ul>

<hr>

<a name="LinearDeviceSegment"></a>
<h3>LinearDeviceSegment</h3>

LinearDeviceSegment maps a range of linear PageId's to contiguous blocks of a
RandomAccessDevice.  The diagram below shows the most general case:

<hr>
\image html LinearDeviceSegment.gif
<hr>

In this example, the segment currently has five pages allocated (the light gray
rectangles).  The start of the segment does not have to coincide with the start
of the device; this is controlled by the firstBlockId parameter.  The white
blocks are inaccessible via this segment; they could be unused, or allocated to
other segments.  The dark gray rectangles represent blocks which could be
allocated in the future, up to the nPagesMax limit.

<p>

The device size only needs to be big enough to contain the allocated pages.  If
nPagesMax is beyond the end of the device, then the segment will attempt to
expand the device when necessary for allocation of new pages, in chunks
determined by the nPagesIncrement parameter.

<h4>Uses</h4>

LinearDeviceSegment can be used directly to store data structures with
predictable allocation patterns (e.g. a fixed-size hash table), or as a basis
for the more complex segments described later.

<hr>

<a name="RandomAllocationSegment"></a>
<h3>RandomAllocationSegment</h3>

RandomAllocationSegment implements random page allocation/deallocation in terms
of an underlying linear segment (in the simplest case, directly on top of a
LinearDeviceSegment).  PageId 0 is reserved as a segment map.  The rest of the
segment is divided up into extents, each headed with an extent map page, as
shown in the following diagram:

<hr>
\image html RandomAllocationSegment.gif
<hr>

Here, each extent contains one extent map page followed by four data pages;
real extents are much bigger.  The entries in the PageId 0 segment map page
contain the number of unallocated pages in each extent (light gray rectangles
represent unallocated blocks).  Each extent map contains the allocation status
of the corresponding data pages, together with successor PageId entries,
forming singly-linked lists terminated with NULL_PAGE_ID).  Storing the
successor chain in the extent map allows for efficient prefetch even in the
presence of fragmentation.  In this example, the successor sequence is
2,3,4,5,8,14,10.  The first extent is compact, but the last two extents are
fragmented.

<p>

Not shown is an additional field which allows an owner ID to be recorded
together with each page allocation in an extent map.  This field is also
interpreted as the actual allocation status (with a reserved owner ID
representing an unallocated page).

TODO: This can be used for storage integrity verification and repair, as well
as efficient wholesale deallocation of complex structures.

<p>

For very large segments, multiple segment map pages may be required.  These are
interspersed before each run of extents (rather than all at the beginning after
PageId 0) in order to allow a segment to grow without bound.  The last segment
map page is marked to indicate that there are no more.

<h4>Size Calculations</h4>

The extent size and number of extents per segment map page are fixed based on:

<ul>


<li>page size

<li>size of headers on map pages

<li>size of entries on map pages

</ul>

The exact formulas used are in the constructor
(RandomAllocationSegment::RandomAllocationSegment).  Here's an example
calculation:

<ol>

<li>let page size be either 4096 bytes (4KB) or 16384 bytes (16KB)

<li>assume no page footers required for underlying segment

<li>typical segment map header size is 32 bytes

<li>typical extent map header size is 8 bytes

<li>typical segment map entry size is 4 bytes

<li>typical extent map entry size is 16 bytes

<li>number of pages mapped per extent is (4096 - 16)/8 = 510 for a 4KB page
size, or 2046 for a 16KB page size

<li>extent size is 510*4KB=2040KB (close to 2MB) for a 4KB page size, or 32MB
for a 16KB page size

<li>number of extents per segment map node is (4096 - 32)/4 = 1016 for a 4KB
page size, or 4088 for a 16KB page size

<li>number of pages mapped per segment map node is 1016*510=518160 for a 4KB
page size, or 8364048 for a 16KB page size

<li>data mapped per segment map node is 518160*4KB=2072640KB (close to 2GB) for
a 4KB page size, or 133GB for a 16KB page size

<li>a terabyte database would require over 500 segment map pages with a 4KB
page size, but fewer than 10 with a 16KB page size

</ol>

<h4>Growth</h4>

If the underlying segment supports it, a RandomAllocationSegment can grow
whenever all of its initial allocation is exhausted.  Growth is in increments
of one extent at a time.  When an entire segment map page is full (all extents
allocated), a new segment map page with one empty extent is allocated.

<h4>Uses</h4>

RandomAllocationSegment is good for implementing dynamic data structures such
as BTrees where the page allocation/deallocation pattern is unpredictable.

<hr>

<a name="CircularSegment"></a>
<h3>CircularSegment</h3>

CircularSegment implements "infinite" page allocation in terms of a fixed-size
underlying linear segment via wrap-around (so CircularSegment
PageId <code>x</code> corresponds to PageId <code>x modulo n</code> in the
underlying linear segment, where <code>n</code> is the fixed size).  Allocation
fails if old pages are not deallocated soon enough.  The diagram below provides
an example:

<hr>
\image html CircularSegment.gif
<hr>

<h4>Uses</h4>

CircularSegment is good for implementing logs (but requires regular
checkpointing in order to free log space).

<hr>

<a name="VersionedSegment"></a>
<a name="WALSegment"></a>
<h3>VersionedSegment and WALSegment</h3>

VersionedSegment and WALSegment collaborate to provide a rudimentary page-based
versioning and recovery scheme.  (Currently the versioning is only used
internally for recovery, and only two versions are maintained, but eventually
online multi-versioning may be exposed for application use).  The recovery
guarantee made by VersionedSegment is that after a crash it can be restored to
the exact physical state written at the time of a checkpoint prior to the
crash.  (This may be the last checkpoint or the penultimate checkpoint
depending on the checkpoint type, as discussed later.)

<p>

VersionedSegment stores data pages in an underlying segment of any kind.  It
stores log pages in a separate WALSegment, which provides the bookkeeping
needed to implement the write-ahead logging protocol.  WALSegment requires its
underlying segment to support ascending (but not necessarily linear) allocation
with successor support.  The diagram below shows the state of a
VersionedSegment after several checkpoints:

<hr>
\image html VersionedSegment.gif
<hr>

Some allocated pages are <em>clean</em>, meaning they have not been modified
since the last checkpoint, and hence have no corresponding pages in the
WALSegment.  Others are <em>dirty</em> and have corresponding log entries.  The
association between data and log pages is two-way; the data-to-log direction is
stored in an in-memory table (dataToLogMap), but the log-to-data direction is
stored persistently via the footer of each logged page; this footer also
records the page version number and checksum for use during recovery.  In
addition, WALSegment maintains a dirtyPageSet automatically by implementing the
cache's MappedPageListener interface.  This allows VersionedSegment to
determine whether the log page for a given dirty data page has been flushed
yet; if not, VersionedSegment prevents the cache from flushing the data page by
overriding MappedPageListener::canFlushPage.  VersionedSegment also overrides
MappedPageListener::notifyPageDirty to intercept each write to a page, giving
it a chance to log the clean page data before the write takes place.  Logging
for a particular page is only required before the first write after each
checkpoint, since recovery is not responsible for restoring intermediate
states.

<h4>Checkpoints</h4>

Two kinds of checkpoints are supported for VersionedSegments.  The simplest is
a sharp checkpoint, which has the following sequence:

<ol>

<li>All dirty log pages are flushed.

<li>All dirty data pages are flushed.

<li>The log is truncated (all log pages are deallocated).  This step is
encapsulated in a separate method (VersionedSegment::deallocateCheckpointedLog)
to allow VersionedSegment to participate in higher-level checkpoint processes.

</ol>

The large amount of synchronous I/O required for a sharp checkpoint may result
in poor throughput, so fuzzy checkpoints are also supported.  When fuzzy
checkpointing is used, log data spans two checkpoint intervals rather than just
one.  When a checkpoint is requested, the only data pages that are flushed are
those that were also dirty at the time of the previous checkpoint; any others
are left dirty in the hope that they will be flushed for other reasons, such as
cache victimization, before the next checkpoint.  The bookkeeping for this is
encapsulated by FuzzyCheckpointSet.  After a fuzzy checkpoint, the log cannot
be truncated completely; instead, three log PageId pointers are maintained:

<ul>

<li>oldestLogPageId records the earliest log page allocated

<li>lastCheckpointLogPageId records the latest log page allocated before the
previous checkpoint

<li>newestLogPageId records the latest log page allocated

</ul>

During a fuzzy checkpoint, all pages earlier than lastCheckpointLogPageId are
deallocated, and the log pointers are shifted so that oldestLogPageId advances
to one page past lastCheckpointLogPageId, and lastCheckpointLogPageId advances
to newestLogPageId.

<p>

TODO:  a version/log history diagram is needed to help explain this

<h4>Recovery</h4>

Regardless of whether sharp or fuzzy checkpoints were used online, the recovery
procedure is always the same:

<ol>

<li> The caller to VersionedSegment::recover must provide the PageId from which
to start reading the log.  This must be recorded while online by calling
VersionedSegment::getRecoveryPageId during each checkpoint and storing the
result in some other durable location.  VersionedSegment also requires the
caller to store and provide the latest segment version number, available from
VersionedSegment::getVersionNumber.

<li> When a log page is read, its footer is used to determine what to do with
it.

<li> If the checksum recorded in the footer does not match a recovery checksum
computed from the page contents, the page is considered invalid and recovery
terminates (successfully, under the assumption that the page represents the
result of an incomplete write at the end of the log).  TBD:  onlineUuid. 

<li> If the page version is older than the latest version number, the page is
skipped and recovery continues (TBD: why does this happen?  I think fuzzy
checkpointing).

<li> If the previous tests pass, the page is assumed to be a good log page, and
the contents are copied over to the corresponding data page.

<li> Recovery advances to the next log page using the successor relationship.
If NULL_PAGE_ID is encountered, recovery terminates successfully.

</ol>

<h4>Uses</h4>

VersionedSegment can be used for UNDO recovery to restore an action-consistent
checkpoint state.  More complex transactional guarantees, such as REDO recovery
to a last committed state, must be programmed at a higher level (TBD: link to
txn design docs).  WALSegment can be used independently as part of other
logging schemes.

<hr>

<a name="LinearViewSegment"></a>
<h3>LinearViewSegment</h3>

LinearViewSegment allows a linear view to be imposed on top of a random
allocation segment.  The linear sequence is defined in terms of the successor
function, so given the first page in a successor chain, the linear view
is constructed by following the chain and building up an in-memory page table
for random access, as in the following diagram:

<hr>
\image html LinearViewSegment.gif
<hr>

<h4>Uses</h4>

LinearViewSegment is good for storing large dynamically allocated random-access
arrays, such as LOB data, in an underlying RandomAllocationSegment.

<hr>

<a name="ScratchSegment"></a>
<h3>ScratchSegment</h3>

ScratchSegment is like a ramdisk.  Rather than storing data in a device, it
allocates scratch pages from the cache.  Page contents persist only until the
scratch segment is closed, and remain implicitly locked into cache until then.

<p>

Scratch page BlockIds reference the cache's singleton instance of the
RandomAccessNullDevice.  However, these BlockId's are not mapped by the
cache, so all access to ScratchSegment pages must go through a
special CacheAccessor implementation.

<h4>Uses</h4>

ScratchSegment is useful for creating efficient temporary structures, since
it eliminates the overhead associated with persistent page allocation.  It can
also be useful as a means of managing miscellaneous scratch memory required by
a complex process such as a query execution.

<hr>

<h3>Segment Layering</h3>

Several of the segment types defined above are built on top of another
underlying segment.  This segment layering can be of arbitrary depth; abstract
base class DelegatingSegment makes it easy to create new segment
implementations which use layering.  The diagram below shows an example of how
segments can be layered in interesting ways:

<hr>
\image html SegmentLayering.gif
<hr>

At the lowest level are two LinearDeviceSegments.  The first accesses a data
device, and the second accesses a log device.  The CircularSegment imposes a
particular page allocation pattern on the log, while the WALSegment adds the
write-ahead logging support required by VersionedSegment.  The VersionedSegment
combines the data device with the log for physical recovery.  On top of this,
the pages of the VersionedSegment are interpreted as a RandomAllocationSegment,
which is further subdivided by two LinearViewSegments.

<p>

Layering order can make a big difference.  In this example, since the
RandomAllocationSegment is above the VersionedSegment, all pages are versioned,
including the segment and extent maps.  This is good, since recovery needs to
restore these to be consistent with the data page contents.  However, in other
contexts it might make sense to use versioning above a RandomAllocationSegment,
for example to version one fixed-allocation object independently of everything
else.

<p>

When footers are required by more than one layer, the footers are appended,
with the lower layers towards the end of the page.  Accordingly, the usable
page size is cumulatively reduced.  DelegatingSegment ensures that events such
as checkpoints and page notifications are propagated through all layers (unless
a layer consumes it without forwarding it on to those beneath it).  Since cache
pages can only be mapped to one segment at a time, they are mapped to the
highest layer to guarantee that all layers see notifications.

<p>

Layering is designed to allow for abstraction.  In the example above, it should
be possible to eliminate the CircularSegment to create an ever-growing log
instead, without the layers above knowing anything about it.

<hr>

<h3>Page Access</h3>

So far, this document has focused on how various segment implementations map
logical pages to physical storage.  The segment layer also provides
infrastructure useful for the implementation of complex structures stored in
segments (e.g. BTrees):

<ul>

<li> SegmentAccessor is a convenience class which combines a Segment with a
CacheAccessor, since these are usually needed together in order to access pages
via the Cache.

<li> SegPageLock is a scoped guard for locking pages and guaranteeing that they
get unlocked even if an exception occurs, as well as hiding the details of
PageId/BlockId translation and MappedPageListener.  Code above the segment
layer should almost always use this class rather than accessing the cache layer
directly.

<li> SegNodeLock and StoredNode provide a generic framework for prefixing the
data of each page with a fixed-size, class-specific header, including a magic
number for verifying that a newly accessed page is of the expected type.

<li> SegPageIter allows a chain of page successors to be prefetched
automatically as part of iteration.

<li> SegOutputStream and SegInputStream allow segments supporting the
successor relationship to be used for direct storage of sequential-access
data such as transaction logs.  SpillOutputStream provides a buffering
optimization.  CrcSegOutputStream and CrcSegInputStream compute checksums on
stored stream data.

</ul>

<hr>

<h3>TODO</h3>

Some segment implementations which will be provided in the future include:

<ul>

<li> <em>RollingLogSegment</em>:  for logs which are stored in multiple files,
with new files being created as the log grows, and old files being deleted as
the corresponding logs are deallocated by checkpoints.

<li> <em>ArchivedCircularSegment</em>:  a specialization of CircularSegment
which uses a background thread to archive old blocks before they are
reallocated.

<li> <em>CopyOnWriteSegment</em>: a union which allows multiple read-write
views over a shared underlying read-only segment.

</ul>

It would be easy to define a CompoundSegment allowing multiple Segments to be
combined, but it's probably a better idea to rely on operating system volume
management support (with the exception of special cases that can benefit from
database-level semantics, such as ping-pong writes for automatic log
mirroring).

<hr>

 */
struct SegmentDesign 
{
    // NOTE:  dummy class for doxygen
};

FENNEL_END_CPPFILE("$Id$");

// End SegmentDesign.cpp
