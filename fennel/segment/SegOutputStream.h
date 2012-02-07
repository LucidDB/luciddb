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

#ifndef Fennel_SegOutputStream_Included
#define Fennel_SegOutputStream_Included

#include "fennel/segment/SegStream.h"
#include "fennel/common/ByteOutputStream.h"

FENNEL_BEGIN_NAMESPACE

/**
 * SegOutputStream implements the ByteOutputStream interface by writing data
 * to pages allocated from a Segment.  The Segment must support the
 * get/setPageSuccessor interface for chaining the pages together.
 */
class FENNEL_SEGMENT_EXPORT SegOutputStream
    : public SegStream, public ByteOutputStream
{
protected:
    /**
     * First PageId allocated.
     */
    PageId firstPageId;

    /**
     * Last PageId allocated.
     */
    PageId lastPageId;

    /**
     * Maximum number of data bytes which can be stored per page
     * (does not include header).
     */
    uint cbMaxPageData;

    /**
     * Counter for getPageCount().
     */
    BlockNum nPagesAllocated;

    /**
     * @return number of bytes already written to current page
     */
    uint getBytesWrittenThisPage() const;

    // implement the ByteOutputStream interface
    virtual void flushBuffer(uint cbRequested);
    virtual void closeImpl();

    explicit SegOutputStream(SegmentAccessor const &,uint cbExtraHeader = 0);

    /**
     * Hook for subclasses; default is to do nothing.
     *
     * @param node the node being flushed
     */
    virtual void writeExtraHeaders(SegStreamNode &node);

public:
    /**
     * Creates a new SegOutputStream.
     *
     * @param segmentAccessor accessor for the segment in which to store the
     * data
     *
     * @return shared_ptr to new SegOutputStream
     */
    static SharedSegOutputStream newSegOutputStream(
        SegmentAccessor const &segmentAccessor);

    /**
     * Gets the first PageId allocated.  For non-linear segments, this is
     * required in order to be able to read the data back via SegInputStream.
     *
     * @return the first PageId allocated, or NULL_PAGE_ID if no data has been
     * written to the stream yet
     */
    PageId getFirstPageId() const;

    /**
     * @return the number of pages allocated by this stream
     */
    BlockNum getPageCount() const;

    /**
     * Updates current page header without unlocking; allows
     * a SegInputStream to read the contents from the same thread
     * (but another thread would be locked out).
     */
    void updatePage();

    // implement the SegStream interface
    virtual void getSegPos(SegStreamPosition &pos);
};

FENNEL_END_NAMESPACE

#endif

// End SegOutputStream.h
