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

#ifndef Fennel_CrcSegInputStream_Included
#define Fennel_CrcSegInputStream_Included

#include "fennel/segment/SegInputStream.h"

#include <boost/crc.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * CrcSegInputStream extends SegInputStream by verifying checksum
 * information on each page read.  An invalid page is interpreted as end of
 * stream.
 */
class FENNEL_SEGMENT_EXPORT CrcSegInputStream
    : public SegInputStream
{
    PseudoUuid onlineUuid;

    // TODO:  use a 64-bit crc instead
    boost::crc_32_type crcComputer;

    explicit CrcSegInputStream(
        SegmentAccessor const &segmentAccessor,
        PseudoUuid onlineUuid,
        PageId beginPageId);

    inline bool lockBufferParanoid();

    virtual void lockBuffer();

public:
    /**
     * Creates a new CrcSegInputStream.
     *
     * @param segmentAccessor accessor for the segment containing the stream
     * data
     *
     * @param onlineUuid PseudoUuid which each page should match
     *
     * @param beginPageId the first page of stream data; if the default
     * FIRST_LINEAR_PAGE_ID is passed, the segment must support
     * LINEAR_ALLOCATION, and the stream starts at the first page of the
     * segment; if NULL_PAGE_ID is passed, an empty stream is returned
     *
     * @return shared_ptr to new SegInputStream
     */
    static SharedSegInputStream newCrcSegInputStream(
        SegmentAccessor const &segmentAccessor,
        PseudoUuid onlineUuid,
        PageId beginPageId = FIRST_LINEAR_PAGE_ID);
};

FENNEL_END_NAMESPACE

#endif

// End CrcSegInputStream.h
