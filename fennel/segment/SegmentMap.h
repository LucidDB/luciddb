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

#ifndef Fennel_SegmentMap_Included
#define Fennel_SegmentMap_Included

FENNEL_BEGIN_NAMESPACE

/**
 * SegmentMap defines an interface for mapping a SegmentId to a loaded Segment
 * instance.
 */
class FENNEL_SEGMENT_EXPORT SegmentMap
{
public:
    virtual ~SegmentMap()
    {
    }

    /**
     * Finds a segment by its SegmentId.
     *
     * @param segmentId the SegmentId to find
     *
     * @param pDataSegment the specific segment associated with a statement,
     * if a specific segment must be used
     *
     * @return loaded segment, or a singular SharedSegment if not found
     */
    virtual SharedSegment getSegmentById(
        SegmentId segmentId,
        SharedSegment pDataSegment) = 0;
};

FENNEL_END_NAMESPACE

#endif

// End SegmentMap.h
