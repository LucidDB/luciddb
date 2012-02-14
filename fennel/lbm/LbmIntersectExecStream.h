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

#ifndef Fennel_LbmIntersectExecStream_Included
#define Fennel_LbmIntersectExecStream_Included

#include "fennel/lbm/LbmBitOpExecStream.h"

FENNEL_BEGIN_NAMESPACE

/**
 * LbmIntersectExecStreamParams defines parameters for instantiating a
 * LbmIntersectExecStream
 */
struct LbmIntersectExecStreamParams : public LbmBitOpExecStreamParams
{
};

/**
 * LbmIntersectExecStream is the execution stream used to perform
 * intersection on two or more bitmap stream inputs
 *
 * @author Zelaine Fong
 * @version $Id$
 */
class FENNEL_LBM_EXPORT LbmIntersectExecStream
    : public LbmBitOpExecStream
{
    /**
     * Number of inputs with overlapping rid values.  Must be equal to
     * nInputs for an intersection to take place.
     */
    uint nMatches;

    /**
     * Minimum length of overlapping bitmap segments found thus far
     */
    uint minLen;

    /**
     * Performs intersect operation on all segments
     *
     * @param len length of intersecting segments
     *
     * @return false if buffer overflow occurred writing out a segment
     */
    bool intersectSegments(uint len);

public:
    virtual void prepare(LbmIntersectExecStreamParams const &params);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
    virtual void closeImpl();
};

FENNEL_END_NAMESPACE

#endif

// End LbmIntersectExecStream.h
