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

#ifndef Fennel_LbmSeqSegmentReader_Included
#define Fennel_LbmSeqSegmentReader_Included

#include "fennel/tuple/TupleData.h"
#include "fennel/exec/ExecStreamDefs.h"
#include "fennel/lbm/LbmSegmentReaderBase.h"

FENNEL_BEGIN_NAMESPACE

/**
 * LbmSeqSegmentReader provides the interace necessary to read byte segments
 * sequentially. Only segments that contain set bit values are returned to
 * the caller.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
class FENNEL_LBM_EXPORT LbmSeqSegmentReader
    : public LbmSegmentReaderBase
{
public:
    /**
     * Initializes reader to start reading byte segments from a specified
     * input stream
     *
     * @param pInAccessorInit input stream accessor
     *
     * @param bitmapSegTupleInit tuple data for reading segments
     */
    void init(
        SharedExecStreamBufAccessor &pInAccessorInit,
        TupleData &bitmapSegTupleInit);

    /**
     * Reads the current byte segment, based on current position and advances
     * forward to the next segment
     *
     * @param byteNum returns byte number corresponding to the start
     * of the current byte segment
     *
     * @param byteSeg returns current byte segment; note that the segment read
     * is stored backwards, so the caller needs to read from right to left
     * starting at byteSeg
     *
     * @param len returns length of current byte segment
     */
    ExecStreamResult readSegmentAndAdvance(
        LbmByteNumber &byteNum, PBuffer &byteSeg, uint &len);

    /**
     * Returns the start rid of the current tuple
     */
    LcsRid getSrid();
};

FENNEL_END_NAMESPACE

#endif

// End LbmSeqSegmentReader.h
